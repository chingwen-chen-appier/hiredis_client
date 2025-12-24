#include <appier/cntk/toolbox/toolbox.hpp>
#include <appier/cntk/flow/flow.hpp>
#include <appier/cntk/proc/ping_data.hpp>
#include <appier/cntk/storage/storage.hpp>

#include <iostream>
#include <vector>
#include <deque>
#include <chrono>
#include <thread>
#include <fstream>
#include <atomic>
#include <mutex>
#include <string>
#include <memory>

#include <hiredis/hiredis.h>
#include <yaml-cpp/yaml.h>
#include "MurmurHash3.h"


namespace apct = appier::cntk::toolbox;
namespace apcf = appier::cntk::flow;
namespace apcp = appier::cntk::proc;
namespace apcs = appier::cntk::storage;


struct RedisRequest {
    std::string userId;
    std::function<void(int err, std::chrono::microseconds duration)> onFinish;
};


struct BenchmarkMetrics {
    std::atomic<long> totalRequests{0};
    std::atomic<long> totalSuccess{0};
    std::atomic<long> totalErrors{0};
    std::atomic<long> totalTimeouts{0};
};


class RedisPool {
public:
    RedisPool(const std::string& host, int port, int size, const timeval& timeout) {
        for (int i = 0; i < size; ++i) {
            redisContext* ctx = redisConnectWithTimeout(host.c_str(), port, timeout);
            if (!ctx || ctx->err) {
                std::cerr << "Redis connection failed: "
                          << (ctx ? ctx->errstr : "null context") << std::endl;
                if (ctx) redisFree(ctx);
                continue;
            }
            pool_.push_back(ctx);
        }
    }

    ~RedisPool() {
        std::lock_guard<std::mutex> lock(mtx_);
        for (auto* ctx : pool_) {
            redisFree(ctx);
        }
    }

    redisContext* acquire() {
        std::lock_guard<std::mutex> lock(mtx_);
        if (pool_.empty()) return nullptr;
        redisContext* ctx = pool_.front();
        pool_.pop_front();
        return ctx;
    }

    void release(redisContext* ctx) {
        if (!ctx) return;
        std::lock_guard<std::mutex> lock(mtx_);
        pool_.push_back(ctx);
    }

private:
    std::deque<redisContext*> pool_;
    std::mutex mtx_;
};


class RedisPoolSet {
public:
    void addPool(std::unique_ptr<RedisPool> pool) {
        pools_.emplace_back(std::move(pool));
    }

    RedisPool& get(size_t index) {
        return *pools_[index];
    }

    size_t size() const {
        return pools_.size();
    }

private:
    std::vector<std::unique_ptr<RedisPool>> pools_;
};


uint32_t murmurHash(const std::string& value, uint32_t seed = 0) {
    uint32_t out;
    MurmurHash3_x86_32(value.data(), value.size(), seed, &out);
    return out;
}


void asyncWorker(
        const std::string& redisKey,
        RedisPoolSet& poolSet,
        apct::Channel<RedisRequest>& requestChan,
        BenchmarkMetrics& metrics,
        apct::WaitGroup& workerWg) {
    while (true) {
        auto [req, ok] = requestChan.read();
        if (!ok) {
            break;
        }

        size_t poolIndex = murmurHash(req.userId) % poolSet.size();
        RedisPool& pool = poolSet.get(poolIndex);

        redisContext* ctx = pool.acquire();
        if (!ctx) {
            ++metrics.totalErrors;
            if (req.onFinish) {
                req.onFinish(REDIS_ERR, std::chrono::microseconds(0));
            }
            continue;
        }

        auto start = std::chrono::high_resolution_clock::now();
        redisReply* reply = static_cast<redisReply*>(
            redisCommand(ctx, "HGET %s %s", redisKey.c_str(), req.userId.c_str())
        );
        auto end = std::chrono::high_resolution_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

        metrics.totalRequests++;

        int errStatus = 0;
        if (!reply) {
            if (ctx->err == REDIS_ERR_TIMEOUT) {
                ++metrics.totalTimeouts;
            } else {
                ++metrics.totalErrors;
            }
        } else {
            ++metrics.totalSuccess;
            freeReplyObject(reply);
        }

        pool.release(ctx);

        if (req.onFinish) {
            req.onFinish(errStatus, std::chrono::microseconds(duration));
        }
    }

    workerWg.done();
}


void syncWorker(
    int requestsPerThread,
    const std::vector<std::string>& userIds,
    const std::string& redisKey,
    RedisPoolSet& poolSet,
    BenchmarkMetrics& metrics) {
    size_t userIdx =
        std::hash<std::thread::id>{}(std::this_thread::get_id()) % userIds.size();

    for (int i = 0; i < requestsPerThread; ++i) {
        const std::string& userId = userIds[userIdx];
        userIdx = (userIdx + 1) % userIds.size();

        size_t poolIndex = murmurHash(userId) % poolSet.size();
        RedisPool& pool = poolSet.get(poolIndex);

        redisContext* ctx = pool.acquire();
        if (!ctx) {
            ++metrics.totalErrors;
            continue;
        }

        auto start = std::chrono::high_resolution_clock::now();
        redisReply* reply = static_cast<redisReply*>(
            redisCommand(ctx, "HGET %s %s", redisKey.c_str(), userId.c_str())
        );
        auto end = std::chrono::high_resolution_clock::now();

        auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

        ++metrics.totalRequests;

        if (!reply) {
            if (ctx->err == REDIS_ERR_TIMEOUT) {
                ++metrics.totalTimeouts;
            } else {
                ++metrics.totalErrors;
            }
        } else {
            ++metrics.totalSuccess;
            freeReplyObject(reply);
        }

        pool.release(ctx);
    }
}


int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0]
                  << " <async[true|false]> <threads> <requests_per_thread> <config.yaml>\n";
        return 1;
    }

    const bool asyncMode = std::string(argv[1]) == "true";
    const int threads = std::stoi(argv[2]);
    const int requestsPerThread = std::stoi(argv[3]);
    const std::string configPath = argv[4];

    YAML::Node config = YAML::LoadFile(configPath);

    const int poolSize = config["pool_size"].as<int>();
    const int timeoutMs = config["connection_timeout_ms"].as<int>();
    const std::string userIdFile = config["user_id_file"].as<std::string>();
    const std::string redisKey = config["user_db_key"].as<std::string>();

    timeval timeout{
        timeoutMs / 1000,
        (timeoutMs % 1000) * 1000
    };

        RedisPoolSet poolSet;

    for (const auto& entry : config["redis_pool_set"]) {
        const auto& server = entry[0];
        poolSet.addPool(std::make_unique<RedisPool>(
            server["host"].as<std::string>(),
            server["port"].as<int>(),
            poolSize,
            timeout
        ));
    }

    std::vector<std::string> userIds;
    std::ifstream infile(userIdFile);
    for (std::string line; std::getline(infile, line);) {
        if (!line.empty()) userIds.emplace_back(std::move(line));
    }

    BenchmarkMetrics metrics;
    std::chrono::high_resolution_clock::time_point benchStart;

    if (asyncMode) {
        std::cout << "Running in asynchronous mode.\n";

        apct::Channel<RedisRequest> requestChan(100000);
        apct::WaitGroup workerWg;
        apct::WaitGroup callbackWg;

        for (int i = 0; i < threads; ++i) {
            workerWg.add(1);
            std::thread(asyncWorker, redisKey, std::ref(poolSet), std::ref(requestChan),
                        std::ref(metrics), std::ref(workerWg)).detach();
        }

        benchStart = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < threads * requestsPerThread; ++i) {
            callbackWg.add(1);
            requestChan.write({
                userIds[i % userIds.size()],
                [&callbackWg](int err, std::chrono::microseconds dur) {
                    callbackWg.done();
                }
            });
        }
        requestChan.close();
        workerWg.wait();
        callbackWg.wait();

    } else {
        std::cout << "Running in synchronous mode.\n";

        std::vector<std::thread> workers;
        workers.reserve(threads);

        benchStart = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < threads; ++i) {
            workers.emplace_back(
                syncWorker,
                requestsPerThread,
                std::cref(userIds),
                std::cref(redisKey),
                std::ref(poolSet),
                std::ref(metrics)
            );
        }

        for (auto& t : workers) {
            t.join();
        }
    }

    auto benchEnd = std::chrono::high_resolution_clock::now();
    double elapsedSeconds =
        std::chrono::duration<double>(benchEnd - benchStart).count();
    long long elapsedMicroseconds =
        std::chrono::duration_cast<std::chrono::microseconds>(
            benchEnd - benchStart).count();
    auto average = elapsedMicroseconds / metrics.totalRequests;
    std::cout << "\n--- Benchmark Results ---\n";
    std::cout << "Total Requests: " << metrics.totalRequests << "\n";
    std::cout << "Success:        " << metrics.totalSuccess << "\n";
    std::cout << "Timeouts:       " << metrics.totalTimeouts << "\n";
    std::cout << "Errors:         " << metrics.totalErrors << "\n";
    std::cout << "Elapsed Time:   " << elapsedSeconds << " s\n";
    std::cout << "Average req time:   "
              << average
              << " us\n";
    std::cout << "Average req time (Thread):    "
              << average * threads
              << " us\n";

    return 0;
}
