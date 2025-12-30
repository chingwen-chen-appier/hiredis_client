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


constexpr int REDIS_ERR_BATCHTIMEOUT = 100;


namespace apct = appier::cntk::toolbox;
namespace apcf = appier::cntk::flow;
namespace apcp = appier::cntk::proc;
namespace apcs = appier::cntk::storage;


using RedisReplyPointer =
    std::unique_ptr<redisReply, decltype(&freeReplyObject)>;


using RedisContextPtr = std::unique_ptr<redisContext, decltype(&redisFree)>;


struct RedisRequest {
    std::string userId;
    std::function<void(int err)> onFinish;
};


struct BenchmarkMetrics {
    std::atomic<long> totalRequests{0};
    std::atomic<long> totalSuccess{0};
    std::atomic<long> totalErrors{0};
    std::atomic<long> totalQueryTimeouts{0};
    std::atomic<long> totalBatchTimeouts{0};
};


class RedisPool {
public:
    RedisPool(const std::string& host, int port, int poolSize,
              const timeval& connectionTimeout, const timeval& queryTimeout)
        : host_(host), port_(port), poolSize_(poolSize),
          connectionTimeout_(connectionTimeout), queryTimeout_(queryTimeout) {
        for (int i = 0; i < poolSize_; ++i) {
            auto ctx = createConnection();
            if (ctx) {
                pool_.push_back(std::move(ctx));
            }
        }
    }

    ~RedisPool() = default;

    RedisContextPtr acquire() {
        std::lock_guard<std::mutex> lock(mtx_);
        if (pool_.empty()) {
            return RedisContextPtr(nullptr, redisFree);
        }

        auto ctx = std::move(pool_.front());
        pool_.pop_front();
        return ctx;
    }

    void release(RedisContextPtr ctx) {
        if (!ctx || ctx->err != REDIS_OK) {
            ctx.reset();
            auto newCtx = createConnection();
            if (newCtx) {
                std::lock_guard<std::mutex> lock(mtx_);
                pool_.push_back(std::move(newCtx));
            }
            return;
        }

        std::lock_guard<std::mutex> lock(mtx_);
        pool_.push_back(std::move(ctx));
    }

private:
    RedisContextPtr createConnection() {
        redisContext* rawCtx = redisConnectWithTimeout(
            host_.c_str(), port_, connectionTimeout_);

        if (!rawCtx) {
            std::cerr << "Redis connection failed to " << host_ << ":" << port_
                      << ": null context" << std::endl;
            return RedisContextPtr(nullptr, redisFree);
        }

        if (rawCtx->err) {
            std::cerr << "Redis connection failed to " << host_ << ":" << port_
                      << ": " << rawCtx->errstr << std::endl;
            redisFree(rawCtx);
            return RedisContextPtr(nullptr, redisFree);
        }

        if (redisSetTimeout(rawCtx, queryTimeout_) != REDIS_OK) {
            std::cerr << "Failed to set timeout on Redis connection to "
                      << host_ << ":" << port_ << std::endl;
            redisFree(rawCtx);
            return RedisContextPtr(nullptr, redisFree);
        }

        return RedisContextPtr(rawCtx, redisFree);
    }

    std::deque<RedisContextPtr> pool_;
    std::mutex mtx_;
    std::string host_;
    int port_;
    size_t poolSize_;
    timeval connectionTimeout_;
    timeval queryTimeout_;
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


struct BatchInfo {
    RedisContextPtr ctx;
    RedisPool* pool;
    std::atomic<bool> completed{false};
    RedisRequest request;
    std::shared_ptr<apct::WaitGroup> wg;
    std::string redisKey;
};


uint32_t murmurHash(const std::string& value, uint32_t seed = 0) {
    uint32_t out;
    MurmurHash3_x86_32(value.data(), value.size(), seed, &out);
    return out;
}


void redisWorker(apct::Channel<std::shared_ptr<BatchInfo>>& asyncCh) {
    while (true) {
        auto [info, ok] = asyncCh.read();
        if (!ok) break;

        redisContext* ctx = info->ctx.get();
        RedisReplyPointer reply(
            static_cast<redisReply*>(
                redisCommand(ctx, "HGET %s %s",
                             info->redisKey.c_str(),
                             info->request.userId.c_str())
            ),
            freeReplyObject
        );

        if (!info->completed.exchange(true)) {
            int err = reply ? REDIS_OK : ctx->err;
            info->request.onFinish(err);
        }

        info->pool->release(std::move(info->ctx));
        info->wg->done();
    }
}


void asyncWorker(
        const uint64_t waitDuration,
        const int batches,
        const std::string& redisKey,
        RedisPoolSet& poolSet,
        apct::Channel<std::string>& userIdCh,
        apct::Channel<std::shared_ptr<BatchInfo>>& asyncCh,
        BenchmarkMetrics& metrics,
        apct::WaitGroup& workerWg) {

    while (true) {
        auto batchWg = std::make_shared<apct::WaitGroup>();
        std::vector<std::shared_ptr<BatchInfo>> batchInfos;
        batchInfos.reserve(batches);

        bool channelClosed = false;
        for (int i = 0; i < batches; ++i) {
            auto [uid, ok] = userIdCh.read();
            if (!ok) {
                channelClosed = true;
                break;
            }

            ++metrics.totalRequests;

            size_t poolIndex = murmurHash(uid) % poolSet.size();
            RedisPool& pool = poolSet.get(poolIndex);

            auto ctx = pool.acquire();
            if (!ctx) {
                ++metrics.totalErrors;
                continue;
            }

            RedisRequest request{
                std::move(uid),
                [&metrics](int errStatus) {
                    if (errStatus == REDIS_OK) {
                        ++metrics.totalSuccess;
                    } else if (errStatus == REDIS_ERR_TIMEOUT) {
                        ++metrics.totalQueryTimeouts;
                    } else if (errStatus == REDIS_ERR_BATCHTIMEOUT) {
                        ++metrics.totalBatchTimeouts;
                    } else {
                        ++metrics.totalErrors;
                    }
                }
            };

            auto batchInfo = std::make_shared<BatchInfo>(std::move(ctx),
                                                     &pool,
                                                     false,
                                                     std::move(request),
                                                     batchWg,
                                                     redisKey);
            batchInfos.push_back(batchInfo);
            batchWg->add(1);
            asyncCh.write(batchInfo);
        }

        bool batchCompleted =
            batchWg->wait(std::chrono::microseconds(waitDuration));

        if (!batchCompleted) {
            for (auto& batchInfo : batchInfos) {
                if (!batchInfo->completed.exchange(true)) {
                    batchInfo->request.onFinish(REDIS_ERR_BATCHTIMEOUT);
                }
            }
            batchWg->wait();
        }

        if (channelClosed) {
            break;
        }
    }

    workerWg.done();
}


void syncWorker(
        const uint64_t waitDuration,
        const int batches,
        const std::string& redisKey,
        RedisPoolSet& poolSet,
        apct::Channel<std::string>& userIdCh,
        BenchmarkMetrics& metrics) {

    while (true) {
        auto batchStart = std::chrono::high_resolution_clock::now();
        bool channelClosed = false;
        bool batchTimedOut = false;
        for (int i = 0; i < batches; ++i) {
            auto [uid, ok] = userIdCh.read();
            if (!ok) {
                channelClosed = true;
                break;
            }

            ++metrics.totalRequests;

            if (batchTimedOut) {
                ++metrics.totalBatchTimeouts;
                continue;
            }

            size_t poolIndex = murmurHash(uid) % poolSet.size();
            RedisPool& pool = poolSet.get(poolIndex);

            auto ctx = pool.acquire();
            if (!ctx) {
                ++metrics.totalErrors;
                continue;
            }

            RedisReplyPointer reply(
                static_cast<redisReply*>(
                    redisCommand(ctx.get(), "HGET %s %s",
                                 redisKey.c_str(),
                                 uid.c_str())
                ),
                freeReplyObject
            );

            auto elapsed =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() -
                    batchStart).count();

            if (elapsed >= waitDuration) {
                batchTimedOut = true;
                ++metrics.totalBatchTimeouts;
                pool.release(std::move(ctx));
                continue;
            }

            if (!reply) {
                if (ctx->err == REDIS_ERR_TIMEOUT) {
                    ++metrics.totalQueryTimeouts;
                } else {
                    ++metrics.totalErrors;
                }
            } else {
                ++metrics.totalSuccess;
            }

            pool.release(std::move(ctx));
        }

        if (channelClosed) {
            break;
        }
    }
}


int main(int argc, char* argv[]) {
    if (argc < 7) {
        std::cerr << "Usage: " << argv[0]
                << " <asyncWorker[true|false]> <wait_duration> <threads>"
                << " <times> <batches> <config.yaml>\n";
        return 1;
    }

    const bool asyncMode = std::string(argv[1]) == "true";
    const uint64_t waitDuration = std::stoull(argv[2]);
    const int threads = std::stoi(argv[3]);
    const int times = std::stoi(argv[4]);
    const int batches = std::stoi(argv[5]);
    const std::string configPath = argv[6];
    YAML::Node config = YAML::LoadFile(configPath);

    const int poolSize = config["pool_size"].as<int>();
    const int connectionTimeoutMs = config["connection_timeout_ms"].as<int>();
    const int queryTimeoutMs = config["query_timeout_ms"].as<int>();
    const std::string userIdFile = config["user_id_file"].as<std::string>();
    const std::string redisKey = config["user_db_key"].as<std::string>();

    timeval connectionTimeout{
        connectionTimeoutMs / 1000,
        (connectionTimeoutMs % 1000) * 1000
    };

    timeval queryTimeout{
        queryTimeoutMs / 1000,
        (queryTimeoutMs % 1000) * 1000
    };

    RedisPoolSet poolSet;
    for (const auto& entry : config["redis_pool_set"]) {
        const auto& server = entry[0];
        poolSet.addPool(std::make_unique<RedisPool>(
            server["host"].as<std::string>(),
            server["port"].as<int>(),
            poolSize,
            connectionTimeout,
            queryTimeout
        ));
    }

    std::vector<std::string> userIds;
    std::ifstream infile(userIdFile);
    for (std::string line; std::getline(infile, line);) {
        if (!line.empty()) userIds.emplace_back(std::move(line));
    }

    apct::Channel<std::string> userIdCh(100);
    std::thread userThread([&] {
        for (size_t n = 0; n < times; ++n) {
            for (const auto& id : userIds) {
                userIdCh.write(id);
            }
        }
        userIdCh.close();
    });

    BenchmarkMetrics metrics;
    std::chrono::high_resolution_clock::time_point benchStart;

    if (asyncMode) {
        std::cout << "Running in asynchronous mode.\n";

        apct::WaitGroup workerWg;
        apct::Channel<std::shared_ptr<BatchInfo>> asyncCh(1000);

        std::vector<std::thread> asyncThreads;
        for (int i = 0; i < threads; ++i)
            asyncThreads.emplace_back(redisWorker, std::ref(asyncCh));

        benchStart = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> workers;
        for (int i = 0; i < threads; ++i) {
            workerWg.add(1);
            workers.emplace_back(asyncWorker, waitDuration, batches,
                                 std::cref(redisKey), std::ref(poolSet),
                                 std::ref(userIdCh), std::ref(asyncCh),
                                 std::ref(metrics), std::ref(workerWg));
        }

        workerWg.wait();
        asyncCh.close();

        for (auto& worker : workers) {
            worker.join();
        }

        for (auto& thread : asyncThreads) {
            thread.join();
        }
    } else {
        std::cout << "Running in synchronous mode.\n";

        std::vector<std::thread> workers;
        workers.reserve(threads);

        benchStart = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < threads; ++i) {
            workers.emplace_back(syncWorker, std::cref(waitDuration),
                                 std::cref(batches), std::cref(redisKey),
                                 std::ref(poolSet), std::ref(userIdCh),
                                 std::ref(metrics));
        }

        for (auto& worker : workers) {
            worker.join();
        }
    }

    userThread.join();
    auto benchEnd = std::chrono::high_resolution_clock::now();

    double elapsedSeconds =
        std::chrono::duration<double>(benchEnd - benchStart).count();
    long long elapsedMicroseconds =
        std::chrono::duration_cast<std::chrono::microseconds>(
            benchEnd - benchStart).count();
    double average = static_cast<double>(elapsedMicroseconds) / metrics.totalRequests;
    std::cout << "\n--- Benchmark Results ---\n";
    std::cout << "Total Requests: " << metrics.totalRequests << "\n";
    std::cout << "Success:        " << metrics.totalSuccess << "\n";
    std::cout << "Query Timeouts:       " << metrics.totalQueryTimeouts << "\n";
    std::cout << "Batch Timeouts:       " << metrics.totalBatchTimeouts << "\n";
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
