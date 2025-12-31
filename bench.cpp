#include <appier/cntk/toolbox/toolbox.hpp>
#include <appier/cntk/toolbox/command_line.hpp>
#include <appier/cntk/toolbox/config.hpp>
#include <appier/cntk/toolbox/data_id.h>
#include <appier/cntk/toolbox/data.hpp>

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
#include <cerrno>

#include <hiredis/hiredis.h>
#include <yaml-cpp/yaml.h>
#include "MurmurHash3.h"


namespace apct = appier::cntk::toolbox;


namespace {


constexpr int REDIS_ERR_BATCHTIMEOUT = 100;
const uint32_t BENCH_CMD_DATA_ID = APCT_MAX_DATA_ID - 1;
enum class RequestState { Pending, Completed, BatchTimeout };


using RedisReplyPointer =
    std::unique_ptr<redisReply, decltype(&freeReplyObject)>;
using RedisContextPtr = std::unique_ptr<redisContext, decltype(&redisFree)>;


class BenchSharedData : public apct::Data {
public:
    BenchSharedData() : apct::Data(BENCH_CMD_DATA_ID) { }

    std::shared_ptr<Data> clone() const override {
        return std::make_shared<BenchSharedData>(*this);
    }

    void setConfig(const YAML::Node& conf) {
        config_ = conf;
    }

    const YAML::Node& config() const noexcept {
        return config_;
    }

private:
    YAML::Node config_;
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
    RedisPool(const std::string& host, const int port, const int poolSize,
        const timeval& connectionTimeout, const timeval& queryTimeout)
            : host_(host), port_(port), poolSize_(poolSize),
              connectionTimeout_(connectionTimeout),
              queryTimeout_(queryTimeout) {
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

private:
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


struct RedisRequest {
    std::string userId;
    std::function<void(int err)> onFinish;
    std::atomic<RequestState> state{RequestState::Pending};

    RedisRequest(std::string userId_,
                 std::function<void(int err)> onFinish_)
        : userId(std::move(userId_)),
          onFinish(std::move(onFinish_)) {}
};


struct BatchInfo {
    RedisContextPtr ctx;
    RedisPool* pool;
    RedisRequest request;
    std::shared_ptr<apct::WaitGroup> wg;
    std::string redisKey;

    BatchInfo(RedisContextPtr ctx_,
              RedisPool* pool_,
              std::string userId_,
              std::function<void(int err)> onFinish_,
              std::shared_ptr<apct::WaitGroup> wg_,
              std::string redisKey_)
        : ctx(std::move(ctx_)),
          pool(pool_),
          request(std::move(userId_), std::move(onFinish_)),
          wg(std::move(wg_)),
          redisKey(std::move(redisKey_)) {}
};


uint32_t murmurHash(const std::string& value, uint32_t seed = 0) {
    uint32_t out;
    MurmurHash3_x86_32(value.data(), value.size(), seed, &out);
    return out;
}


void asyncRedisWorker(apct::Channel<std::shared_ptr<BatchInfo>>& asyncCh) {
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

        auto expected = RequestState::Pending;
        if (info->request.state.compare_exchange_strong(
                expected, RequestState::Completed, std::memory_order_acq_rel)) {
            int err = reply ? REDIS_OK : ctx->err;
            info->request.onFinish(err);
        } else {
            info->request.onFinish(REDIS_ERR_BATCHTIMEOUT);
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
        BenchmarkMetrics& metrics) {

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

            auto batchInfo = std::make_shared<BatchInfo>(
                std::move(ctx),
                &pool,
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
                },
                batchWg,
                redisKey
            );
            batchInfos.push_back(batchInfo);
            batchWg->add(1);
            asyncCh.write(batchInfo);
        }

        bool batchCompleted =
            batchWg->wait(std::chrono::microseconds(waitDuration));

        if (!batchCompleted) {
            for (auto& batchInfo : batchInfos) {
                batchInfo->request.state.store(
                    RequestState::BatchTimeout, std::memory_order_release);
            }

            batchWg->wait();
        }

        if (channelClosed) {
            break;
        }
    }
}


void syncWorker(
        const uint64_t waitDuration,
        const int batches,
        const std::string& redisKey,
        RedisPoolSet& poolSet,
        apct::Channel<std::string>& userIdCh,
        BenchmarkMetrics& metrics) {
    while (true) {
        auto batchDeadline = std::chrono::high_resolution_clock::now() +
                             std::chrono::microseconds(waitDuration);
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

            auto now = std::chrono::high_resolution_clock::now();

            if (now > batchDeadline) {
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


class RedisBenchCommand : public apct::Command {
public:
    RedisBenchCommand() : apct::Command("bench", "Run Redis benchmark") { }

private:
    void setup() override {
        auto app = cliApp();
        app->add_flag("--async",
                      asyncMode_,
                      "Use asynchronous operations for benchmark");
        app->add_option("--wait",
                        waitDuration_,
                        "Wait duration (in microseconds) for batch timeout")
            ->default_val(10000);
        app->add_option("--workers",
                        workerThreadCount_,
                        "Number of worker threads")
            ->default_val(1);
        app->add_option("--times",
                        times_,
                        "Number of times to iterate through user IDs")
            ->default_val(1);
        app->add_option("--batches",
                        batches_,
                        "Number of requests per batch")
            ->default_val(10);
        app->add_option("--async-threads",
                        asyncThreadCount_,
                        "Number of async Redis worker threads "
                        "(async mode only)")
            ->default_val(0);
    }

    std::pair<int, bool> forwardRun() override {
        auto sdata = apct::fetchData<BenchSharedData>(
            commandMain()->sharedData(), BENCH_CMD_DATA_ID);

        const auto& config = sdata->config();

        const int poolSize = config["pool_size"].as<int>();
        const int connectionTimeoutMs =
            config["connection_timeout_ms"].as<int>();
        const int queryTimeoutMs = config["query_timeout_ms"].as<int>();
        const std::string userIdFile = config["user_id_file"].as<std::string>();
        const std::string redisKey = config["user_db_key"].as<std::string>();

        timeval connectionTimeout{connectionTimeoutMs / 1000,
                                  (connectionTimeoutMs % 1000) * 1000};
        timeval queryTimeout{queryTimeoutMs / 1000,
                             (queryTimeoutMs % 1000) * 1000};

        RedisPoolSet poolSet;
        for (const auto& redisPool : config["redis_pool_set"]) {
            for (const auto& poolObject : redisPool) {
                poolSet.addPool(std::make_unique<RedisPool>(
                    poolObject["host"].as<std::string>(),
                    poolObject["port"].as<int>(),
                    poolSize,
                    connectionTimeout,
                    queryTimeout
                ));
            }
        }

        std::vector<std::string> userIds;
        std::ifstream infile(userIdFile);
        for (std::string line; std::getline(infile, line);) {
            if (!line.empty()) userIds.emplace_back(std::move(line));
        }

        if (asyncMode_ && asyncThreadCount_ == 0) {
            asyncThreadCount_ = workerThreadCount_;
        }

        apct::Channel<std::string> userIdCh(1000);
        std::thread userThread([&] {
            for (size_t n = 0; n < times_; ++n) {
                for (const auto& id : userIds) {
                    userIdCh.write(id);
                }
            }
            userIdCh.close();
        });

        BenchmarkMetrics metrics;
        std::chrono::high_resolution_clock::time_point benchStart;

        if (asyncMode_) {
            std::cout << "Running in asynchronous mode.\n";

            apct::Channel<std::shared_ptr<BatchInfo>> asyncCh(1000);
            std::vector<std::thread> asyncThreads;
            for (int i = 0; i < asyncThreadCount_; ++i)
                asyncThreads.emplace_back(asyncRedisWorker, std::ref(asyncCh));

            benchStart = std::chrono::high_resolution_clock::now();

            std::vector<std::thread> workers;
            for (int i = 0; i < workerThreadCount_; ++i) {
                workers.emplace_back(asyncWorker, waitDuration_, batches_,
                                     std::cref(redisKey), std::ref(poolSet),
                                     std::ref(userIdCh), std::ref(asyncCh),
                                     std::ref(metrics));
            }

            for (auto& worker : workers) {
                worker.join();
            }

            asyncCh.close();

            for (auto& thread : asyncThreads) {
                thread.join();
            }
        } else {
            std::cout << "Running in synchronous mode.\n";

            std::vector<std::thread> workers;
            workers.reserve(workerThreadCount_);

            benchStart = std::chrono::high_resolution_clock::now();

            for (int i = 0; i < workerThreadCount_; ++i) {
                workers.emplace_back(syncWorker, std::cref(waitDuration_),
                                     std::cref(batches_), std::cref(redisKey),
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
        double average = elapsedMicroseconds / metrics.totalRequests;

        std::cout << "\n--- Benchmark Results ---\n";
        std::cout << "Total Requests: " << metrics.totalRequests << "\n";
        std::cout << "Success:        " << metrics.totalSuccess << "\n";
        std::cout << "Query Timeouts: " << metrics.totalQueryTimeouts << "\n";
        std::cout << "Batch Timeouts: " << metrics.totalBatchTimeouts << "\n";
        std::cout << "Errors:         " << metrics.totalErrors << "\n";
        std::cout << "Elapsed Time:   " << elapsedSeconds << " s\n";
        std::cout << "Average req time:   "
                  << average
                  << " us\n";
        std::cout << "Average req time (Thread):   "
                  << average * workerThreadCount_
                  << " us\n";

        return { 0, false };
    }

private:
    bool asyncMode_ = false;
    uint64_t waitDuration_ = 1000;
    int workerThreadCount_ = 1;
    int times_ = 1;
    int batches_ = 100;
    int asyncThreadCount_ = 0;
};


class MainCommand : public apct::Command {
public:
    MainCommand() : apct::Command("Redis Benchmark Tool") { }

private:
    void setup() override {
        auto app = cliApp();
        app->add_option("--config,-c",
                        configPath_,
                        "Path to the configuration YAML file")
            ->required();

        addSubcommand(std::make_unique<RedisBenchCommand>());
    }

    std::pair<int, bool> forwardRun() override {
        if (configPath_.empty()) {
            std::cerr << "Configuration file is required.\n";
            return { -1, false };
        }

        YAML::Node config = YAML::LoadFile(configPath_);

        auto sharedData = std::make_shared<BenchSharedData>();
        sharedData->setConfig(config);
        commandMain()->sharedData().addData(std::move(sharedData));

        return { 0, true };
    }

private:
    std::string configPath_;
};


} // namespace


int main(int argc, char* argv[]) {
    try {
        return apct::CommandMain(std::make_unique<MainCommand>())(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return -1;
    } catch (...) {
        std::cerr << "Unknown error occurred." << std::endl;
        return -1;
    }
}