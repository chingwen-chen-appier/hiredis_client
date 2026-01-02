#include "hiredis_client.hpp"

#include <appier/cntk/toolbox/config.hpp>
#include <appier/cntk/toolbox/data_id.h>

#include "MurmurHash3.h"

#include <chrono>
#include <thread>
#include <fstream>
#include <cerrno>


namespace {


uint32_t murmurHash(const std::string& value, uint32_t seed = 0) {
    uint32_t out;
    MurmurHash3_x86_32(value.data(), value.size(), seed, &out);
    return out;
}


void recordMetrics(const RedisRequest& request, BenchmarkMetrics& metrics) {
    if (request.error == REDIS_OK) {
        ++metrics.totalSuccess;
    } else if (request.error == REDIS_ERR_TIMEOUT) {
        ++metrics.totalQueryTimeouts;
    } else if (request.error == REDIS_ERR_IO &&
              (request.savedErrno == EAGAIN ||
               request.savedErrno == ETIMEDOUT)) {
        ++metrics.totalQueryTimeouts;
    } else if (request.error == REDIS_ERR_BATCHTIMEOUT) {
        ++metrics.totalBatchTimeouts;
    } else {
        ++metrics.totalErrors;
    }
}


void asyncRedisWorker(apct::Channel<std::shared_ptr<BatchInfo>>& asyncCh) {
    while (true) {
        auto [info, ok] = asyncCh.read();
        if (!ok) break;

        redisContext* ctx = info->ctx.get();
        RedisReplyPtr reply(
            static_cast<redisReply*>(
                redisCommand(ctx, "HGET %s %s",
                             info->redisKey.c_str(),
                             info->request.userId.c_str())
            ),
            freeReplyObject
        );

        auto expected = RequestState::IN_PROGRESS;
        if (info->request.state.compare_exchange_strong(
                expected, RequestState::DONE, std::memory_order_acq_rel)) {
            info->request.error = reply ? REDIS_OK : ctx->err;
            info->request.savedErrno = errno;
        } else {
            info->request.error = REDIS_ERR_BATCHTIMEOUT;
        }

        info->pool->release(std::move(info->ctx));
        info->wg->done();
    }
}


void asyncWorker(
        const uint64_t waitDuration,
        const size_t batches,
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
        for (size_t i = 0; i < batches; ++i) {
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
                    RequestState::BATCH_TIMEOUT, std::memory_order_release);
            }

            batchWg->wait();
        }

        for (auto& batchInfo : batchInfos) {
            recordMetrics(batchInfo->request, metrics);
        }

        if (channelClosed) {
            break;
        }
    }
}


void syncWorker(
        const uint64_t waitDuration,
        const size_t batches,
        const std::string& redisKey,
        RedisPoolSet& poolSet,
        apct::Channel<std::string>& userIdCh,
        BenchmarkMetrics& metrics) {
    while (true) {
        auto batchDeadline = std::chrono::high_resolution_clock::now() +
                             std::chrono::microseconds(waitDuration);
        std::vector<std::shared_ptr<RedisRequest>> requests;
        requests.reserve(batches);

        bool channelClosed = false;
        bool batchTimedOut = false;
        for (size_t i = 0; i < batches; ++i) {
            auto [uid, ok] = userIdCh.read();
            if (!ok) {
                channelClosed = true;
                break;
            }

            ++metrics.totalRequests;

            auto request = std::make_shared<RedisRequest>(std::move(uid));

            if (batchTimedOut) {
                request->error = REDIS_ERR_BATCHTIMEOUT;
                requests.push_back(request);
                continue;
            }

            size_t poolIndex = murmurHash(request->userId) % poolSet.size();
            RedisPool& pool = poolSet.get(poolIndex);

            auto ctx = pool.acquire();
            if (!ctx) {
                request->error = REDIS_ERR;
                requests.push_back(request);
                continue;
            }

            RedisReplyPtr reply(
                static_cast<redisReply*>(
                    redisCommand(ctx.get(), "HGET %s %s",
                                 redisKey.c_str(),
                                 request->userId.c_str())
                ),
                freeReplyObject
            );

            int error = reply ? REDIS_OK : ctx->err;
            int savedErrno = errno;

            auto now = std::chrono::high_resolution_clock::now();
            if (now > batchDeadline) {
                error = REDIS_ERR_BATCHTIMEOUT;
                batchTimedOut = true;
            }

            request->error = error;
            request->savedErrno = savedErrno;
            requests.push_back(request);
            pool.release(std::move(ctx));
        }

        for (const auto& request : requests) {
            recordMetrics(*request, metrics);
        }

        if (channelClosed) {
            break;
        }
    }
}


} // namespace


// RedisBenchCommand implementation
RedisBenchCommand::RedisBenchCommand()
    : apct::Command("bench", "Run Redis benchmark") { }

void RedisBenchCommand::setup() {
        auto app = cliApp();
        app->add_option("--key", key_, "Redis hash key for HGET operations")
            ->required();
        app->add_option("--input",
                        input_,
                        "Input file containing hash fields for HGET operations")
            ->required();
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
        app->add_option("--async_threads",
                        asyncThreadCount_,
                        "Number of async Redis worker threads "
                        "(async mode only)")
            ->default_val(0);
}

std::pair<int, bool> RedisBenchCommand::forwardRun() {
    auto sdata = apct::fetchData<BenchSharedData>(
        commandMain()->sharedData(), BENCH_CMD_DATA_ID);

    const auto& config = sdata->config();

    const int poolSize = config["pool_size"].as<int>();
    const int connectionTimeoutMs =
        config["connection_timeout_ms"].as<int>();
    const int queryTimeoutMs = config["query_timeout_ms"].as<int>();

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
    std::ifstream infile(input_);
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
        for (size_t i = 0; i < asyncThreadCount_; ++i)
            asyncThreads.emplace_back(asyncRedisWorker, std::ref(asyncCh));

        benchStart = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> workers;
        for (size_t i = 0; i < workerThreadCount_; ++i) {
            workers.emplace_back(asyncWorker, waitDuration_, batches_,
                                 std::cref(key_), std::ref(poolSet),
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

        benchStart = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> workers;
        for (size_t i = 0; i < workerThreadCount_; ++i) {
            workers.emplace_back(syncWorker, std::cref(waitDuration_),
                                 std::cref(batches_), std::cref(key_),
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


// MainCommand implementation
MainCommand::MainCommand() : apct::Command("Redis Benchmark Tool") { }

void MainCommand::setup() {
    auto app = cliApp();
    app->add_option("--config,-c",
                    configPath_,
                    "Path to the configuration YAML file")
        ->required();

    addSubcommand(std::make_unique<RedisBenchCommand>());
}

std::pair<int, bool> MainCommand::forwardRun() {
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