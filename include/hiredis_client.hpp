#pragma once

#include <appier/cntk/toolbox/toolbox.hpp>
#include <appier/cntk/toolbox/command_line.hpp>
#include <appier/cntk/toolbox/data.hpp>

#include <hiredis/hiredis.h>
#include <yaml-cpp/yaml.h>

#include <memory>
#include <string>
#include <vector>
#include <deque>
#include <atomic>
#include <mutex>
#include <iostream>


namespace apct = appier::cntk::toolbox;


constexpr int REDIS_ERR_BATCHTIMEOUT = 100;
const uint32_t BENCH_CMD_DATA_ID = APCT_MAX_DATA_ID - 1;
enum class RequestState { IN_PROGRESS, DONE, BATCH_TIMEOUT };


using RedisReplyPtr = std::unique_ptr<redisReply, decltype(&freeReplyObject)>;
using RedisContextPtr = std::unique_ptr<redisContext, decltype(&redisFree)>;


struct BenchmarkMetrics {
    std::atomic<size_t> totalRequests{0};
    std::atomic<size_t> totalSuccess{0};
    std::atomic<size_t> totalErrors{0};
    std::atomic<size_t> totalQueryTimeouts{0};
    std::atomic<size_t> totalBatchTimeouts{0};
};


class RedisPool {
public:
    RedisPool(const std::string& host, int port, int poolSize,
              const timeval& connectionTimeout, const timeval& queryTimeout)
        : host_(host), port_(port), poolSize_(poolSize),
          connectionTimeout_(connectionTimeout),
          queryTimeout_(queryTimeout) {
        for (size_t i = 0; i < poolSize_; ++i) {
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


struct RedisRequest {
    std::string userId;
    int error{0};
    int savedErrno{0};
    std::atomic<RequestState> state{RequestState::IN_PROGRESS};

    RedisRequest() = default;

    RedisRequest(std::string userId_) : userId(std::move(userId_)) {}
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
              std::shared_ptr<apct::WaitGroup> wg_,
              std::string redisKey_)
        : ctx(std::move(ctx_)),
          pool(pool_),
          request(std::move(userId_)),
          wg(std::move(wg_)),
          redisKey(std::move(redisKey_)) {}
};


class RedisBenchCommand : public apct::Command {
public:
    RedisBenchCommand();

private:
    void setup() override;
    std::pair<int, bool> forwardRun() override;

    std::string key_;
    std::string input_;
    bool asyncMode_ = false;
    bool recordTimeoutKeys_ = false;
    uint64_t waitDuration_ = 10000;
    size_t workerThreadCount_ = 1;
    size_t times_ = 1;
    size_t batches_ = 10;
    size_t asyncThreadCount_ = 1;
};


class MainCommand : public apct::Command {
public:
    MainCommand();

private:
    void setup() override;
    std::pair<int, bool> forwardRun() override;

    std::string configPath_;
};
