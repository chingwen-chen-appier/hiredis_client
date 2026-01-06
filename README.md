# Redis Benchmark Tool

A hiredis benchmarking tool for testing synchronous and asynchronous HGET operations with advanced connection pooling, batch processing, and timeout management capabilities.

## Building

### Prerequisites

- C++23 compiler
- CMake 3.20+
- hiredis library
- Appier cntk and xdep

### Build Instructions

```bash
cd hiredis_client
cmake --workflow --preset default_release
```

The executable will be located at:
```
workspace/install/bin/hiredis_client
```

### Running with Proper Library Path

```bash
export LD_LIBRARY_PATH=/opt/appier/cntk/lib:/opt/appier/xdep/lib
```

## Usage

### Basic Command

```bash
./hiredis_client --config conf/bench.yaml bench \
  --key "my_hash_key" \
  --input user_ids_path \
  --workers 4 \
  --batches 10 \
  --wait 1000 \
  --times 1
```

### Configuration File (bench.yaml)

```yaml
redis_pool_set:
    - - host: localhost
        port: 6379
pool_size: 10
connection_timeout_ms: 1000
query_timeout_ms: 1000
```

### Command Options

#### Required Options

| Option | Description |
|--------|-------------|
| `--config` | Path to YAML configuration file |
| `--key` | Redis hash key for HGET operations |
| `--input` | Path to input file containing hash fields (one per line) |

#### Optional Parameters

| Option | Description | Default |
|--------|-------------|---------|
| `--async` | Enable asynchronous mode | `false` |
| `--workers` | Number of worker threads processing batches | `1` |
| `--batches` | Number of requests per batch | `10` |
| `--wait` | Batch timeout in microseconds (operations exceeding this are marked as batch timeouts) | `10000` |
| `--times` | Number of iterations through the entire input file | `1` |
| `--async_threads` | Number of async Redis worker threads (async mode only) | `1` |
| `--timeout_keys` | Output list of keys that timed out during the benchmark | `false` |

#### Configuration File Parameters (bench.yaml)

| Parameter | Description |
|-----------|-------------|
| `redis_pool_set` | List of Redis server pools (supports multiple servers for sharding)
| `pool_size` | Number of persistent connections per pool
| `connection_timeout_ms` | Timeout for establishing Redis connections
| `query_timeout_ms` | Timeout for individual Redis query operations

## Examples

### Synchronous Benchmark

```bash
# Run with 4 worker threads, batches of 20 requests, 100μs batch timeout
./hiredis_client --config conf/bench.yaml bench --key "user_db" --input ../user_ids --workers 4 --batches 20 --wait 100 --times 10
```

### Asynchronous Benchmark

```bash
# Run with 4 worker threads, 8 async Redis threads, batches of 20 requests, 100μs batch timeout
./hiredis_client --config conf/bench.yaml bench --key "user_db" --input ../user_ids --workers 4 --batches 20 --wait 100 --times 10 --async --async_threads 8
```

### Display Timeout Keys

```bash
# Record and display keys that timed out
./hiredis_client --config conf/bench.yaml bench --key "user_db" --input ../user_ids --workers 4 --batches 20 --wait 100 --times 10 --timeout_keys
```

## Benchmark Results

The tool outputs comprehensive metrics after completion:

```
--- Benchmark Results ---
Operations: 100000
Total Duration: 0.345806s
QPS: 289179
Total OP Average: 3us
Total Batch Average: 17us
OP Average: 6us
Batch Average: 34us
Success:        100000
Query Timeouts: 0
Batch Timeouts: 0
Errors:         0
Timeout keys:
timeoutkey_1
timeoutkey_2
```

### Metrics Explained
- **Operations**: Total number of Redis HGET operations executed
- **Total Duration**: Wall-clock time from benchmark start to completion (seconds)
- **QPS**: Queries per second
- **Total OP Average**: Average time per operation from the total duration perspective (Total Duration ÷ Operations)
- **Total Batch Average**: Average time per batch from the total duration perspective (Total Duration ÷ Total Batches)
- **OP Average**: Average processing time per operation from worker thread perspective (Sum of Thread Durations ÷ Operations)
- **Batch Average**: Average processing time per batch from worker thread perspective (Sum of Thread Durations ÷ Total Batches)
- **Success**: Number of operations completed successfully
- **Query Timeouts**: Operations that exceeded the query timeout set in configuration
- **Batch Timeouts**: Operations cancelled because the batch exceeded the `--wait` duration
- **Errors**: Other errors
- **Timeout keys**: List of hash fields that timed out (only shown when `--timeout_keys` is enabled)
