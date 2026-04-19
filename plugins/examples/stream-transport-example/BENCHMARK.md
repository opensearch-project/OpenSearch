# Benchmark API

The Benchmark API allows you to compare the performance of Stream Transport (Arrow Flight) vs Regular Transport (Netty4) for node-to-node communication in OpenSearch.

## Package

`org.opensearch.example.stream.benchmark`

## Endpoint

```
POST /_benchmark/stream
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `rows` | int | 100 | Number of rows per request |
| `columns` | int | 10 | Number of columns per row |
| `avg_column_length` | int | 100 | Average column length in bytes |
| `parallel_requests` | int | 1 | Number of parallel requests to execute |
| `total_requests` | int | 0 | Total requests to send (0 = use parallel_requests only) |
| `batch_size` | int | 100 | Rows per batch for stream transport |
| `use_stream_transport` | boolean | true | Use stream transport (true) or regular transport (false) |

## Examples

### Stream Transport (Arrow Flight)

```bash
curl -X POST "localhost:9200/_benchmark/stream?rows=100000&parallel_requests=10&total_requests=600&batch_size=10000&use_stream_transport=true&pretty&human=true"
```

**Response:**
```json
{
  "total_rows" : 60000000,
  "total_bytes" : 57970000000,
  "total_size" : "53.9gb",
  "total_size_bytes" : 57970000000,
  "duration_ms" : 47730,
  "throughput_rows_per_sec" : "1257071.02",
  "throughput_mb_per_sec" : "1158.28",
  "latency_ms" : {
    "min" : 179,
    "max" : 1862,
    "avg" : 1578,
    "p5" : 1134,
    "p10" : 1425,
    "p20" : 1481,
    "p25" : 1495,
    "p35" : 1532,
    "p50" : 1622,
    "p75" : 1717,
    "p90" : 1791,
    "p99" : 1838
  },
  "parallel_requests" : 10,
  "used_stream_transport" : true
}
```

### Regular Transport (Netty4)

```bash
curl -X POST "localhost:9200/_benchmark/stream?rows=100000&parallel_requests=10&total_requests=600&batch_size=10000&use_stream_transport=false&pretty&human=true"
```

**Response:**
```json
{
  "total_rows" : 60000000,
  "total_bytes" : 57970000000,
  "total_size" : "53.9gb",
  "total_size_bytes" : 57970000000,
  "duration_ms" : 52100,
  "throughput_rows_per_sec" : "1151632.57",
  "throughput_mb_per_sec" : "1061.45",
  "latency_ms" : {
    "min" : 195,
    "max" : 2012,
    "avg" : 1723,
    "p5" : 1245,
    "p10" : 1567,
    "p20" : 1623,
    "p25" : 1645,
    "p35" : 1689,
    "p50" : 1756,
    "p75" : 1834,
    "p90" : 1923,
    "p99" : 1987
  },
  "parallel_requests" : 10,
  "used_stream_transport" : false
}
```

## Response Fields

| Field | Description |
|-------|-------------|
| `total_rows` | Total number of rows processed |
| `total_bytes` | Total bytes transferred |
| `total_size` | Human-readable size (with `human=true`) |
| `duration_ms` | Total duration in milliseconds |
| `throughput_rows_per_sec` | Rows processed per second |
| `throughput_mb_per_sec` | Megabytes transferred per second |
| `latency_ms.min` | Minimum request latency |
| `latency_ms.max` | Maximum request latency |
| `latency_ms.avg` | Average request latency |
| `latency_ms.p5` - `latency_ms.p99` | Latency percentiles |
| `parallel_requests` | Number of parallel requests used |
| `used_stream_transport` | Whether stream transport was used |

## Use Cases

### 1. Compare Transport Performance

```bash
# Stream transport
curl -X POST "localhost:9200/_benchmark/stream?rows=10000&parallel_requests=10&use_stream_transport=true&pretty"

# Regular transport
curl -X POST "localhost:9200/_benchmark/stream?rows=10000&parallel_requests=10&use_stream_transport=false&pretty"
```

### 2. Test High Throughput

```bash
curl -X POST "localhost:9200/_benchmark/stream?rows=100000&parallel_requests=50&total_requests=1000&batch_size=10000&use_stream_transport=true&pretty&human=true"
```

### 3. Test Large Payloads

```bash
curl -X POST "localhost:9200/_benchmark/stream?rows=50000&columns=50&avg_column_length=1000&parallel_requests=5&use_stream_transport=true&pretty&human=true"
```

### 4. Latency Analysis

```bash
curl -X POST "localhost:9200/_benchmark/stream?rows=1000&parallel_requests=100&total_requests=1000&use_stream_transport=true&pretty" | jq '.latency_ms'
```

## Performance Tips

1. **Parallel Requests**: The API maintains a cushion of 2x `parallel_requests` in-flight to maximize throughput
2. **Batch Size**: For stream transport, larger batch sizes (10000+) reduce overhead
3. **Total Requests**: Use `total_requests` to send more requests than `parallel_requests` for sustained load testing
4. **Network**: Run benchmarks on nodes with good network connectivity for accurate results

## Implementation Details

- **Batching**: Maintains 2x parallel_requests in-flight using lock-free refill mechanism
- **Thread Pools**: Uses dedicated `benchmark` and `benchmark_response` thread pools
- **Data Generation**: Generates synthetic data with non-compressible patterns
- **Latency Tracking**: Measures end-to-end latency including all batches for stream transport

## Integration Tests

See `BenchmarkStreamIT.java` for comprehensive test examples covering:
- Basic benchmarks
- Parallel requests
- Stream vs regular transport comparison
- Large payloads
- Custom batch sizes
- Latency percentile validation
