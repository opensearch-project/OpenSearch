# Arrow Flight RPC Metrics

The Arrow Flight RPC plugin provides comprehensive metrics to monitor the performance and health of the transport. These metrics are available through the Flight Stats API.

## Accessing Metrics

Metrics can be accessed using the Flight Stats API:

```
GET /_flight/stats
```

This returns metrics for all nodes. To get metrics for a specific node:

```
GET /_flight/stats/{node_id}
```

## Monitoring Streaming Tasks

Streaming transport tasks can be monitored using the existing Tasks API:

```bash
curl "localhost:9200/_cat/tasks?v"
```

Streaming tasks are identified by the `stream-transport` type:

```
action                                task_id                     parent_task_id              type             start_time    timestamp running_time ip        node
indices:data/read/search              TVk0SciMQtSwplV6rQwyMA:2165 -                           transport        1754082449785 21:07:29  169.5ms      127.0.0.1 node-1
indices:data/read/search[phase/query] TVk0SciMQtSwplV6rQwyMA:2166 TVk0SciMQtSwplV6rQwyMA:2165 stream-transport 1754082449786 21:07:29  168.4ms      127.0.0.1 node-1
```

## Metrics Structure

Metrics are organized into the following categories:

### Client Call Metrics

Metrics related to client-side calls:

| Metric | Description |
|--------|-------------|
| `started` | Number of client calls started |
| `completed` | Number of client calls completed |
| `duration` | Duration statistics for client calls (min, max, avg, sum) |
| `request_bytes` | Size statistics for requests sent by clients (min, max, avg, sum) |
| `response` | Total size of responses received by clients (with human-readable format) |
| `response_bytes` | Total size of responses received by clients (in bytes) |

### Client Batch Metrics

Metrics related to client-side batch operations:

| Metric | Description |
|--------|-------------|
| `requested` | Number of batches requested by clients |
| `received` | Number of batches received by clients |
| `received_bytes` | Size statistics for batches received by clients (min, max, avg, sum) |
| `processing_time` | Time statistics for processing received batches (min, max, avg, sum) |

### Server Call Metrics

Metrics related to server-side calls:

| Metric | Description |
|--------|-------------|
| `started` | Number of server calls started |
| `completed` | Number of server calls completed |
| `duration` | Duration statistics for server calls (min, max, avg, sum) |
| `request_bytes` | Size statistics for requests received by servers (min, max, avg, sum) |
| `response` | Total size of responses sent by servers (with human-readable format) |
| `response_bytes` | Total size of responses sent by servers (in bytes) |

### Server Batch Metrics

Metrics related to server-side batch operations:

| Metric | Description |
|--------|-------------|
| `sent` | Number of batches sent by servers |
| `sent_bytes` | Size statistics for batches sent by servers (min, max, avg, sum) |
| `processing_time` | Time statistics for processing and sending batches (min, max, avg, sum) |

### Status Metrics

Metrics related to call status codes:

| Metric | Description |
|--------|-------------|
| `client.{status}` | Count of client calls completed with each status code (OK, CANCELLED, UNAVAILABLE, etc.) |
| `server.{status}` | Count of server calls completed with each status code (OK, CANCELLED, UNAVAILABLE, etc.) |

### Resource Metrics

Metrics related to resource usage:

| Metric | Description |
|--------|-------------|
| `arrow_allocated` | Current Arrow memory allocation (human-readable format) |
| `arrow_allocated_bytes` | Current Arrow memory allocation in bytes |
| `arrow_peak` | Peak Arrow memory allocation (human-readable format) |
| `arrow_peak_bytes` | Peak Arrow memory allocation in bytes |
| `direct_memory` | Current direct memory usage (human-readable format) |
| `direct_memory_bytes` | Current direct memory usage in bytes |
| `client_threads_active` | Number of active client threads |
| `client_threads_total` | Total number of client threads |
| `server_threads_active` | Number of active server threads |
| `server_threads_total` | Total number of server threads |
| `client_channels_active` | Number of active client channels |
| `server_channels_active` | Number of active server channels |


## Cluster-Level Metrics

The API also provides cluster-level aggregated metrics that combine data from all nodes:

```
GET /_flight/stats
```

The response includes a `cluster_stats` section with aggregated metrics for:

- Client calls and batches (aggregated across all nodes)
- Server calls and batches (aggregated across all nodes)
- Average durations and throughput

Note: All duration and size fields include both human-readable formats (e.g., "1s", "24.6kb") and raw values in nanoseconds/bytes.

## Example Response

```json
{
  "cluster_name": "opensearch",
  "nodes": {
    "node_id": {
      "name": "node_name",
      "streamAddress": "localhost:9400",
      "flight_metrics": {
        "client_calls": {
          "started": 6,
          "completed": 6,
          "duration": {
            "count": 6,
            "sum": "1s",
            "sum_nanos": 1019,
            "min": "9ms",
            "min_nanos": 9,
            "max": "743.7ms",
            "max_nanos": 743,
            "avg": "169.8ms",
            "avg_nanos": 169
          },
          "request_bytes": {
            "count": 6,
            "sum": "5.9kb",
            "sum_bytes": 6132,
            "min": "1022b",
            "min_bytes": 1022,
            "max": "1022b",
            "max_bytes": 1022,
            "avg": "1022b",
            "avg_bytes": 1022
          },
          "response": "24.6kb",
          "response_bytes": 25276
        },
        "client_batches": {
          "requested": 6,
          "received": 6,
          "received_bytes": {
            "count": 6,
            "sum": "24.6kb",
            "sum_bytes": 25276,
            "min": "3.3kb",
            "min_bytes": 3477,
            "max": "4.2kb",
            "max_bytes": 4361,
            "avg": "4.1kb",
            "avg_bytes": 4212
          },
          "processing_time": {
            "count": 6,
            "sum": "12.1ms",
            "sum_nanos": 12,
            "min": "352micros",
            "min_nanos": 0,
            "max": "9.5ms",
            "max_nanos": 9,
            "avg": "2ms",
            "avg_nanos": 2
          }
        },
        "server_calls": {
          "started": 3,
          "completed": 3,
          "duration": {
            "count": 3,
            "sum": "147.9ms",
            "sum_nanos": 147,
            "min": "6ms",
            "min_nanos": 6,
            "max": "135.7ms",
            "max_nanos": 135,
            "avg": "49.3ms",
            "avg_nanos": 49
          },
          "request_bytes": {
            "count": 3,
            "sum": "2.9kb",
            "sum_bytes": 3066,
            "min": "1022b",
            "min_bytes": 1022,
            "max": "1022b",
            "max_bytes": 1022,
            "avg": "1022b",
            "avg_bytes": 1022
          },
          "response": "12.7kb",
          "response_bytes": 13083
        },
        "server_batches": {
          "sent": 3,
          "sent_bytes": {
            "count": 3,
            "sum": "12.7kb",
            "sum_bytes": 13083,
            "min": "4.2kb",
            "min_bytes": 4361,
            "max": "4.2kb",
            "max_bytes": 4361,
            "avg": "4.2kb",
            "avg_bytes": 4361
          },
          "processing_time": {
            "count": 3,
            "sum": "6.4ms",
            "sum_nanos": 6,
            "min": "525.4micros",
            "min_nanos": 0,
            "max": "5.3ms",
            "max_nanos": 5,
            "avg": "2.1ms",
            "avg_nanos": 2
          }
        },
        "status": {
          "client": {
            "OK": 6
          },
          "server": {
            "OK": 3
          }
        },
        "resources": {
          "arrow_allocated": "0b",
          "arrow_allocated_bytes": 0,
          "arrow_peak": "48kb",
          "arrow_peak_bytes": 49152,
          "direct_memory": "120.7mb",
          "direct_memory_bytes": 126642920,
          "client_threads_active": 0,
          "client_threads_total": 0,
          "server_threads_active": 0,
          "server_threads_total": 0,
          "client_channels_active": 2,
          "server_channels_active": 1
        }
      }
    }
  },
  "cluster_stats": {
    "client": {
      "calls": {
        "started": 6,
        "completed": 6,
        "duration": "1s",
        "duration_nanos": 1019,
        "avg_duration": "169.8ms",
        "avg_duration_nanos": 169,
        "request": "5.9kb",
        "request_bytes": 6132,
        "response": "24.6kb",
        "response_bytes": 25276
      },
      "batches": {
        "requested": 6,
        "received": 6,
        "received_size": "24.6kb",
        "received_bytes": 25276,
        "avg_processing_time": "2ms",
        "avg_processing_time_nanos": 2
      }
    },
    "server": {
      "calls": {
        "started": 6,
        "completed": 6,
        "duration": "556ms",
        "duration_nanos": 556,
        "avg_duration": "92.6ms",
        "avg_duration_nanos": 92,
        "request": "5.9kb",
        "request_bytes": 6132,
        "response": "24.6kb",
        "response_bytes": 25276
      },
      "batches": {
        "sent": 6,
        "sent_size": "24.6kb",
        "sent_bytes": 25276,
        "avg_processing_time": "34.6ms",
        "avg_processing_time_nanos": 34
      }
    }
  }
}
```

## Interpreting Metrics

### Performance Monitoring

- **High latency**: Check `duration` metrics for client and server calls
- **Memory pressure**: Monitor `arrow_allocated_bytes` and `arrow_peak_bytes`
- **Thread pool saturation**: Check `client_thread_utilization_percent` and `server_thread_utilization_percent`

### Error Detection

- **Failed calls**: Monitor non-OK status counts in `status.client` and `status.server`
- **Cancelled operations**: Check `CANCELLED` status counts
- **Resource exhaustion**: Watch for `RESOURCE_EXHAUSTED` status counts

### Throughput Analysis

- **Request throughput**: Monitor `client_calls.started` and `server_calls.started` rates
- **Data throughput**: Track `client_calls.request_bytes` and `server_batches.sent_bytes` rates
- **Batch efficiency**: Compare `client_batches.received` with `client_batches.requested`
