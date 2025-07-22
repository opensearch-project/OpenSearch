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
| `response_bytes` | Total size of responses received by clients |

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
| `response_bytes` | Total size of responses sent by servers |

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
| `arrow_allocated_bytes` | Current Arrow memory allocation in bytes |
| `arrow_peak_bytes` | Peak Arrow memory allocation in bytes |
| `direct_memory_bytes` | Current direct memory usage in bytes |
| `client_threads_active` | Number of active client threads |
| `client_threads_total` | Total number of client threads |
| `server_threads_active` | Number of active server threads |
| `server_threads_total` | Total number of server threads |
| `client_channels_active` | Number of active client channels |
| `server_channels_active` | Number of active server channels |
| `client_thread_utilization_percent` | Percentage of client threads that are active |
| `server_thread_utilization_percent` | Percentage of server threads that are active |

## Cluster-Level Metrics

The API also provides cluster-level aggregated metrics that combine data from all nodes:

```
GET /_flight/stats
```

The response includes a `cluster_stats` section with aggregated metrics for:

- Client calls and batches
- Server calls and batches
- Average durations and throughput

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
          "started": 100,
          "completed": 98,
          "duration": {
            "count": 98,
            "sum_nanos": 1250000000,
            "min_nanos": 5000000,
            "max_nanos": 50000000,
            "avg_nanos": 12755102
          },
          "request_bytes": {
            "count": 98,
            "sum_bytes": 245000,
            "min_bytes": 1000,
            "max_bytes": 5000,
            "avg_bytes": 2500
          },
          "response_bytes": 980000
        },
        "client_batches": {
          "requested": 150,
          "received": 145,
          "received_bytes": {
            "count": 145,
            "sum_bytes": 980000,
            "min_bytes": 2000,
            "max_bytes": 10000,
            "avg_bytes": 6758
          },
          "processing_time": {
            "count": 145,
            "sum_nanos": 725000000,
            "min_nanos": 1000000,
            "max_nanos": 15000000,
            "avg_nanos": 5000000
          }
        },
        "server_calls": {
          "started": 200,
          "completed": 195,
          "duration": {
            "count": 195,
            "sum_nanos": 2500000000,
            "min_nanos": 8000000,
            "max_nanos": 60000000,
            "avg_nanos": 12820512
          },
          "request_bytes": {
            "count": 195,
            "sum_bytes": 487500,
            "min_bytes": 1000,
            "max_bytes": 5000,
            "avg_bytes": 2500
          },
          "response_bytes": 1950000
        },
        "server_batches": {
          "sent": 390,
          "sent_bytes": {
            "count": 390,
            "sum_bytes": 1950000,
            "min_bytes": 2000,
            "max_bytes": 10000,
            "avg_bytes": 5000
          },
          "processing_time": {
            "count": 390,
            "sum_nanos": 1950000000,
            "min_nanos": 2000000,
            "max_nanos": 20000000,
            "avg_nanos": 5000000
          }
        },
        "status": {
          "client": {
            "OK": 95,
            "CANCELLED": 2,
            "UNAVAILABLE": 1
          },
          "server": {
            "OK": 190,
            "CANCELLED": 3,
            "INTERNAL": 2
          }
        },
        "resources": {
          "arrow_allocated_bytes": 10485760,
          "arrow_peak_bytes": 20971520,
          "direct_memory_bytes": 52428800,
          "client_threads_active": 5,
          "client_threads_total": 10,
          "server_threads_active": 15,
          "server_threads_total": 20,
          "client_channels_active": 25,
          "server_channels_active": 30,
          "client_thread_utilization_percent": 50.0,
          "server_thread_utilization_percent": 75.0
        }
      }
    }
  },
  "cluster_stats": {
    "client": {
      "calls": {
        "started": 100,
        "completed": 98,
        "duration_nanos": 1250000000,
        "avg_duration_nanos": 12755102,
        "request_bytes": 245000,
        "response_bytes": 980000
      },
      "batches": {
        "requested": 150,
        "received": 145,
        "received_bytes": 980000,
        "avg_processing_time_nanos": 5000000
      }
    },
    "server": {
      "calls": {
        "started": 200,
        "completed": 195,
        "duration_nanos": 2500000000,
        "avg_duration_nanos": 12820512,
        "request_bytes": 487500,
        "response_bytes": 1950000
      },
      "batches": {
        "sent": 390,
        "sent_bytes": 1950000,
        "avg_processing_time_nanos": 5000000
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
