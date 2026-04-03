#!/bin/bash
# Setup test indices and data for analytics engine development.
# Usage: ./sandbox/scripts/setup-test-data.sh [host:port]

HOST="${1:-localhost:9200}"

echo "=== Setting up test data on $HOST ==="

# Delete existing indices (ignore errors if they don't exist)
curl -s -X DELETE "$HOST/parquet_logs" > /dev/null 2>&1
curl -s -X DELETE "$HOST/parquet_metrics" > /dev/null 2>&1

# Create parquet_logs
echo -n "Creating parquet_logs... "
curl -s -X PUT "$HOST/parquet_logs" -H "Content-Type: application/json" -d '{
  "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
  "mappings": {
    "properties": {
      "ts": { "type": "date" },
      "status": { "type": "integer" },
      "message": { "type": "keyword" },
      "ip_addr": { "type": "keyword" }
    }
  }
}' | jq -r '.acknowledged // .error.reason'

# Create parquet_metrics
echo -n "Creating parquet_metrics... "
curl -s -X PUT "$HOST/parquet_metrics" -H "Content-Type: application/json" -d '{
  "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
  "mappings": {
    "properties": {
      "ts": { "type": "date" },
      "cpu": { "type": "double" },
      "memory": { "type": "double" },
      "host": { "type": "keyword" }
    }
  }
}' | jq -r '.acknowledged // .error.reason'

# Insert parquet_logs data
echo -n "Inserting parquet_logs data... "
curl -s -X POST "$HOST/parquet_logs/_bulk?refresh=true" -H "Content-Type: application/x-ndjson" -d '
{"index":{}}
{"ts":"2024-01-15T10:30:00Z","status":200,"message":"Request completed","ip_addr":"192.168.1.1"}
{"index":{}}
{"ts":"2024-01-15T10:31:00Z","status":200,"message":"Health check OK","ip_addr":"192.168.1.2"}
{"index":{}}
{"ts":"2024-01-15T10:32:00Z","status":500,"message":"Internal server error","ip_addr":"192.168.1.3"}
{"index":{}}
{"ts":"2024-01-15T10:33:00Z","status":200,"message":"Request completed","ip_addr":"192.168.1.4"}
{"index":{}}
{"ts":"2024-01-15T10:34:00Z","status":404,"message":"Not found","ip_addr":"192.168.1.5"}
' | jq -r '"items=\(.items | length), errors=\(.errors)"'

# Insert parquet_metrics data
echo -n "Inserting parquet_metrics data... "
curl -s -X POST "$HOST/parquet_metrics/_bulk?refresh=true" -H "Content-Type: application/x-ndjson" -d '
{"index":{}}
{"ts":"2024-01-15T10:30:00Z","cpu":75.5,"memory":8192.5,"host":"host-1"}
{"index":{}}
{"ts":"2024-01-15T10:31:00Z","cpu":82.3,"memory":7680.5,"host":"host-2"}
{"index":{}}
{"ts":"2024-01-15T10:32:00Z","cpu":45.1,"memory":6144.0,"host":"host-1"}
{"index":{}}
{"ts":"2024-01-15T10:33:00Z","cpu":91.7,"memory":7200.0,"host":"host-3"}
' | jq -r '"items=\(.items | length), errors=\(.errors)"'

# Verify
echo ""
echo "=== Verification ==="
echo -n "parquet_logs count: "
curl -s "$HOST/parquet_logs/_count" | jq -r '.count'
echo -n "parquet_metrics count: "
curl -s "$HOST/parquet_metrics/_count" | jq -r '.count'

echo ""
echo "=== Sample queries ==="
echo 'curl -X POST "'$HOST'/_plugins/_ppl" -H "Content-Type: application/json" -d '"'"'{"query": "source = parquet_logs | fields ts, status, message"}'"'"''
echo 'curl -X POST "'$HOST'/_plugins/_ppl" -H "Content-Type: application/json" -d '"'"'{"query": "source = parquet_logs | where status = 200 | stats count() as cnt"}'"'"''
echo 'curl -X POST "'$HOST'/_plugins/_ppl" -H "Content-Type: application/json" -d '"'"'{"query": "source = parquet_metrics | stats avg(cpu) as avg_cpu by host"}'"'"''
