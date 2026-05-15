#!/bin/bash
# Force docValuesGen != -1 topology and test star tree upgrade
set -e
HOST="localhost:9200"
INDEX="test_dvgen_forced"

echo "=== Step 1: Create index with merges disabled ==="
curl -s -X DELETE "$HOST/$INDEX" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/$INDEX" -H 'Content-Type: application/json' -d '{
  "settings": {
    "index.merge.policy.max_merge_at_once": 1,
    "index.merge.scheduler.max_thread_count": 0,
    "index.number_of_shards": 1,
    "index.number_of_replicas": 0,
    "index.refresh_interval": "-1"
  },
  "mappings": {
    "properties": {
      "day_of_week_i": {"type": "integer"},
      "currency_id": {"type": "integer"},
      "taxful_total_price": {"type": "double"},
      "order_id": {"type": "integer"}
    }
  }
}' > /dev/null
echo "  Created with merges disabled"

echo ""
echo "=== Step 2: Ingest 3 batches with flush after each ==="
python3 -c "
import json, random
random.seed(42)
for batch in range(3):
    with open(f'/tmp/dvgen_batch_{batch}.ndjson', 'w') as f:
        for i in range(batch*10000, (batch+1)*10000):
            f.write(json.dumps({'index': {'_index': '$INDEX', '_id': str(i)}}) + '\n')
            f.write(json.dumps({
                'day_of_week_i': random.randint(0, 6),
                'currency_id': random.randint(1, 3),
                'taxful_total_price': round(random.uniform(5, 500), 2),
                'order_id': i
            }) + '\n')
"
for BATCH in 0 1 2; do
  curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/dvgen_batch_${BATCH}.ndjson > /dev/null
  curl -s -X POST "$HOST/$INDEX/_flush?force=true" > /dev/null
  echo "  Batch $BATCH: flushed"
done

echo ""
echo "=== Step 3: Delete docs spanning ALL segments ==="
python3 -c "
import json, random
random.seed(99)
# Delete 500 from each batch (1500 total across all 3 segments)
ids = []
for batch in range(3):
    ids.extend(random.sample(range(batch*10000, (batch+1)*10000), 500))
with open('/tmp/dvgen_del.ndjson', 'w') as f:
    for i in ids:
        f.write(json.dumps({'delete': {'_index': '$INDEX', '_id': str(i)}}) + '\n')
print(f'Generated {len(ids)} deletes across all 3 segments')
"
curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/dvgen_del.ndjson > /dev/null
curl -s -X POST "$HOST/$INDEX/_flush?force=true" > /dev/null

echo ""
echo "=== Step 4: Check segment topology ==="
curl -s "$HOST/$INDEX/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                print(f'  {seg_name}: docs={seg[\"num_docs\"]}, del={seg.get(\"deleted_docs\",0)}')
"

echo ""
echo "=== Step 5: Refresh and capture baseline ==="
curl -s -X POST "$HOST/$INDEX/_refresh" > /dev/null
echo -n "  Doc count: "
curl -s "$HOST/$INDEX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

QUERY='{"size":0,"aggs":{"by_day":{"terms":{"field":"day_of_week_i"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}}}}}}'
BASELINE=$(curl -s -X POST "$HOST/$INDEX/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$BASELINE" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for b in d['aggregations']['by_day']['buckets']:
    print(f'  BEFORE {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
" | tee /tmp/before_upgrade.txt

echo ""
echo "=== Step 6: Upgrade ==="
RESULT=$(curl -s -X POST "$HOST/$INDEX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "test_st",
    "ordered_dimensions": [{"name": "day_of_week_i"}, {"name": "currency_id"}],
    "metrics": [{"name": "taxful_total_price", "stats": ["sum", "value_count"]}]
  }
}')
echo "  Result: $RESULT"
sleep 2

echo ""
echo "=== Step 7: Post-upgrade query ==="
echo -n "  Doc count: "
curl -s "$HOST/$INDEX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

POST=$(curl -s -X POST "$HOST/$INDEX/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$POST" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  terminated_early: {d.get(\"terminated_early\", False)}')
buckets = d['aggregations']['by_day']['buckets']
print(f'  bucket count: {len(buckets)}')
for b in buckets:
    print(f'  AFTER {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
" | tee /tmp/after_upgrade.txt

echo ""
echo "=== Step 8: Diff ==="
if diff /tmp/before_upgrade.txt /tmp/after_upgrade.txt > /dev/null 2>&1; then
  echo "  PASS: before == after"
else
  echo "  FAIL: values differ"
  diff /tmp/before_upgrade.txt /tmp/after_upgrade.txt
fi

echo ""
echo "=== Step 9: Check logs ==="
echo "  Cache size:"
grep "cacheSize=" build/testclusters/runTask-0/logs/runTask.log 2>/dev/null | grep "$INDEX" | tail -3
echo "  Cache hits:"
grep "cacheHit=" build/testclusters/runTask-0/logs/runTask.log 2>/dev/null | grep "$INDEX" | tail -10
echo "  Paths:"
grep "path=" build/testclusters/runTask-0/logs/runTask.log 2>/dev/null | grep "$INDEX" | tail -10
echo "  docValuesGen:"
grep "populateCache.*$INDEX\|docValuesGen" build/testclusters/runTask-0/logs/runTask.log 2>/dev/null | grep "$INDEX" | tail -10

echo ""
echo "=== Done ==="
