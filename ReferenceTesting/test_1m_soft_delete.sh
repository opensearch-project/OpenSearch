#!/bin/bash
# 1M doc test with soft deletes — measures upgrade and merge timing
set -e
HOST="localhost:9200"
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}}}}}}'

echo "========================================================"
echo "TEST: 1M DOCS + SOFT DELETES + UPGRADE + MERGE (TIMED)"
echo "========================================================"

echo ""
echo "=== Cleanup ==="
curl -s -X DELETE "$HOST/ecom_1m" > /dev/null 2>&1 || true
sleep 1

echo "=== Step 1: Create index ==="
curl -s -X PUT "$HOST/ecom_1m" -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "index.refresh_interval": "-1",
    "index.merge.policy.max_merged_segment": "100gb",
    "index.merge.policy.segments_per_tier": "100"
  },
  "mappings": {
    "properties": {
      "customer_gender": { "type": "keyword" },
      "currency": { "type": "keyword" },
      "day_of_week": { "type": "keyword" },
      "order_date": { "type": "date" },
      "taxful_total_price": { "type": "double" },
      "taxless_total_price": { "type": "double" },
      "total_quantity": { "type": "integer" },
      "total_unique_products": { "type": "integer" },
      "day_of_week_i": { "type": "integer" }
    }
  }
}' | python3 -c "import sys,json; r=json.load(sys.stdin); print(f'  acknowledged={r.get(\"acknowledged\")}')"

echo ""
echo "=== Step 2: Generate 1M docs ==="
START_GEN=$(date +%s)
python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/1m_bulk.ndjson', 'w') as f:
    for i in range(1000000):
        f.write(json.dumps({'index': {'_index': 'ecom_1m', '_id': str(i)}}) + '\n')
        dt = base + datetime.timedelta(hours=random.randint(0, 8760))
        f.write(json.dumps({
            'customer_gender': random.choice(genders),
            'currency': random.choice(currencies),
            'day_of_week': random.choice(days),
            'order_date': dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'taxful_total_price': round(random.uniform(5, 500), 2),
            'taxless_total_price': round(random.uniform(4, 450), 2),
            'total_quantity': random.randint(1, 20),
            'total_unique_products': random.randint(1, 10),
            'day_of_week_i': random.randint(0, 6)
        }) + '\n')
"
END_GEN=$(date +%s)
echo "  Generated 1M docs in $((END_GEN - START_GEN))s"

echo ""
echo "=== Step 3: Ingest 1M docs ==="
START_INGEST=$(date +%s)
TOTAL=$(wc -l < /tmp/1m_bulk.ndjson)
OFF=1; BATCH=20000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/1m_bulk.ndjson > /tmp/_chunk.ndjson
  curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/_chunk.ndjson > /dev/null
  OFF=$((END + 1))
done
END_INGEST=$(date +%s)
echo "  Ingested 1M docs in $((END_INGEST - START_INGEST))s"

echo "=== Flush ==="
curl -s -X POST "$HOST/ecom_1m/_flush?force=true" > /dev/null
curl -s -X POST "$HOST/ecom_1m/_refresh" > /dev/null

echo ""
echo "=== Step 4: Delete ~100k docs ==="
START_DEL=$(date +%s)
DEL=$(curl -s -X POST "$HOST/ecom_1m/_delete_by_query?refresh=true" -H 'Content-Type: application/json' -d '{
  "query": { "range": { "total_quantity": { "lte": 2 } } }
}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('deleted',0))")
END_DEL=$(date +%s)
echo "  Deleted: $DEL docs in $((END_DEL - START_DEL))s"

echo "=== Flush ==="
curl -s -X POST "$HOST/ecom_1m/_flush?force=true" > /dev/null

echo ""
echo "=== Step 5: Segments before upgrade ==="
curl -s "$HOST/ecom_1m/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
total_docs = 0; total_del = 0; seg_count = 0
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                seg_count += 1
                total_docs += seg['num_docs']
                total_del += seg.get('deleted_docs', 0)
print(f'  {seg_count} segments, {total_docs} live docs, {total_del} deleted')
"

echo ""
echo "=== Step 6: Aggregation BASELINE ==="
BASELINE=$(curl -s -X POST "$HOST/ecom_1m/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$BASELINE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
total = 0
for b in d['aggregations']['by_gender']['buckets']:
    total += b['doc_count']
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
print(f'    TOTAL: {total}')
"

echo ""
echo "=========================================="
echo "=== Step 7: UPGRADE TO STAR TREE (TIMED) ==="
echo "=========================================="
START_UPGRADE=$(python3 -c "import time; print(int(time.time()*1000))")
UPGRADE_RESP=$(curl -s -X POST "$HOST/ecom_1m/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "ecom_star_tree",
    "ordered_dimensions": [
      {"name": "customer_gender"},
      {"name": "currency"},
      {"name": "day_of_week"},
      {"name": "order_date"}
    ],
    "metrics": [
      {"name": "taxful_total_price", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "taxless_total_price", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "total_quantity", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "total_unique_products", "stats": ["sum", "avg", "value_count"]}
    ]
  }
}')
END_UPGRADE=$(python3 -c "import time; print(int(time.time()*1000))")
UPGRADE_MS=$((END_UPGRADE - START_UPGRADE))
echo "  ⏱️  Upgrade time: ${UPGRADE_MS}ms"
echo "$UPGRADE_RESP" | python3 -c "
import sys, json
try:
    r = json.load(sys.stdin)
    print(f'    successful={r.get(\"_shards\",{}).get(\"successful\")}, failed={r.get(\"_shards\",{}).get(\"failed\")}')
except: pass
" 2>/dev/null

echo ""
echo "=== Step 8: Aggregation AFTER upgrade ==="
AFTER_UPGRADE=$(curl -s -X POST "$HOST/ecom_1m/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER_UPGRADE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
total = 0
for b in d['aggregations']['by_gender']['buckets']:
    total += b['doc_count']
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
print(f'    TOTAL: {total}')
"

echo ""
echo "=== Step 9: COMPARE baseline vs after-upgrade ==="
python3 -c "
import json, sys
baseline = json.loads('''$BASELINE''')
after = json.loads('''$AFTER_UPGRADE''')
bb = {b['key']: b for b in baseline['aggregations']['by_gender']['buckets']}
ab = {b['key']: b for b in after['aggregations']['by_gender']['buckets']}
all_pass = True
for key in sorted(bb.keys()):
    if key not in ab: print(f'    {key}: MISSING'); all_pass=False; continue
    bv = bb[key]['doc_count']; av = ab[key]['doc_count']
    if bv != av: all_pass = False
    brev = bb[key]['revenue']['value']; arev = ab[key]['revenue']['value']
    if abs(brev - arev) >= 0.01: all_pass = False
if all_pass: print('    *** UPGRADE TEST: PASSED ***')
else: print('    *** UPGRADE TEST: FAILED ***')
"

echo ""
echo "=========================================="
echo "=== Step 10: FORCE MERGE (TIMED) ==="
echo "=========================================="
START_MERGE=$(python3 -c "import time; print(int(time.time()*1000))")
MERGE_RESP=$(curl -s -X POST "$HOST/ecom_1m/_forcemerge?max_num_segments=1&flush=true")
END_MERGE=$(python3 -c "import time; print(int(time.time()*1000))")
MERGE_MS=$((END_MERGE - START_MERGE))
echo "  ⏱️  Force merge time: ${MERGE_MS}ms"
echo "$MERGE_RESP" | python3 -c "
import sys, json
try:
    r = json.load(sys.stdin)
    print(f'    successful={r.get(\"_shards\",{}).get(\"successful\")}, failed={r.get(\"_shards\",{}).get(\"failed\")}')
except: pass
" 2>/dev/null

sleep 3
curl -s -X POST "$HOST/ecom_1m/_refresh" > /dev/null

echo ""
echo "=== Step 11: Segments after merge ==="
curl -s "$HOST/ecom_1m/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                print(f'    {seg_name}: docs={seg[\"num_docs\"]}, deleted={seg.get(\"deleted_docs\",0)}')
"

echo ""
echo "=== Step 12: Aggregation AFTER merge ==="
AFTER_MERGE=$(curl -s -X POST "$HOST/ecom_1m/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER_MERGE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
total = 0
for b in d['aggregations']['by_gender']['buckets']:
    total += b['doc_count']
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
print(f'    TOTAL: {total}')
print(f'    terminated_early: {d.get(\"terminated_early\")}')
"

echo ""
echo "=== Step 13: COMPARE baseline vs after-merge ==="
python3 -c "
import json, sys
baseline = json.loads('''$BASELINE''')
after_merge = json.loads('''$AFTER_MERGE''')
bb = {b['key']: b for b in baseline['aggregations']['by_gender']['buckets']}
ab = {b['key']: b for b in after_merge['aggregations']['by_gender']['buckets']}
all_pass = True
for key in sorted(bb.keys()):
    if key not in ab: print(f'    {key}: MISSING'); all_pass=False; continue
    bv = bb[key]['doc_count']; av = ab[key]['doc_count']
    ok = bv == av
    if not ok: all_pass = False
    print(f'    {key}.doc_count: {bv} vs {av} → {\"PASS\" if ok else \"FAIL\"}')
    bv = bb[key]['revenue']['value']; av = ab[key]['revenue']['value']
    ok = abs(bv - av) < 0.01
    if not ok: all_pass = False
    print(f'    {key}.revenue: {bv:.2f} vs {av:.2f} → {\"PASS\" if ok else \"FAIL\"}')
if all_pass: print('    *** 1M MERGE TEST: PASSED ***')
else: print('    *** 1M MERGE TEST: FAILED ***')
"

echo ""
echo "=========================================="
echo "TIMING SUMMARY"
echo "=========================================="
echo "  Data generation:  $((END_GEN - START_GEN))s"
echo "  Ingestion (1M):   $((END_INGEST - START_INGEST))s"
echo "  Delete (~100k):   $((END_DEL - START_DEL))s"
echo "  ⏱️  UPGRADE:       ${UPGRADE_MS}ms ($((UPGRADE_MS / 1000))s)"
echo "  ⏱️  FORCE MERGE:   ${MERGE_MS}ms ($((MERGE_MS / 1000))s)"
echo "=========================================="
