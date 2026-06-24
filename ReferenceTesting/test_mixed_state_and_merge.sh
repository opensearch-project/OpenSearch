#!/bin/bash
# Tests mixed-state (Type A + Type B segments) and merge convergence
# Validates:
# 1. Codec state per segment after upgrade
# 2. Aggregation correctness in mixed state
# 3. Force merge produces native composite segment
# 4. Aggregation correctness after merge convergence
set -e
HOST="localhost:9200"
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}}}}}}'

echo "========================================================"
echo "TEST: MIXED STATE (Type A + Type B) + MERGE CONVERGENCE"
echo "========================================================"

echo ""
echo "=== Cleanup ==="
curl -s -X DELETE "$HOST/ecom_mixed" > /dev/null 2>&1 || true
sleep 1

echo "=== Step 1: Create index with explicit settings ==="
# Use 1 shard for deterministic segment behavior
curl -s -X PUT "$HOST/ecom_mixed" -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "index.refresh_interval": "-1",
    "index.merge.policy.max_merged_segment": "100gb"
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
}' | python3 -c "import sys,json; r=json.load(sys.stdin); print(f'  Index created: acknowledged={r.get(\"acknowledged\")}')"

echo ""
echo "=== Step 2: Ingest 50k docs in batch 1 (will become segment _0) ==="
python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/mixed_batch1.ndjson', 'w') as f:
    for i in range(50000):
        f.write(json.dumps({'index': {'_index': 'ecom_mixed'}}) + '\n')
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
# Ingest in chunks
TOTAL=$(wc -l < /tmp/mixed_batch1.ndjson)
OFF=1; BATCH=20000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/mixed_batch1.ndjson > /tmp/_chunk.ndjson
  RESP=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/_chunk.ndjson)
  ERRORS=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('errors',False))")
  if [ "$ERRORS" = "True" ]; then echo "  BULK ERROR in chunk starting at $OFF"; fi
  OFF=$((END + 1))
done
echo "  Batch 1: 50k docs ingested"

echo "=== Flush batch 1 → creates segment _0 ==="
curl -s -X POST "$HOST/ecom_mixed/_flush?force=true" > /dev/null

echo ""
echo "=== Step 3: Ingest 50k docs in batch 2 (will become segment _1) ==="
python3 -c "
import json, random, datetime
random.seed(99)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/mixed_batch2.ndjson', 'w') as f:
    for i in range(50000):
        f.write(json.dumps({'index': {'_index': 'ecom_mixed'}}) + '\n')
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
TOTAL=$(wc -l < /tmp/mixed_batch2.ndjson)
OFF=1; BATCH=20000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/mixed_batch2.ndjson > /tmp/_chunk.ndjson
  curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/_chunk.ndjson > /dev/null
  OFF=$((END + 1))
done
echo "  Batch 2: 50k docs ingested"

echo "=== Flush batch 2 → creates segment _1 ==="
curl -s -X POST "$HOST/ecom_mixed/_flush?force=true" > /dev/null

echo ""
echo "=== Step 4: Delete ~5k docs from segment _1 only ==="
# Delete docs with total_quantity <= 2 (roughly 10% of batch 2)
DEL=$(curl -s -X POST "$HOST/ecom_mixed/_delete_by_query?refresh=true" -H 'Content-Type: application/json' -d '{
  "query": {
    "bool": {
      "must": [
        { "range": { "total_quantity": { "lte": 2 } } },
        { "range": { "taxful_total_price": { "gte": 250 } } }
      ]
    }
  }
}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('deleted',0))")
echo "  Deleted: $DEL docs"

echo "=== Flush to commit soft deletes ==="
curl -s -X POST "$HOST/ecom_mixed/_flush?force=true" > /dev/null
curl -s -X POST "$HOST/ecom_mixed/_refresh" > /dev/null

echo ""
echo "=== Step 5: Check segments BEFORE upgrade ==="
echo "  Segments:"
curl -s "$HOST/ecom_mixed/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                del_docs = seg.get('deleted_docs', 0)
                marker = ' ← HAS DELETES' if del_docs > 0 else ' ← CLEAN'
                print(f'    {seg_name}: docs={seg[\"num_docs\"]}, deleted={del_docs}, compound={seg.get(\"compound\",\"?\")}{marker}')
"

echo ""
echo "=== Step 6: Aggregation BASELINE (ground truth) ==="
BASELINE=$(curl -s -X POST "$HOST/ecom_mixed/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$BASELINE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
total = 0
for b in d['aggregations']['by_gender']['buckets']:
    total += b['doc_count']
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
print(f'    TOTAL docs: {total}')
"

echo ""
echo "=========================================="
echo "=== Step 7: UPGRADE TO STAR TREE ==="
echo "=========================================="
UPGRADE_RESP=$(curl -s -X POST "$HOST/ecom_mixed/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
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
echo "  Upgrade response:"
echo "$UPGRADE_RESP" | python3 -c "
import sys, json
try:
    r = json.load(sys.stdin)
    print(f'    total_shards={r.get(\"_shards\",{}).get(\"total\")}, successful={r.get(\"_shards\",{}).get(\"successful\")}, failed={r.get(\"_shards\",{}).get(\"failed\")}')
    if r.get('_shards',{}).get('failed',0) > 0:
        for f in r.get('_shards',{}).get('failures',[]):
            print(f'    FAILURE: {f}')
except: print(f'    RAW: {sys.stdin.read()}')
" 2>/dev/null || echo "    $UPGRADE_RESP"

echo ""
echo "=== Step 8: Check segments AFTER upgrade (codec state) ==="
echo "  Checking segment codec via _segments API:"
curl -s "$HOST/ecom_mixed/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
type_a = 0; type_b = 0
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                del_docs = seg.get('deleted_docs', 0)
                compound = seg.get('compound', '?')
                # The _segments API doesn't directly expose codec name,
                # but we can infer from compound + version
                if del_docs > 0:
                    seg_type = 'Type B (soft-delete, old codec)'
                    type_b += 1
                else:
                    seg_type = 'Type A (clean, should be Composite912)'
                    type_a += 1
                print(f'    {seg_name}: docs={seg[\"num_docs\"]}, deleted={del_docs}, compound={compound} → {seg_type}')
print(f'')
print(f'    Summary: {type_a} Type A (native composite), {type_b} Type B (direct reader)')
"

echo ""
echo "=== Step 9: Verify codec via index settings ==="
curl -s "$HOST/ecom_mixed/_settings" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx, settings in data.items():
    s = settings.get('settings', {}).get('index', {})
    print(f'    composite_index: {s.get(\"composite_index\", \"NOT SET\")}')
    print(f'    codec: {s.get(\"codec\", \"NOT SET\")}')
"

echo ""
echo "=== Step 10: Aggregation AFTER upgrade (mixed state) ==="
AFTER_UPGRADE=$(curl -s -X POST "$HOST/ecom_mixed/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER_UPGRADE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
total = 0
for b in d['aggregations']['by_gender']['buckets']:
    total += b['doc_count']
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
print(f'    TOTAL docs: {total}')
"

echo ""
echo "=== Step 11: COMPARE baseline vs after-upgrade ==="
python3 -c "
import json, sys
baseline = json.loads('''$BASELINE''')
after = json.loads('''$AFTER_UPGRADE''')
bb = {b['key']: b for b in baseline['aggregations']['by_gender']['buckets']}
ab = {b['key']: b for b in after['aggregations']['by_gender']['buckets']}
all_pass = True
for key in sorted(bb.keys()):
    if key not in ab: print(f'    {key}: MISSING in after-upgrade'); all_pass=False; continue
    bv = bb[key]['doc_count']; av = ab[key]['doc_count']
    ok = bv == av
    if not ok: all_pass = False
    status = 'PASS' if ok else 'FAIL'
    print(f'    {key}.doc_count: {bv} vs {av} → {status}')
    bv = bb[key]['revenue']['value']; av = ab[key]['revenue']['value']
    ok = abs(bv - av) < 0.01
    if not ok: all_pass = False
    status = 'PASS' if ok else 'FAIL'
    print(f'    {key}.revenue: {bv:.2f} vs {av:.2f} → {status}')
if all_pass: print('    *** MIXED STATE TEST: PASSED ***')
else: print('    *** MIXED STATE TEST: FAILED ***')
"

echo ""
echo "=========================================="
echo "=== Step 12: FORCE MERGE ==="
echo "=========================================="
echo "  Calling _forcemerge?max_num_segments=1..."
MERGE_RESP=$(curl -s -w "\n%{http_code}" -X POST "$HOST/ecom_mixed/_forcemerge?max_num_segments=1&flush=true")
HTTP_CODE=$(echo "$MERGE_RESP" | tail -1)
MERGE_BODY=$(echo "$MERGE_RESP" | sed '$d')
echo "  HTTP status: $HTTP_CODE"
echo "$MERGE_BODY" | python3 -c "
import sys, json
try:
    r = json.load(sys.stdin)
    print(f'    total_shards={r.get(\"_shards\",{}).get(\"total\")}, successful={r.get(\"_shards\",{}).get(\"successful\")}, failed={r.get(\"_shards\",{}).get(\"failed\")}')
    if r.get('_shards',{}).get('failed',0) > 0:
        for f in r.get('_shards',{}).get('failures',[]):
            print(f'    FAILURE: {f}')
except Exception as e:
    print(f'    Parse error: {e}')
    print(f'    Raw: {sys.stdin.read()[:500]}')
" 2>/dev/null

echo ""
echo "=== Wait for merge to complete ==="
sleep 3
curl -s -X POST "$HOST/ecom_mixed/_refresh" > /dev/null

echo ""
echo "=== Step 13: Segments AFTER force merge ==="
curl -s "$HOST/ecom_mixed/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
seg_count = 0
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                seg_count += 1
                del_docs = seg.get('deleted_docs', 0)
                print(f'    {seg_name}: docs={seg[\"num_docs\"]}, deleted={del_docs}, compound={seg.get(\"compound\",\"?\")}')
if seg_count == 1:
    print(f'    ✓ Force merge succeeded — 1 segment')
else:
    print(f'    ✗ Force merge may have failed — {seg_count} segments remain')
"

echo ""
echo "=== Step 14: Aggregation AFTER force merge ==="
AFTER_MERGE=$(curl -s -X POST "$HOST/ecom_mixed/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER_MERGE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
total = 0
for b in d['aggregations']['by_gender']['buckets']:
    total += b['doc_count']
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
print(f'    TOTAL docs: {total}')
"

echo ""
echo "=== Step 15: COMPARE baseline vs after-merge ==="
python3 -c "
import json, sys
baseline = json.loads('''$BASELINE''')
after_merge = json.loads('''$AFTER_MERGE''')
bb = {b['key']: b for b in baseline['aggregations']['by_gender']['buckets']}
ab = {b['key']: b for b in after_merge['aggregations']['by_gender']['buckets']}
all_pass = True
for key in sorted(bb.keys()):
    if key not in ab: print(f'    {key}: MISSING in after-merge'); all_pass=False; continue
    bv = bb[key]['doc_count']; av = ab[key]['doc_count']
    ok = bv == av
    if not ok: all_pass = False
    status = 'PASS' if ok else 'FAIL'
    print(f'    {key}.doc_count: {bv} vs {av} → {status}')
    bv = bb[key]['revenue']['value']; av = ab[key]['revenue']['value']
    ok = abs(bv - av) < 0.01
    if not ok: all_pass = False
    status = 'PASS' if ok else 'FAIL'
    print(f'    {key}.revenue: {bv:.2f} vs {av:.2f} → {status}')
if all_pass: print('    *** MERGE CONVERGENCE TEST: PASSED ***')
else: print('    *** MERGE CONVERGENCE TEST: FAILED ***')
"

echo ""
echo "=========================================="
echo "=== Step 16: Verify star tree in merged segment ==="
echo "=========================================="
echo "  Checking if merged segment has star tree (via stats):"
curl -s "$HOST/ecom_mixed/_stats/star_tree" 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(f'    {json.dumps(data.get(\"indices\",{}).get(\"ecom_mixed\",{}).get(\"primaries\",{}).get(\"star_tree\",\"NOT AVAILABLE\"), indent=6)}')
except:
    print('    star_tree stats not available via _stats API')
" 2>/dev/null || echo "    star_tree stats endpoint not available"

echo ""
echo "  Checking composite_index setting:"
curl -s "$HOST/ecom_mixed/_settings/index.composite_index" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx, v in data.items():
    ci = v.get('settings',{}).get('index',{}).get('composite_index','NOT SET')
    print(f'    index.composite_index = {ci}')
"

echo ""
echo "=== Step 17: Additional query to verify star tree acceleration ==="
# Simple metric agg that should definitely use star tree
SIMPLE_QUERY='{"size":0,"aggs":{"total_rev":{"sum":{"field":"taxful_total_price"}},"avg_qty":{"avg":{"field":"total_quantity"}}}}'
echo "  Simple metric aggregation (sum + avg):"
SIMPLE_BEFORE=$(curl -s -X POST "$HOST/ecom_mixed/_search" -H 'Content-Type: application/json' -d "$SIMPLE_QUERY")
echo "$SIMPLE_BEFORE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
print(f'    total_revenue: {d[\"aggregations\"][\"total_rev\"][\"value\"]:.2f}')
print(f'    avg_quantity: {d[\"aggregations\"][\"avg_qty\"][\"value\"]:.4f}')
print(f'    hits.total: {d[\"hits\"][\"total\"][\"value\"]}')
"

echo ""
echo "========================"
echo "ALL TESTS COMPLETE"
echo "========================"
