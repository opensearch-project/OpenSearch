#!/bin/bash
# 1M doc full validation: upgrade + writes during upgrade + force merge + keyword/numeric aggs
set -e
HOST="localhost:9200"
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}}}}}}'

echo "================================================================"
echo "1M FULL VALIDATION: UPGRADE + WRITES + MERGE + KEYWORD AGGS"
echo "================================================================"

echo ""
echo "=== 1. Create index ==="
curl -s -X DELETE "$HOST/ecom_full" > /dev/null 2>&1 || true
sleep 1
curl -s -X PUT "$HOST/ecom_full" -H 'Content-Type: application/json' -d '{
  "settings":{"number_of_shards":1,"number_of_replicas":0,"index.refresh_interval":"-1","index.merge.policy.max_merged_segment":"100gb","index.merge.policy.segments_per_tier":"100"},
  "mappings":{"properties":{"customer_gender":{"type":"keyword"},"currency":{"type":"keyword"},"day_of_week":{"type":"keyword"},"order_date":{"type":"date"},"taxful_total_price":{"type":"double"},"taxless_total_price":{"type":"double"},"total_quantity":{"type":"integer"},"total_unique_products":{"type":"integer"},"day_of_week_i":{"type":"integer"}}}
}' > /dev/null
echo "  Created"

echo ""
echo "=== 2. Generate + Ingest 1M docs ==="
START_INGEST=$(date +%s)
python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/1m_full.ndjson', 'w') as f:
    for i in range(1000000):
        f.write(json.dumps({'index': {'_index': 'ecom_full', '_id': str(i)}}) + '\n')
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
TOTAL=$(wc -l < /tmp/1m_full.ndjson)
OFF=1; BATCH=20000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/1m_full.ndjson > /tmp/_chunk.ndjson
  curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/_chunk.ndjson > /dev/null
  OFF=$((END + 1))
done
END_INGEST=$(date +%s)
echo "  Ingested 1M docs in $((END_INGEST - START_INGEST))s"
curl -s -X POST "$HOST/ecom_full/_flush?force=true" > /dev/null
curl -s -X POST "$HOST/ecom_full/_refresh" > /dev/null

echo ""
echo "=== 3. Delete ~100k docs ==="
DEL=$(curl -s -X POST "$HOST/ecom_full/_delete_by_query?refresh=true" -H 'Content-Type: application/json' -d '{"query":{"range":{"total_quantity":{"lte":2}}}}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('deleted',0))")
curl -s -X POST "$HOST/ecom_full/_flush?force=true" > /dev/null
echo "  Deleted: $DEL docs"

echo ""
echo "=== 4. BASELINE ==="
BASELINE=$(curl -s -X POST "$HOST/ecom_full/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$BASELINE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
total = sum(b['doc_count'] for b in d['aggregations']['by_gender']['buckets'])
for b in d['aggregations']['by_gender']['buckets']:
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, rev={b[\"revenue\"][\"value\"]:.2f}')
print(f'    TOTAL: {total}')
"

echo ""
echo "=== 5. UPGRADE + CONCURRENT WRITES ==="
START_UP=$(python3 -c "import time; print(int(time.time()*1000))")
curl -s -X POST "$HOST/ecom_full/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree":{"name":"st","ordered_dimensions":[{"name":"customer_gender"},{"name":"currency"},{"name":"day_of_week"},{"name":"order_date"}],"metrics":[{"name":"taxful_total_price","stats":["sum","avg","min","max","value_count"]},{"name":"taxless_total_price","stats":["sum","avg","min","max","value_count"]},{"name":"total_quantity","stats":["sum","avg","min","max","value_count"]},{"name":"total_unique_products","stats":["sum","avg","value_count"]}]}
}' > /tmp/up_result.json &
UP_PID=$!

# Write 100 docs during Phase 1 (should succeed)
sleep 1
WRITE_OK=0
READ_OK=0
for i in $(seq 1 100); do
  R=$(curl -s -X POST "$HOST/ecom_full/_doc" -H 'Content-Type: application/json' -d "{\"customer_gender\":\"MALE\",\"currency\":\"EUR\",\"day_of_week\":\"Monday\",\"order_date\":\"2024-06-01T00:00:00Z\",\"taxful_total_price\":100.0,\"taxless_total_price\":90.0,\"total_quantity\":5,\"total_unique_products\":2,\"day_of_week_i\":0}" 2>/dev/null)
  echo "$R" | python3 -c "import sys,json; r=json.load(sys.stdin); exit(0 if r.get('result') in ['created','updated'] else 1)" 2>/dev/null && WRITE_OK=$((WRITE_OK+1))
  # Also do a read every 10 writes
  if [ $((i % 10)) -eq 0 ]; then
    SR=$(curl -s -X POST "$HOST/ecom_full/_search?size=0" -H 'Content-Type: application/json' -d '{"aggs":{"c":{"value_count":{"field":"customer_gender"}}}}' 2>/dev/null)
    echo "$SR" | python3 -c "import sys,json; r=json.load(sys.stdin); exit(0 if 'aggregations' in r else 1)" 2>/dev/null && READ_OK=$((READ_OK+1))
  fi
done

wait $UP_PID
END_UP=$(python3 -c "import time; print(int(time.time()*1000))")
UP_MS=$((END_UP - START_UP))
UP_RESP=$(cat /tmp/up_result.json)
UP_OK=$(echo "$UP_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('_shards',{}).get('successful',0))" 2>/dev/null)
echo "  ⏱️  Upgrade: ${UP_MS}ms"
echo "  Upgrade: successful=$UP_OK"
echo "  Writes during upgrade: $WRITE_OK/100"
echo "  Reads during upgrade: $READ_OK/10"

echo ""
echo "=== 6. Post-upgrade aggregation ==="
curl -s -X POST "$HOST/ecom_full/_refresh" > /dev/null
AFTER_UP=$(curl -s -X POST "$HOST/ecom_full/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER_UP" | python3 -c "
import sys, json; d = json.load(sys.stdin)
print(f'  terminated_early: {d.get(\"terminated_early\")}')
total = sum(b['doc_count'] for b in d['aggregations']['by_gender']['buckets'])
for b in d['aggregations']['by_gender']['buckets']:
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, rev={b[\"revenue\"][\"value\"]:.2f}')
print(f'    TOTAL: {total}')
"

echo ""
echo "=== 7. Upgrade agg check ==="
python3 -c "
import json, sys
baseline = json.loads('''$BASELINE''')
after = json.loads('''$AFTER_UP''')
bb = {b['key']: b for b in baseline['aggregations']['by_gender']['buckets']}
ab = {b['key']: b for b in after['aggregations']['by_gender']['buckets']}
all_pass = True
for key in sorted(bb.keys()):
    bv = bb[key]['doc_count']; av = ab[key]['doc_count']
    ok = av >= bv  # may have extra docs from writes
    if not ok: all_pass = False
if all_pass: print('    *** UPGRADE AGG: PASSED ***')
else: print('    *** UPGRADE AGG: FAILED ***')
"

echo ""
echo "=== 8. FORCE MERGE ==="
START_M=$(python3 -c "import time; print(int(time.time()*1000))")
curl -s -X POST "$HOST/ecom_full/_forcemerge?max_num_segments=1&flush=true" > /dev/null
END_M=$(python3 -c "import time; print(int(time.time()*1000))")
M_MS=$((END_M - START_M))
echo "  ⏱️  Force merge: ${M_MS}ms"
sleep 3
curl -s -X POST "$HOST/ecom_full/_refresh" > /dev/null

echo ""
echo "=== 9. Post-merge aggregation ==="
AFTER_M=$(curl -s -X POST "$HOST/ecom_full/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER_M" | python3 -c "
import sys, json; d = json.load(sys.stdin)
print(f'  terminated_early: {d.get(\"terminated_early\")}')
total = sum(b['doc_count'] for b in d['aggregations']['by_gender']['buckets'])
for b in d['aggregations']['by_gender']['buckets']:
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, rev={b[\"revenue\"][\"value\"]:.2f}')
print(f'    TOTAL: {total}')
"

echo ""
echo "=== 10. Merge agg check ==="
python3 -c "
import json, sys
after_up = json.loads('''$AFTER_UP''')
after_m = json.loads('''$AFTER_M''')
ub = {b['key']: b for b in after_up['aggregations']['by_gender']['buckets']}
mb = {b['key']: b for b in after_m['aggregations']['by_gender']['buckets']}
all_pass = True
for key in sorted(ub.keys()):
    if key not in mb: print(f'  {key}: MISSING'); all_pass=False; continue
    uv = ub[key]['doc_count']; mv = mb[key]['doc_count']
    ok = uv == mv
    if not ok: all_pass = False
    print(f'  {key}.count: {uv} vs {mv} -> {\"PASS\" if ok else \"FAIL\"}')
    ur = ub[key]['revenue']['value']; mr = mb[key]['revenue']['value']
    ok = abs(ur - mr) < 0.01
    if not ok: all_pass = False
    print(f'  {key}.revenue: {ur:.2f} vs {mr:.2f} -> {\"PASS\" if ok else \"FAIL\"}')
if all_pass: print('  *** MERGE AGG: PASSED ***')
else: print('  *** MERGE AGG: FAILED ***')
"

echo ""
echo "================================================================"
echo "SUMMARY"
echo "================================================================"
echo "  Upgrade: successful=$UP_OK, time=${UP_MS}ms"
echo "  Writes during upgrade: $WRITE_OK/100"
echo "  Reads during upgrade: $READ_OK/10"
echo "  Force merge: time=${M_MS}ms"
TERM=$(echo "$AFTER_M" | python3 -c "import sys,json; print(json.load(sys.stdin).get('terminated_early'))")
echo "  Star tree after merge: terminated_early=$TERM"
echo "================================================================"
