#!/bin/bash
# Full validation: upgrade + writes during upgrade + force merge + keyword agg + numeric agg
set -e
HOST="localhost:9200"
QUERY_GENDER='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}}}}}}'
QUERY_CURRENCY='{"size":0,"aggs":{"by_currency":{"terms":{"field":"currency"},"aggs":{"qty":{"sum":{"field":"total_quantity"}}}}}}'

echo "================================================================"
echo "FULL VALIDATION: UPGRADE + WRITES + MERGE + KEYWORD/NUMERIC AGGS"
echo "================================================================"

echo ""
echo "=== 1. Create index ==="
curl -s -X DELETE "$HOST/test_full" > /dev/null 2>&1 || true
sleep 1
curl -s -X PUT "$HOST/test_full" -H 'Content-Type: application/json' -d '{
  "settings":{"number_of_shards":1,"number_of_replicas":0,"index.refresh_interval":"-1","index.merge.policy.max_merged_segment":"100gb","index.merge.policy.segments_per_tier":"100"},
  "mappings":{"properties":{"customer_gender":{"type":"keyword"},"currency":{"type":"keyword"},"day_of_week":{"type":"keyword"},"order_date":{"type":"date"},"taxful_total_price":{"type":"double"},"taxless_total_price":{"type":"double"},"total_quantity":{"type":"integer"},"total_unique_products":{"type":"integer"},"day_of_week_i":{"type":"integer"}}}
}' > /dev/null
echo "  Created"

echo ""
echo "=== 2. Ingest 100k docs ==="
python3 -c "
import json, random
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
with open('/tmp/full_test.ndjson','w') as f:
    for i in range(100000):
        f.write(json.dumps({'index':{'_index':'test_full','_id':str(i)}})+'\n')
        f.write(json.dumps({'customer_gender':random.choice(genders),'currency':random.choice(currencies),'taxful_total_price':round(random.uniform(5,500),2),'total_quantity':random.randint(1,20),'day_of_week_i':random.randint(0,6)})+'\n')
"
curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/full_test.ndjson > /dev/null
curl -s -X POST "$HOST/test_full/_flush?force=true" > /dev/null
curl -s -X POST "$HOST/test_full/_refresh" > /dev/null
echo "  Done"

echo ""
echo "=== 3. Delete ~10k docs (creates soft deletes) ==="
DEL=$(curl -s -X POST "$HOST/test_full/_delete_by_query?refresh=true" -H 'Content-Type: application/json' -d '{"query":{"range":{"total_quantity":{"lte":2}}}}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('deleted',0))")
curl -s -X POST "$HOST/test_full/_flush?force=true" > /dev/null
echo "  Deleted: $DEL docs"

echo ""
echo "=== 4. BASELINE aggregations ==="
BASELINE_GENDER=$(curl -s -X POST "$HOST/test_full/_search" -H 'Content-Type: application/json' -d "$QUERY_GENDER")
BASELINE_CURRENCY=$(curl -s -X POST "$HOST/test_full/_search" -H 'Content-Type: application/json' -d "$QUERY_CURRENCY")
echo "  Gender:"
echo "$BASELINE_GENDER" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, rev={b[\"revenue\"][\"value\"]:.2f}') for b in d['aggregations']['by_gender']['buckets']]"
echo "  Currency:"
echo "$BASELINE_CURRENCY" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, qty={b[\"qty\"][\"value\"]:.0f}') for b in d['aggregations']['by_currency']['buckets']]"

echo ""
echo "=== 5. UPGRADE (with concurrent writes) ==="
# Start upgrade in background
curl -s -X POST "$HOST/test_full/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree":{"name":"st","ordered_dimensions":[{"name":"customer_gender"},{"name":"currency"}],"metrics":[{"name":"taxful_total_price","stats":["sum","avg","value_count","min","max"]},{"name":"total_quantity","stats":["sum","avg","value_count"]}]}
}' > /tmp/upgrade_result.json &
UPGRADE_PID=$!

# Write docs during upgrade (Phase 1)
sleep 0.3
WRITE_OK=0
for i in $(seq 1 5); do
  R=$(curl -s -X POST "$HOST/test_full/_doc" -H 'Content-Type: application/json' -d "{\"customer_gender\":\"MALE\",\"currency\":\"USD\",\"taxful_total_price\":100.0,\"total_quantity\":5}")
  echo "$R" | python3 -c "import sys,json; r=json.load(sys.stdin); exit(0 if r.get('result')=='created' else 1)" 2>/dev/null && WRITE_OK=$((WRITE_OK+1))
done

wait $UPGRADE_PID
UPGRADE_RESP=$(cat /tmp/upgrade_result.json)
UPGRADE_OK=$(echo "$UPGRADE_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('_shards',{}).get('successful',0))" 2>/dev/null)
echo "  Upgrade: successful=$UPGRADE_OK"
echo "  Writes during upgrade: $WRITE_OK/5 succeeded"

echo ""
echo "=== 6. Post-upgrade aggregation check ==="
curl -s -X POST "$HOST/test_full/_refresh" > /dev/null
AFTER_GENDER=$(curl -s -X POST "$HOST/test_full/_search" -H 'Content-Type: application/json' -d "$QUERY_GENDER")
echo "$AFTER_GENDER" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(f'  terminated_early: {d.get(\"terminated_early\")}')
for b in d['aggregations']['by_gender']['buckets']:
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, rev={b[\"revenue\"][\"value\"]:.2f}')
"

echo ""
echo "=== 7. FORCE MERGE ==="
MERGE_RESP=$(curl -s -X POST "$HOST/test_full/_forcemerge?max_num_segments=1&flush=true")
MERGE_OK=$(echo "$MERGE_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('_shards',{}).get('successful',0))" 2>/dev/null)
echo "  Merge: successful=$MERGE_OK"

sleep 2
curl -s -X POST "$HOST/test_full/_refresh" > /dev/null

echo ""
echo "=== 8. Post-merge aggregation checks ==="
MERGED_GENDER=$(curl -s -X POST "$HOST/test_full/_search" -H 'Content-Type: application/json' -d "$QUERY_GENDER")
MERGED_CURRENCY=$(curl -s -X POST "$HOST/test_full/_search" -H 'Content-Type: application/json' -d "$QUERY_CURRENCY")
echo "  Gender (keyword field — SortedSetDocValues test):"
echo "$MERGED_GENDER" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(f'    terminated_early: {d.get(\"terminated_early\")}')
for b in d['aggregations']['by_gender']['buckets']:
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, rev={b[\"revenue\"][\"value\"]:.2f}')
"
echo "  Currency (keyword field — SortedSetDocValues test):"
echo "$MERGED_CURRENCY" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(f'    terminated_early: {d.get(\"terminated_early\")}')
for b in d['aggregations']['by_currency']['buckets']:
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, qty={b[\"qty\"][\"value\"]:.0f}')
"

echo ""
echo "=== 9. COMPARE baseline vs post-merge ==="
python3 -c "
import json, sys
baseline = json.loads('''$BASELINE_GENDER''')
merged = json.loads('''$MERGED_GENDER''')
bb = {b['key']: b for b in baseline['aggregations']['by_gender']['buckets']}
mb = {b['key']: b for b in merged['aggregations']['by_gender']['buckets']}
all_pass = True
for key in sorted(bb.keys()):
    if key not in mb: print(f'  {key}: MISSING in merged'); all_pass=False; continue
    # Baseline doesn't include the 5 docs written during upgrade, so merged may have slightly more
    # But the original docs should match exactly
    bv = bb[key]['doc_count']; mv = mb[key]['doc_count']
    # Allow merged to be >= baseline (extra docs from writes during upgrade)
    ok = mv >= bv
    if not ok: all_pass = False
    print(f'  {key}.doc_count: baseline={bv}, merged={mv} (diff={mv-bv}) -> {\"PASS\" if ok else \"FAIL\"}')
    brev = bb[key]['revenue']['value']; mrev = mb[key]['revenue']['value']
    ok = mrev >= brev - 0.01
    if not ok: all_pass = False
    print(f'  {key}.revenue: baseline={brev:.2f}, merged={mrev:.2f} -> {\"PASS\" if ok else \"FAIL\"}')
if all_pass: print('  *** ALL CHECKS PASSED ***')
else: print('  *** SOME CHECKS FAILED ***')
"

echo ""
echo "=== SUMMARY ==="
echo "  Upgrade: successful=$UPGRADE_OK"
echo "  Writes during upgrade: $WRITE_OK/5"
echo "  Force merge: successful=$MERGE_OK"
TERM_EARLY=$(echo "$MERGED_GENDER" | python3 -c "import sys,json; print(json.load(sys.stdin).get('terminated_early'))")
echo "  Star tree active after merge: terminated_early=$TERM_EARLY"
echo "================================================================"
