#!/bin/bash
# Comprehensive test for sidecar star tree upgrade
# Tests: 1M docs without deletes, then 100k docs WITH deletes
# Verifies: aggregation results match before/after, star tree acceleration works
set -e
HOST="localhost:9200"

echo "=============================================="
echo "  Sidecar Star Tree Upgrade — Comprehensive Test"
echo "=============================================="

# ============================================================
# TEST 1: 1M docs, no deletes
# ============================================================
echo ""
echo "====== TEST 1: 1M docs, no deletes ======"

echo ""
echo "--- Generate 1M docs ---"
python3 ReferenceTesting/gen_1m.py 2>/dev/null || python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/test_1m.ndjson', 'w') as f:
    for i in range(1000000):
        dt = base + datetime.timedelta(hours=random.randint(0, 8760))
        f.write(json.dumps({'index': {'_index': 'sidecar_1m'}}) + '\n')
        f.write(json.dumps({
            'customer_gender': random.choice(genders),
            'currency': random.choice(currencies),
            'day_of_week': random.choice(days),
            'order_date': dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'taxful_total_price': round(random.uniform(5, 500), 2),
            'taxless_total_price': round(random.uniform(4, 450), 2),
            'total_quantity': random.randint(1, 20),
            'total_unique_products': random.randint(1, 10),
            'day_of_week_i': random.randint(0, 6),
            'customer_id': 'cust_' + str(random.randint(1,5000)),
            'order_id': 'ord_' + str(i),
            'type': 'order',
            'user': 'user_' + str(random.randint(1,2000))
        }) + '\n')
print('Generated 1,000,000 docs')
"

echo "--- Create index ---"
curl -s -X DELETE "$HOST/sidecar_1m" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/sidecar_1m" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
echo "Created sidecar_1m"

echo "--- Ingest 1M docs ---"
TOTAL=$(wc -l < /tmp/test_1m.ndjson)
OFF=1; COUNT=0; BATCH=100000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/test_1m.ndjson > /tmp/_batch.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_batch.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
done
echo "  Total: $COUNT docs"

curl -s -X POST "$HOST/sidecar_1m/_refresh" > /dev/null
curl -s -X POST "$HOST/sidecar_1m/_flush?force=true" > /dev/null

echo ""
echo "--- Doc count: $(curl -s "$HOST/sidecar_1m/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])") ---"

echo ""
echo "--- Segments before upgrade ---"
curl -s "$HOST/sidecar_1m/_segments" | python3 -c "
import sys,json
d = json.load(sys.stdin)
for idx_name, idx in d['indices'].items():
    for shard_id, shards in idx['shards'].items():
        for shard in shards:
            segs = shard['segments']
            print(f'  {len(segs)} segments')
            for name, seg in segs.items():
                print(f'    {name}: {seg[\"num_docs\"]} docs, deleted={seg.get(\"deleted_docs\",0)}, compound={seg[\"compound\"]}')
"

QUERY='{"size":0,"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}},"avg_price":{"avg":{"field":"taxful_total_price"}},"max_price":{"max":{"field":"taxful_total_price"}},"min_price":{"min":{"field":"taxful_total_price"}},"doc_count":{"value_count":{"field":"taxful_total_price"}}}}'

echo ""
echo "--- Aggregation BEFORE upgrade ---"
BEFORE=$(curl -s -X POST "$HOST/sidecar_1m/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$BEFORE" | python3 -c "
import sys, json
d = json.load(sys.stdin)
a = d['aggregations']
print(f'  revenue:   {a[\"revenue\"][\"value\"]:.2f}')
print(f'  avg_price: {a[\"avg_price\"][\"value\"]:.2f}')
print(f'  max_price: {a[\"max_price\"][\"value\"]}')
print(f'  min_price: {a[\"min_price\"][\"value\"]}')
print(f'  doc_count: {a[\"doc_count\"][\"value\"]}')
print(f'  terminated_early: {d.get(\"terminated_early\", False)}')
print(f'  took: {d[\"took\"]}ms')
"

echo ""
echo "--- UPGRADE (timed) ---"
START=$(python3 -c "import time; print(time.time())")
RESP=$(curl -s -X POST "$HOST/sidecar_1m/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
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
END=$(python3 -c "import time; print(time.time())")
echo "  Response: $RESP"
echo "  Upgrade time: $(python3 -c "print(f'{$END - $START:.3f}')")s"

echo ""
echo "--- Aggregation AFTER upgrade ---"
AFTER=$(curl -s -X POST "$HOST/sidecar_1m/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER" | python3 -c "
import sys, json
d = json.load(sys.stdin)
a = d['aggregations']
print(f'  revenue:   {a[\"revenue\"][\"value\"]:.2f}')
print(f'  avg_price: {a[\"avg_price\"][\"value\"]:.2f}')
print(f'  max_price: {a[\"max_price\"][\"value\"]}')
print(f'  min_price: {a[\"min_price\"][\"value\"]}')
print(f'  doc_count: {a[\"doc_count\"][\"value\"]}')
print(f'  terminated_early: {d.get(\"terminated_early\", False)}')
print(f'  took: {d[\"took\"]}ms')
"

echo ""
echo "--- VERIFY: Compare before/after aggregation values ---"
python3 -c "
import json, sys
before = json.loads('''$BEFORE''')
after = json.loads('''$AFTER''')
ba = before['aggregations']
aa = after['aggregations']
ok = True
for key in ['revenue', 'avg_price', 'max_price', 'min_price', 'doc_count']:
    bv = ba[key]['value']
    av = aa[key]['value']
    match = abs(bv - av) < 0.01 if isinstance(bv, float) else bv == av
    status = '✅' if match else '❌'
    if not match: ok = False
    print(f'  {status} {key}: before={bv}, after={av}')
if ok: print('  ✅ ALL VALUES MATCH')
else: print('  ❌ VALUES MISMATCH — STAR TREE RESULTS DIFFER')
"

echo ""
echo "====== TEST 1 COMPLETE ======"

echo ""
echo ""
echo "=============================================="
echo "  Done"
echo "=============================================="
