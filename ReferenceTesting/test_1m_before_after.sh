#!/bin/bash
# 1M doc before/after upgrade comparison test
# Captures exact aggregation results BEFORE upgrade, then verifies they match AFTER
set -e
HOST="localhost:9200"
INDEX="ecom_1m_verify"

echo "=============================================="
echo "  1M Doc Before/After Upgrade Verification"
echo "=============================================="

# --- Step 1: Generate data ---
echo ""
echo "=== Step 1: Generate 1M docs ==="
python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/test_verify.ndjson', 'w') as f:
    for i in range(1000000):
        dt = base + datetime.timedelta(hours=random.randint(0, 8760))
        f.write(json.dumps({'index': {'_index': '$INDEX'}}) + '\n')
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

# --- Step 2: Create index ---
echo ""
echo "=== Step 2: Create index ==="
curl -s -X DELETE "$HOST/$INDEX" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/$INDEX" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
echo "Created $INDEX"

# --- Step 3: Ingest ---
echo ""
echo "=== Step 3: Ingest 1M docs ==="
TOTAL=$(wc -l < /tmp/test_verify.ndjson)
OFF=1
COUNT=0
BATCH=100000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/test_verify.ndjson > /tmp/_batch_verify.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_batch_verify.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
done
echo "  Total: $COUNT docs"

curl -s -X POST "$HOST/$INDEX/_refresh" > /dev/null

# --- Step 4: BEFORE upgrade — capture baseline ---
echo ""
echo "=============================================="
echo "  BEFORE UPGRADE — Baseline"
echo "=============================================="

echo ""
echo "--- Query 1: Sum of taxful_total_price ---"
BEFORE_SUM=$(curl -s -X POST "$HOST/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": { "total_revenue": { "sum": { "field": "taxful_total_price" } } }
}')
echo "$BEFORE_SUM" | python3 -c "
import sys, json
r = json.load(sys.stdin)
print(f'  took: {r[\"took\"]}ms')
print(f'  terminated_early: {r.get(\"terminated_early\", False)}')
print(f'  sum: {r[\"aggregations\"][\"total_revenue\"][\"value\"]}')
"
BEFORE_SUM_VAL=$(echo "$BEFORE_SUM" | python3 -c "import sys,json; print(json.load(sys.stdin)['aggregations']['total_revenue']['value'])")

echo ""
echo "--- Query 2: Terms on customer_gender + nested sum ---"
BEFORE_NESTED=$(curl -s -X POST "$HOST/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {
    "by_gender": {
      "terms": { "field": "customer_gender", "size": 10 },
      "aggs": {
        "revenue": { "sum": { "field": "taxful_total_price" } },
        "avg_qty": { "avg": { "field": "total_quantity" } }
      }
    }
  }
}')
echo "$BEFORE_NESTED" | python3 -c "
import sys, json
r = json.load(sys.stdin)
print(f'  took: {r[\"took\"]}ms')
print(f'  terminated_early: {r.get(\"terminated_early\", False)}')
buckets = r['aggregations']['by_gender']['buckets']
print(f'  buckets: {len(buckets)}')
for b in buckets:
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]}, avg_qty={b[\"avg_qty\"][\"value\"]}')
"
# Save structured baseline for comparison
BEFORE_NESTED_JSON=$(echo "$BEFORE_NESTED" | python3 -c "
import sys, json
r = json.load(sys.stdin)
buckets = r['aggregations']['by_gender']['buckets']
result = {}
for b in buckets:
    result[b['key']] = {'count': b['doc_count'], 'revenue': b['revenue']['value'], 'avg_qty': b['avg_qty']['value']}
print(json.dumps(result))
")

# --- Step 5: UPGRADE ---
echo ""
echo "=============================================="
echo "  UPGRADING TO STAR TREE"
echo "=============================================="
START=$(python3 -c "import time; print(time.time())")

RESP=$(curl -s -X POST "$HOST/$INDEX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
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
UPGRADE_TIME=$(python3 -c "print(f'{$END - $START:.3f}')")
echo "  Response: $RESP"
echo "  Upgrade time: ${UPGRADE_TIME}s"

# --- Step 6: AFTER upgrade — verify ---
echo ""
echo "=============================================="
echo "  AFTER UPGRADE — Verification"
echo "=============================================="

echo ""
echo "--- Query 1: Sum of taxful_total_price ---"
AFTER_SUM=$(curl -s -X POST "$HOST/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": { "total_revenue": { "sum": { "field": "taxful_total_price" } } }
}')
echo "$AFTER_SUM" | python3 -c "
import sys, json
r = json.load(sys.stdin)
print(f'  took: {r[\"took\"]}ms')
print(f'  terminated_early: {r.get(\"terminated_early\", False)}')
print(f'  sum: {r[\"aggregations\"][\"total_revenue\"][\"value\"]}')
"
AFTER_SUM_VAL=$(echo "$AFTER_SUM" | python3 -c "import sys,json; print(json.load(sys.stdin)['aggregations']['total_revenue']['value'])")

echo ""
echo "--- Query 2: Terms on customer_gender + nested sum ---"
AFTER_NESTED=$(curl -s -X POST "$HOST/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {
    "by_gender": {
      "terms": { "field": "customer_gender", "size": 10 },
      "aggs": {
        "revenue": { "sum": { "field": "taxful_total_price" } },
        "avg_qty": { "avg": { "field": "total_quantity" } }
      }
    }
  }
}')
echo "$AFTER_NESTED" | python3 -c "
import sys, json
r = json.load(sys.stdin)
print(f'  took: {r[\"took\"]}ms')
print(f'  terminated_early: {r.get(\"terminated_early\", False)}')
buckets = r['aggregations']['by_gender']['buckets']
print(f'  buckets: {len(buckets)}')
for b in buckets:
    print(f'    {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]}, avg_qty={b[\"avg_qty\"][\"value\"]}')
"

# --- Step 7: COMPARISON ---
echo ""
echo "=============================================="
echo "  COMPARISON"
echo "=============================================="

python3 -c "
import json, sys

before_sum = float('$BEFORE_SUM_VAL')
after_sum = float('$AFTER_SUM_VAL')
before_nested = json.loads('$BEFORE_NESTED_JSON')

# Parse after nested
after_resp = json.loads('''$(echo "$AFTER_NESTED" | tr -d '\n')''')
after_buckets = after_resp['aggregations']['by_gender']['buckets']
after_nested = {}
for b in after_buckets:
    after_nested[b['key']] = {'count': b['doc_count'], 'revenue': b['revenue']['value'], 'avg_qty': b['avg_qty']['value']}

passed = 0
failed = 0

def check(name, expected, actual, tolerance=0.01):
    global passed, failed
    if isinstance(expected, float) and isinstance(actual, float):
        diff = abs(expected - actual)
        rel = diff / max(abs(expected), 1e-10)
        if rel < tolerance:
            print(f'  PASS: {name} (before={expected}, after={actual}, diff={rel:.6f})')
            passed += 1
        else:
            print(f'  FAIL: {name} (before={expected}, after={actual}, diff={rel:.6f})')
            failed += 1
    else:
        if expected == actual:
            print(f'  PASS: {name} (before={expected}, after={actual})')
            passed += 1
        else:
            print(f'  FAIL: {name} (before={expected}, after={actual})')
            failed += 1

# Query 1: Sum
check('Sum taxful_total_price', before_sum, after_sum)

# Query 2: Nested terms + sum
check('Bucket count', len(before_nested), len(after_nested))

for key in before_nested:
    if key in after_nested:
        check(f'{key} doc_count', before_nested[key]['count'], after_nested[key]['count'])
        check(f'{key} revenue', before_nested[key]['revenue'], after_nested[key]['revenue'])
        check(f'{key} avg_qty', before_nested[key]['avg_qty'], after_nested[key]['avg_qty'])
    else:
        print(f'  FAIL: {key} missing in after-upgrade results')
        failed += 1

print()
print(f'=== RESULTS: {passed} passed, {failed} failed ===')
if failed > 0:
    sys.exit(1)
"

echo ""
echo "  Upgrade time: ${UPGRADE_TIME}s"

# Cleanup
curl -s -X DELETE "$HOST/$INDEX" > /dev/null
echo "  Cleaned up."
