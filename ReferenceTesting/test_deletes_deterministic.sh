#!/bin/bash
# Deterministic delete test: docs with value=id, delete every 10th, verify exact sums
set -e
HOST="localhost:9200"
IDX="test_det_del"

echo "=== Deterministic Delete Test ==="
echo "  100k docs, value=id (1..100000), delete every 10th doc"
echo "  Expected: 90,000 live docs, sum=4,500,000,000"
echo ""

curl -s -X DELETE "$HOST/$IDX" > /dev/null 2>&1 || true

echo "1. Creating index..."
curl -s -X PUT "$HOST/$IDX" -H 'Content-Type: application/json' -d '{
  "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
  "mappings": {"properties": {
    "category": {"type": "keyword"},
    "value": {"type": "long"}
  }}
}' > /dev/null

echo "2. Generating and ingesting 100k docs..."
python3 -c "
import json
with open('/tmp/det_del.ndjson', 'w') as f:
    for i in range(1, 100001):
        cat = 'odd' if i % 2 == 1 else 'even'
        f.write(json.dumps({'index': {'_index': 'test_det_del', '_id': str(i)}}) + '\n')
        f.write(json.dumps({'category': cat, 'value': i}) + '\n')
"

TOTAL=$(wc -l < /tmp/det_del.ndjson)
OFF=1
COUNT=0
BATCH=20000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/det_del.ndjson > /tmp/_det_batch.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_det_batch.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  OFF=$((END + 1))
done
echo "  Ingested: $COUNT docs"

echo "3. Flush..."
curl -s -X POST "$HOST/$IDX/_flush?force=true" > /dev/null
curl -s -X POST "$HOST/$IDX/_refresh" > /dev/null

echo "4. Deleting every 10th doc (10,20,...,100000)..."
python3 -c "
import json
with open('/tmp/det_del_bulk.ndjson', 'w') as f:
    for i in range(10, 100001, 10):
        f.write(json.dumps({'delete': {'_index': 'test_det_del', '_id': str(i)}}) + '\n')
"
DEL=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
  --data-binary @/tmp/det_del_bulk.ndjson | python3 -c "import sys,json;d=json.load(sys.stdin);print(sum(1 for i in d['items'] if i['delete']['result']=='deleted'))")
echo "  Deleted: $DEL docs"

echo "5. Flush + refresh..."
curl -s -X POST "$HOST/$IDX/_flush?force=true" > /dev/null
curl -s -X POST "$HOST/$IDX/_refresh" > /dev/null
sleep 2

echo ""
echo "=== BEFORE UPGRADE ==="
BEFORE=$(curl -s -X POST "$HOST/$IDX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {
    "total_sum": {"sum": {"field": "value"}},
    "doc_count": {"value_count": {"field": "value"}},
    "by_cat": {"terms": {"field": "category"}, "aggs": {"cat_sum": {"sum": {"field": "value"}}}}
  }
}')
echo "$BEFORE" > /tmp/det_before.json
echo "$BEFORE" | python3 -c "
import sys, json
r = json.load(sys.stdin)
a = r['aggregations']
print(f'  Count: {int(a[\"doc_count\"][\"value\"])}')
print(f'  Sum: {int(a[\"total_sum\"][\"value\"])}')
for b in a['by_cat']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, sum={int(b[\"cat_sum\"][\"value\"])}')
"

echo ""
echo "=== UPGRADING ==="
curl -s -X POST "$HOST/$IDX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "st",
    "ordered_dimensions": [{"name": "category"}, {"name": "value"}],
    "metrics": [{"name": "value", "stats": ["sum", "value_count", "min", "max"]}]
  }
}'
echo ""
sleep 2

echo ""
echo "=== AFTER UPGRADE ==="
AFTER=$(curl -s -X POST "$HOST/$IDX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {
    "total_sum": {"sum": {"field": "value"}},
    "doc_count": {"value_count": {"field": "value"}},
    "by_cat": {"terms": {"field": "category"}, "aggs": {"cat_sum": {"sum": {"field": "value"}}}}
  }
}')
echo "$AFTER" > /tmp/det_after.json
echo "$AFTER" | python3 -c "
import sys, json
r = json.load(sys.stdin)
a = r['aggregations']
print(f'  Count: {int(a[\"doc_count\"][\"value\"])}')
print(f'  Sum: {int(a[\"total_sum\"][\"value\"])}')
for b in a['by_cat']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, sum={int(b[\"cat_sum\"][\"value\"])}')
"

echo ""
echo "=== VERIFICATION ==="
python3 << 'PYEOF'
import json, sys

before_str = sys.argv[1] if len(sys.argv) > 1 else ""
after_str = sys.argv[2] if len(sys.argv) > 2 else ""

# Read from files instead
with open('/tmp/det_before.json') as f:
    before = json.load(f)
with open('/tmp/det_after.json') as f:
    after = json.load(f)

ba = before['aggregations']
aa = after['aggregations']

expected_count = 90000
expected_sum = 4500000000

passed = 0
failed = 0

def check(name, expected, actual):
    global passed, failed
    if expected == actual:
        print(f'  PASS: {name} = {actual}')
        passed += 1
    else:
        print(f'  FAIL: {name} expected={expected}, got={actual}')
        failed += 1

after_count = int(aa['doc_count']['value'])
after_sum = int(aa['total_sum']['value'])
before_count = int(ba['doc_count']['value'])
before_sum = int(ba['total_sum']['value'])

check('After count == 90000', expected_count, after_count)
check('After sum == 4500000000', expected_sum, after_sum)
check('Count before==after', before_count, after_count)
check('Sum before==after', before_sum, after_sum)

before_buckets = {b['key']: (b['doc_count'], int(b['cat_sum']['value'])) for b in ba['by_cat']['buckets']}
after_buckets = {b['key']: (b['doc_count'], int(b['cat_sum']['value'])) for b in aa['by_cat']['buckets']}

for cat in ['odd', 'even']:
    if cat in before_buckets and cat in after_buckets:
        check(f'{cat} count before==after', before_buckets[cat][0], after_buckets[cat][0])
        check(f'{cat} sum before==after', before_buckets[cat][1], after_buckets[cat][1])

print()
print(f'=== RESULTS: {passed} passed, {failed} failed ===')
if failed > 0: sys.exit(1)
PYEOF

