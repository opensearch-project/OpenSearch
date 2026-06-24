#!/bin/bash
# Test: do new segments written AFTER upgrade get native star tree data?
# Without engine restart, the running IndexWriter still uses Lucene912Codec.
# This test verifies whether new segments have star tree or not.
set -e
HOST="localhost:9200"
IDX="test_new_segs"

echo "=============================================="
echo "  New Segments After Upgrade Test"
echo "=============================================="

curl -s -X DELETE "$HOST/$IDX" > /dev/null 2>&1 || true

echo "1. Create index + ingest 1000 docs..."
curl -s -X PUT "$HOST/$IDX" -H 'Content-Type: application/json' -d '{
  "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
  "mappings": {"properties": {
    "category": {"type": "keyword"},
    "status": {"type": "keyword"},
    "value": {"type": "long"}
  }}
}' > /dev/null

for i in $(seq 1 100); do
  curl -s -X POST "$HOST/$IDX/_bulk" -H 'Content-Type: application/x-ndjson' -d '
{"index":{}}
{"category":"A","status":"old","value":10}
{"index":{}}
{"category":"B","status":"old","value":20}
{"index":{}}
{"category":"A","status":"old","value":30}
{"index":{}}
{"category":"B","status":"old","value":40}
{"index":{}}
{"category":"A","status":"old","value":50}
{"index":{}}
{"category":"B","status":"old","value":60}
{"index":{}}
{"category":"A","status":"old","value":70}
{"index":{}}
{"category":"B","status":"old","value":80}
{"index":{}}
{"category":"A","status":"old","value":90}
{"index":{}}
{"category":"B","status":"old","value":100}
' > /dev/null
done
curl -s -X POST "$HOST/$IDX/_refresh" > /dev/null

echo "   Docs: $(curl -s "$HOST/$IDX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")"

echo ""
echo "2. Segments BEFORE upgrade:"
curl -s "$HOST/$IDX/_segments" | python3 -c "
import sys,json
d = json.load(sys.stdin)
for idx in d['indices'].values():
    for shards in idx['shards'].values():
        for shard in shards:
            for name, seg in shard['segments'].items():
                print(f'   {name}: {seg[\"num_docs\"]} docs, compound={seg[\"compound\"]}')
"

echo ""
echo "3. Upgrade..."
curl -s -X POST "$HOST/$IDX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "st",
    "ordered_dimensions": [{"name": "category"}, {"name": "status"}],
    "metrics": [{"name": "value", "stats": ["sum", "value_count", "min", "max"]}]
  }
}' | python3 -c "import sys,json;print(f'   {json.load(sys.stdin)}')"

echo ""
echo "4. Verify star tree works on OLD segments:"
curl -s -X POST "$HOST/$IDX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {"total": {"sum": {"field": "value"}}, "by_cat": {"terms": {"field": "category"}}}
}' | python3 -c "
import sys,json; r=json.load(sys.stdin)
print(f'   terminated_early={r.get(\"terminated_early\",False)}')
print(f'   sum={int(r[\"aggregations\"][\"total\"][\"value\"])}')
for b in r['aggregations']['by_cat']['buckets']:
    print(f'   {b[\"key\"]}: {b[\"doc_count\"]}')
"

echo ""
echo "5. Ingest 500 NEW docs (status=new, value=1000) AFTER upgrade..."
for i in $(seq 1 50); do
  curl -s -X POST "$HOST/$IDX/_bulk" -H 'Content-Type: application/x-ndjson' -d '
{"index":{}}
{"category":"A","status":"new","value":1000}
{"index":{}}
{"category":"B","status":"new","value":1000}
{"index":{}}
{"category":"A","status":"new","value":1000}
{"index":{}}
{"category":"B","status":"new","value":1000}
{"index":{}}
{"category":"A","status":"new","value":1000}
{"index":{}}
{"category":"B","status":"new","value":1000}
{"index":{}}
{"category":"A","status":"new","value":1000}
{"index":{}}
{"category":"B","status":"new","value":1000}
{"index":{}}
{"category":"A","status":"new","value":1000}
{"index":{}}
{"category":"B","status":"new","value":1000}
' > /dev/null
done
curl -s -X POST "$HOST/$IDX/_flush?force=true" > /dev/null
curl -s -X POST "$HOST/$IDX/_refresh" > /dev/null

echo "   Total docs: $(curl -s "$HOST/$IDX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")"

echo ""
echo "6. Segments AFTER new ingest (look for new segment):"
curl -s "$HOST/$IDX/_segments" | python3 -c "
import sys,json
d = json.load(sys.stdin)
for idx in d['indices'].values():
    for shards in idx['shards'].values():
        for shard in shards:
            for name, seg in shard['segments'].items():
                print(f'   {name}: {seg[\"num_docs\"]} docs, compound={seg[\"compound\"]}')
"

echo ""
echo "7. Query with new docs — verify correctness:"
echo "   Expected: 1500 docs, sum = 100*550 + 500*1000 = 55000 + 500000 = 555000"
curl -s -X POST "$HOST/$IDX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {
    "total": {"sum": {"field": "value"}},
    "count": {"value_count": {"field": "value"}},
    "by_status": {"terms": {"field": "status"}, "aggs": {"s": {"sum": {"field": "value"}}}}
  }
}' | python3 -c "
import sys,json; r=json.load(sys.stdin)
a = r['aggregations']
total_count = int(a['count']['value'])
total_sum = int(a['total']['value'])
print(f'   terminated_early={r.get(\"terminated_early\",False)}')
print(f'   count={total_count} (expected 1500)')
print(f'   sum={total_sum} (expected 1050000)')
for b in a['by_status']['buckets']:
    print(f'   {b[\"key\"]}: count={b[\"doc_count\"]}, sum={int(b[\"s\"][\"value\"])}')

passed = 0
failed = 0
def check(n, e, a):
    global passed, failed
    if e == a:
        print(f'   PASS: {n}')
        passed += 1
    else:
        print(f'   FAIL: {n} expected={e} got={a}')
        failed += 1

check('total count', 1500, total_count)
check('total sum', 555000, total_sum)

status_map = {b['key']: (b['doc_count'], int(b['s']['value'])) for b in a['by_status']['buckets']}
if 'old' in status_map:
    check('old count', 1000, status_map['old'][0])
    check('old sum', 55000, status_map['old'][1])
else:
    print('   FAIL: old bucket missing')
    failed += 1
if 'new' in status_map:
    check('new count', 500, status_map['new'][0])
    check('new sum', 500000, status_map['new'][1])
else:
    print('   FAIL: new bucket missing')
    failed += 1

print(f'   === {passed} passed, {failed} failed ===')
"

# Cleanup — commented out for debugging
# curl -s -X DELETE "$HOST/$IDX" > /dev/null
