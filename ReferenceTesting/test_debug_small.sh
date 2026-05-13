#!/bin/bash
set -e
HOST="localhost:9200"
IDX="test_debug2"

curl -s -X DELETE "$HOST/$IDX" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/$IDX" -H 'Content-Type: application/json' -d '{
  "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
  "mappings": {"properties": {"gender": {"type": "keyword"}, "category": {"type": "keyword"}, "amount": {"type": "integer"}}}
}' > /dev/null

for i in $(seq 1 20); do
  curl -s -X POST "$HOST/$IDX/_bulk" -H 'Content-Type: application/x-ndjson' -d '
{"index":{}}
{"gender":"MALE","category":"A","amount":100}
{"index":{}}
{"gender":"FEMALE","category":"B","amount":50}
{"index":{}}
{"gender":"MALE","category":"A","amount":75}
{"index":{}}
{"gender":"FEMALE","category":"B","amount":200}
{"index":{}}
{"gender":"MALE","category":"C","amount":30}
' > /dev/null
done
curl -s -X POST "$HOST/$IDX/_refresh" > /dev/null
echo "Docs: $(curl -s "$HOST/$IDX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")"

echo ""
echo "=== BEFORE UPGRADE ==="
echo "Terms:"
curl -s -X POST "$HOST/$IDX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {"g": {"terms": {"field": "gender"}}}
}' | python3 -c "
import sys,json; r=json.load(sys.stdin)
print(f'  terminated_early={r.get(\"terminated_early\",False)}')
for b in r['aggregations']['g']['buckets']:
    print(f'  {b[\"key\"]}: {b[\"doc_count\"]}')
"

echo "Sum:"
curl -s -X POST "$HOST/$IDX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {"s": {"sum": {"field": "amount"}}}
}' | python3 -c "
import sys,json; r=json.load(sys.stdin)
print(f'  terminated_early={r.get(\"terminated_early\",False)}, sum={r[\"aggregations\"][\"s\"][\"value\"]}')
"

echo ""
echo "=== UPGRADING ==="
curl -s -X POST "$HOST/$IDX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "st",
    "ordered_dimensions": [{"name": "gender"}, {"name": "category"}],
    "metrics": [{"name": "amount", "stats": ["sum", "value_count"]}]
  }
}'
echo ""

sleep 2

echo ""
echo "=== AFTER UPGRADE ==="
echo "Terms:"
curl -s -X POST "$HOST/$IDX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {"g": {"terms": {"field": "gender"}}}
}' | python3 -c "
import sys,json; r=json.load(sys.stdin)
print(f'  terminated_early={r.get(\"terminated_early\",False)}')
for b in r['aggregations']['g']['buckets']:
    print(f'  {b[\"key\"]}: {b[\"doc_count\"]}')
if not r['aggregations']['g']['buckets']:
    print('  *** EMPTY BUCKETS ***')
"

echo "Sum:"
curl -s -X POST "$HOST/$IDX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {"s": {"sum": {"field": "amount"}}}
}' | python3 -c "
import sys,json; r=json.load(sys.stdin)
print(f'  terminated_early={r.get(\"terminated_early\",False)}, sum={r[\"aggregations\"][\"s\"][\"value\"]}')
"

echo ""
echo "=== LOGS ==="
grep -i "sidecar\|Populated\|Creating sidecar\|readSegmentId\|star tree upgrade\|CorruptIndex" /Users/dwivashu/OpenSearch/build/testclusters/runTask-0/logs/runTask.log 2>/dev/null | tail -30

# Cleanup
curl -s -X DELETE "$HOST/$IDX" > /dev/null
