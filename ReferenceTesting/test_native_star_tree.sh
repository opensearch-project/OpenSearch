#!/bin/bash
# Test native star tree (created at index time) to see if nested terms+sum works correctly
set -e
HOST="localhost:9200"
IDX="native_st_test"

curl -s -X DELETE "$HOST/$IDX" > /dev/null 2>&1 || true

echo "Creating native star tree index..."
curl -s -X PUT "$HOST/$IDX" -H 'Content-Type: application/json' -d '{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "composite_index": true,
      "append_only.enabled": true
    }
  },
  "mappings": {
    "composite": {
      "st": {
        "type": "star_tree",
        "config": {
          "max_leaf_docs": 10000,
          "ordered_dimensions": [
            {"name": "customer_gender"},
            {"name": "currency"}
          ],
          "metrics": [
            {"name": "taxful_total_price", "stats": ["sum", "value_count", "min", "max", "avg"]}
          ]
        }
      }
    },
    "properties": {
      "customer_gender": {"type": "keyword"},
      "currency": {"type": "keyword"},
      "taxful_total_price": {"type": "half_float"}
    }
  }
}' | python3 -c "import sys,json;print(json.load(sys.stdin))"

echo "Indexing 10000 docs..."
python3 -c "
import json, random
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
with open('/tmp/native_st.ndjson', 'w') as f:
    for i in range(10000):
        f.write(json.dumps({'index': {'_index': '$IDX'}}) + '\n')
        f.write(json.dumps({
            'customer_gender': random.choice(genders),
            'currency': random.choice(currencies),
            'taxful_total_price': round(random.uniform(5, 500), 2)
        }) + '\n')
"
curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/native_st.ndjson > /dev/null
curl -s -X POST "$HOST/$IDX/_refresh" > /dev/null

echo ""
echo "Doc count: $(curl -s "$HOST/$IDX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")"

echo ""
echo "=== Plain terms ==="
curl -s -X POST "$HOST/$IDX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {"by_gender": {"terms": {"field": "customer_gender"}}}
}' | python3 -c "
import sys,json;r=json.load(sys.stdin)
print(f'terminated_early={r.get(\"terminated_early\",False)}')
for b in r['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: {b[\"doc_count\"]}')
"

echo ""
echo "=== Nested terms + sum (THE KEY TEST) ==="
curl -s -X POST "$HOST/$IDX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {"by_gender": {"terms": {"field": "customer_gender"}, "aggs": {"revenue": {"sum": {"field": "taxful_total_price"}}}}}
}' | python3 -c "
import sys,json;r=json.load(sys.stdin)
print(f'terminated_early={r.get(\"terminated_early\",False)}')
for b in r['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]}')
total_count = sum(b['doc_count'] for b in r['aggregations']['by_gender']['buckets'])
print(f'  Total count: {total_count}')
"

curl -s -X DELETE "$HOST/$IDX" > /dev/null
