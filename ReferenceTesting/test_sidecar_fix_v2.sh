#!/bin/bash
# End-to-end test for the sidecar star tree upgrade fix — v2
# Tests: upgrade API, metric aggregations, terms aggregation, doc count, deletes
set -e

BASE="http://localhost:9200"
INDEX="test_sidecar_v2"
PASS=0
FAIL=0

check() {
  local name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "   PASS: $name (expected=$expected, got=$actual)"
    PASS=$((PASS+1))
  else
    echo "   FAIL: $name (expected=$expected, got=$actual)"
    FAIL=$((FAIL+1))
  fi
}

echo "=== Sidecar Star Tree Upgrade Fix Test v2 ==="
echo ""

# 1. Clean up
curl -s -X DELETE "$BASE/$INDEX" 2>/dev/null || true
sleep 1

# 2. Create index WITHOUT star tree
echo "1. Creating index without star tree..."
curl -s -X PUT "$BASE/$INDEX" -H 'Content-Type: application/json' -d '{
  "settings": { "index": { "number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "1s" } },
  "mappings": { "properties": {
    "status": { "type": "keyword" },
    "category": { "type": "keyword" },
    "amount": { "type": "integer" },
    "price": { "type": "float" }
  }}
}' > /dev/null

# 3. Index 1000 docs
echo "2. Indexing 1000 documents..."
for i in $(seq 1 200); do
  curl -s -X POST "$BASE/$INDEX/_bulk" -H 'Content-Type: application/x-ndjson' -d '
{"index":{}}
{"status":"active","category":"electronics","amount":100,"price":29.99}
{"index":{}}
{"status":"active","category":"clothing","amount":50,"price":19.99}
{"index":{}}
{"status":"inactive","category":"electronics","amount":75,"price":49.99}
{"index":{}}
{"status":"active","category":"food","amount":200,"price":9.99}
{"index":{}}
{"status":"inactive","category":"clothing","amount":30,"price":39.99}
' > /dev/null
done

# 4. Delete 100 docs to test soft deletes handling
echo "3. Deleting 100 docs (soft deletes test)..."
curl -s -X POST "$BASE/$INDEX/_delete_by_query?refresh=true" -H 'Content-Type: application/json' -d '{
  "query": { "range": { "amount": { "lte": 30 } } },
  "max_docs": 100
}' > /dev/null

curl -s -X POST "$BASE/$INDEX/_refresh" > /dev/null
sleep 1

# 5. Baseline measurements
echo ""
echo "=== BEFORE UPGRADE ==="
DOC_COUNT_BEFORE=$(curl -s "$BASE/$INDEX/_count" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
echo "   Doc count: $DOC_COUNT_BEFORE"

TERMS_BEFORE=$(curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": { "by_status": { "terms": { "field": "status" } } }
}' | python3 -c "
import sys, json
r = json.load(sys.stdin)
buckets = r['aggregations']['by_status']['buckets']
for b in buckets:
    print(f'{b[\"key\"]}:{b[\"doc_count\"]}', end=' ')
")
echo "   Terms by_status: $TERMS_BEFORE"

SUM_BEFORE=$(curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": { "total": { "sum": { "field": "amount" } } }
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['aggregations']['total']['value'])")
echo "   Sum amount: $SUM_BEFORE"

# 6. Run upgrade
echo ""
echo "=== RUNNING UPGRADE ==="
UPGRADE=$(curl -s -X POST "$BASE/$INDEX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "default",
    "ordered_dimensions": [
      { "name": "status" },
      { "name": "category" }
    ],
    "metrics": [
      { "name": "amount", "stats": ["sum", "value_count", "min", "max"] },
      { "name": "price", "stats": ["sum", "value_count", "min", "max"] }
    ],
    "max_leaf_docs": 10000
  }
}')
echo "   Result: $UPGRADE"
UPGRADE_SUCCESS=$(echo "$UPGRADE" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('_shards',{}).get('successful',0))")

# 7. Post-upgrade measurements
echo ""
echo "=== AFTER UPGRADE ==="
DOC_COUNT_AFTER=$(curl -s "$BASE/$INDEX/_count" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
echo "   Doc count: $DOC_COUNT_AFTER"

TERMS_AFTER=$(curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": { "by_status": { "terms": { "field": "status" } } }
}' | python3 -c "
import sys, json
r = json.load(sys.stdin)
buckets = r['aggregations']['by_status']['buckets']
for b in buckets:
    print(f'{b[\"key\"]}:{b[\"doc_count\"]}', end=' ')
")
echo "   Terms by_status: $TERMS_AFTER"

TERMS_CAT=$(curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": { "by_cat": { "terms": { "field": "category" } } }
}' | python3 -c "
import sys, json
r = json.load(sys.stdin)
buckets = r['aggregations']['by_cat']['buckets']
for b in buckets:
    print(f'{b[\"key\"]}:{b[\"doc_count\"]}', end=' ')
")
echo "   Terms by_category: $TERMS_CAT"

SUM_AFTER=$(curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": { "total": { "sum": { "field": "amount" } } }
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['aggregations']['total']['value'])")
echo "   Sum amount: $SUM_AFTER"

MIN_AFTER=$(curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": { "min_amt": { "min": { "field": "amount" } } }
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['aggregations']['min_amt']['value'])")
echo "   Min amount: $MIN_AFTER"

MAX_AFTER=$(curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": { "max_amt": { "max": { "field": "amount" } } }
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['aggregations']['max_amt']['value'])")
echo "   Max amount: $MAX_AFTER"

# 8. Verify
echo ""
echo "=== VERIFICATION ==="
check "Upgrade succeeded" "1" "$UPGRADE_SUCCESS"
check "Doc count preserved" "$DOC_COUNT_BEFORE" "$DOC_COUNT_AFTER"
check "Sum preserved" "$SUM_BEFORE" "$SUM_AFTER"
check "Terms before=after" "$TERMS_BEFORE" "$TERMS_AFTER"

# Check terms has non-empty buckets
BUCKET_COUNT=$(curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": { "by_status": { "terms": { "field": "status" } } }
}' | python3 -c "import sys,json; print(len(json.load(sys.stdin)['aggregations']['by_status']['buckets']))")
check "Terms has buckets" "2" "$BUCKET_COUNT"

echo ""
echo "=== RESULTS: $PASS passed, $FAIL failed ==="

# Cleanup
curl -s -X DELETE "$BASE/$INDEX" > /dev/null
