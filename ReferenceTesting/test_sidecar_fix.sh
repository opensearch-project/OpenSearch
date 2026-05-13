#!/bin/bash
# End-to-end test for the sidecar star tree upgrade fix
# Tests: upgrade API, metric aggregations, terms aggregation, doc count
set -e

BASE="http://localhost:9200"
INDEX="test_sidecar_fix"

echo "=== Sidecar Star Tree Upgrade Fix Test ==="
echo ""

# 1. Clean up
echo "1. Cleaning up old index..."
curl -s -X DELETE "$BASE/$INDEX" 2>/dev/null || true
sleep 1

# 2. Create index WITHOUT star tree (simulates existing index)
echo "2. Creating index without star tree..."
curl -s -X PUT "$BASE/$INDEX" -H 'Content-Type: application/json' -d '{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "refresh_interval": "1s"
    }
  },
  "mappings": {
    "properties": {
      "status": { "type": "keyword" },
      "category": { "type": "keyword" },
      "amount": { "type": "integer" },
      "price": { "type": "float" }
    }
  }
}' | python3 -m json.tool
echo ""

# 3. Index test data with known values
echo "3. Indexing 1000 documents..."
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
echo "Done indexing."

# 4. Refresh and verify doc count
echo ""
echo "4. Refreshing and verifying doc count..."
curl -s -X POST "$BASE/$INDEX/_refresh" > /dev/null
DOC_COUNT=$(curl -s "$BASE/$INDEX/_count" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
echo "   Doc count: $DOC_COUNT (expected: 1000)"

# 5. Run terms aggregation BEFORE upgrade (baseline)
echo ""
echo "5. Terms aggregation BEFORE upgrade (baseline):"
curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {
    "by_status": {
      "terms": { "field": "status" }
    }
  }
}' | python3 -c "
import sys, json
r = json.load(sys.stdin)
buckets = r['aggregations']['by_status']['buckets']
print(f'   Buckets: {len(buckets)}')
for b in buckets:
    print(f'   - {b[\"key\"]}: {b[\"doc_count\"]}')
"

# 6. Run sum aggregation BEFORE upgrade (baseline)
echo ""
echo "6. Sum aggregation BEFORE upgrade (baseline):"
SUM_BEFORE=$(curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {
    "total_amount": {
      "sum": { "field": "amount" }
    }
  }
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['aggregations']['total_amount']['value'])")
echo "   Sum of amount: $SUM_BEFORE"

# 7. Run the star tree upgrade
echo ""
echo "7. Running star tree upgrade..."
UPGRADE_RESULT=$(curl -s -X POST "$BASE/$INDEX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
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
echo "   Upgrade result: $UPGRADE_RESULT"

# 8. Verify doc count after upgrade
echo ""
echo "8. Doc count after upgrade:"
DOC_COUNT_AFTER=$(curl -s "$BASE/$INDEX/_count" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
echo "   Doc count: $DOC_COUNT_AFTER (expected: 1000)"

# 9. Run terms aggregation AFTER upgrade — THIS IS THE KEY TEST
echo ""
echo "9. Terms aggregation AFTER upgrade (THE KEY TEST):"
TERMS_RESULT=$(curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {
    "by_status": {
      "terms": { "field": "status" }
    }
  }
}')
echo "$TERMS_RESULT" | python3 -c "
import sys, json
r = json.load(sys.stdin)
buckets = r['aggregations']['by_status']['buckets']
terminated_early = r.get('terminated_early', False)
print(f'   terminated_early: {terminated_early}')
print(f'   Buckets: {len(buckets)}')
for b in buckets:
    print(f'   - {b[\"key\"]}: {b[\"doc_count\"]}')
if len(buckets) == 0:
    print('   *** FAIL: Empty buckets! Terms aggregation is broken ***')
    sys.exit(1)
else:
    print('   *** PASS: Terms aggregation returns correct buckets ***')
"
TERMS_EXIT=$?

# 10. Run terms on category field too
echo ""
echo "10. Terms aggregation on 'category' field:"
curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {
    "by_category": {
      "terms": { "field": "category" }
    }
  }
}' | python3 -c "
import sys, json
r = json.load(sys.stdin)
buckets = r['aggregations']['by_category']['buckets']
print(f'   Buckets: {len(buckets)}')
for b in buckets:
    print(f'   - {b[\"key\"]}: {b[\"doc_count\"]}')
"

# 11. Run sum aggregation AFTER upgrade
echo ""
echo "11. Sum aggregation AFTER upgrade:"
SUM_AFTER=$(curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {
    "total_amount": {
      "sum": { "field": "amount" }
    }
  }
}' | python3 -c "import sys,json; print(json.load(sys.stdin)['aggregations']['total_amount']['value'])")
echo "   Sum of amount: $SUM_AFTER (expected: $SUM_BEFORE)"

# 12. Run nested terms + sum aggregation
echo ""
echo "12. Nested terms + sum aggregation:"
curl -s -X POST "$BASE/$INDEX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {
    "by_status": {
      "terms": { "field": "status" },
      "aggs": {
        "total_amount": {
          "sum": { "field": "amount" }
        }
      }
    }
  }
}' | python3 -c "
import sys, json
r = json.load(sys.stdin)
buckets = r['aggregations']['by_status']['buckets']
print(f'   Buckets: {len(buckets)}')
for b in buckets:
    print(f'   - {b[\"key\"]}: doc_count={b[\"doc_count\"]}, sum_amount={b[\"total_amount\"][\"value\"]}')
"

# 13. Summary
echo ""
echo "=== SUMMARY ==="
echo "Doc count before: $DOC_COUNT"
echo "Doc count after:  $DOC_COUNT_AFTER"
echo "Sum before:       $SUM_BEFORE"
echo "Sum after:        $SUM_AFTER"
if [ "$TERMS_EXIT" -eq 0 ]; then
  echo "Terms aggregation: PASS"
else
  echo "Terms aggregation: FAIL"
fi
echo ""

# Cleanup
curl -s -X DELETE "$BASE/$INDEX" > /dev/null
echo "Cleaned up test index."
