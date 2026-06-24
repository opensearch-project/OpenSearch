#!/bin/bash
set -e
HOST="localhost:9200"

echo "=== Doc count before deletes ==="
curl -s "$HOST/ecom_upgrade/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Aggregation before deletes ==="
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}}}}}}'
BEFORE=$(curl -s -X POST "$HOST/ecom_upgrade/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$BEFORE" | python3 -c "import sys,json;d=json.load(sys.stdin);print(f'took={d[\"took\"]}ms terminated_early={d.get(\"terminated_early\",False)}');[print(f'  {b[\"key\"]}: {b[\"doc_count\"]}') for b in d['aggregations']['by_gender']['buckets']]"

echo ""
echo "=== Deleting docs (delete_by_query: day_of_week_i < 2) ==="
DELRESULT=$(curl -s -X POST "$HOST/ecom_upgrade/_delete_by_query" -H 'Content-Type: application/json' -d '{"query":{"range":{"day_of_week_i":{"lt":2}}}}')
echo "$DELRESULT" | python3 -c "import sys,json;d=json.load(sys.stdin);print(f'Deleted: {d.get(\"deleted\",0)} docs')"

echo ""
echo "=== Refresh ==="
curl -s -X POST "$HOST/ecom_upgrade/_refresh" > /dev/null

echo ""
echo "=== Doc count after deletes ==="
curl -s "$HOST/ecom_upgrade/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Aggregation after deletes ==="
AFTER=$(curl -s -X POST "$HOST/ecom_upgrade/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER" | python3 -c "import sys,json;d=json.load(sys.stdin);print(f'took={d[\"took\"]}ms terminated_early={d.get(\"terminated_early\",False)}');[print(f'  {b[\"key\"]}: {b[\"doc_count\"]}') for b in d['aggregations']['by_gender']['buckets']]"

echo ""
echo "=== Server still alive? ==="
curl -s "$HOST" | python3 -c "import sys,json;print(json.load(sys.stdin)['name'])"

echo ""
echo "=== Done ==="
