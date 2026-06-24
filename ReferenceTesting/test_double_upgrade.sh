#!/bin/bash
# Test: Upgrade with config A via API, then try to upgrade again with config B
# Expected: Second upgrade should be REJECTED (index already has star tree)
set -euo pipefail
HOST="localhost:9200"
IDX="test_double_upgrade"

echo "=============================================="
echo "  Test: Double Upgrade (A then B)"
echo "=============================================="

# Cleanup
curl -s -X DELETE "$HOST/$IDX" > /dev/null 2>&1 || true

# 1. Create index WITHOUT star tree
echo "1. Creating index (no star tree) + ingesting 1000 docs..."
curl -s -X PUT "$HOST/$IDX" -H 'Content-Type: application/json' -d '{
  "settings": {"number_of_shards": 1, "number_of_replicas": 0},
  "mappings": {"properties": {
    "category": {"type": "integer"},
    "region": {"type": "integer"},
    "price": {"type": "double"},
    "qty": {"type": "integer"}
  }}
}' > /dev/null

# Ingest
python3 -c "
import json, random
with open('/tmp/double_upgrade.ndjson', 'w') as f:
    for i in range(1000):
        f.write(json.dumps({'index': {}}) + '\n')
        f.write(json.dumps({'category': random.randint(0,4), 'region': random.randint(0,2), 'price': round(random.uniform(1,100),2), 'qty': random.randint(1,20)}) + '\n')
"
curl -s -X POST "$HOST/$IDX/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/double_upgrade.ndjson > /dev/null
curl -s -X POST "$HOST/$IDX/_refresh" > /dev/null
COUNT=$(curl -s "$HOST/$IDX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")
echo "   Doc count: $COUNT"

# 2. First upgrade with config A
echo ""
echo "2. Upgrade with CONFIG A (dims: category+qty, metric: price[sum,count])..."
RESP_A=$(curl -s -X POST "$HOST/$IDX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "star_tree_A",
    "ordered_dimensions": [{"name": "category"}, {"name": "qty"}],
    "metrics": [{"name": "price", "stats": ["sum", "value_count"]}],
    "config": {"max_leaf_docs": 10000, "build_mode": "off_heap"}
  }
}')
echo "   Response: $RESP_A"
FAILED_A=$(echo "$RESP_A" | python3 -c "import sys,json;print(json.load(sys.stdin)['_shards']['failed'])")
echo "   Failed shards: $FAILED_A"

# 3. Verify star tree A works
echo ""
echo "3. Verify star tree A is active..."
AGG=$(curl -s -X POST "$HOST/$IDX/_search?size=0" -H 'Content-Type: application/json' -d '{"aggs":{"s":{"sum":{"field":"price"}}}}')
EARLY=$(echo "$AGG" | python3 -c "import sys,json;print(json.load(sys.stdin).get('terminated_early', False))")
echo "   terminated_early=$EARLY (True=star tree active)"

# 4. Second upgrade with DIFFERENT config B (different name)
echo ""
echo "4. Try upgrade with CONFIG B (different name: star_tree_B)..."
echo "   Expected: REJECTED (index already has a star tree config)"
RESP_B=$(curl -s -X POST "$HOST/$IDX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "star_tree_B",
    "ordered_dimensions": [{"name": "region"}, {"name": "category"}],
    "metrics": [{"name": "qty", "stats": ["sum", "value_count"]}],
    "config": {"max_leaf_docs": 10000, "build_mode": "off_heap"}
  }
}')
echo "   Response: $RESP_B"

# 5. Try with same name but different dims
echo ""
echo "5. Try upgrade with same name (star_tree_A) but different dims..."
echo "   Expected: Should either be idempotent (no-op) or rejected"
RESP_C=$(curl -s -X POST "$HOST/$IDX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "star_tree_A",
    "ordered_dimensions": [{"name": "region"}, {"name": "qty"}],
    "metrics": [{"name": "price", "stats": ["sum", "min", "max"]}],
    "config": {"max_leaf_docs": 10000, "build_mode": "off_heap"}
  }
}')
echo "   Response: $RESP_C"

# 6. Idempotent: exact same config A
echo ""
echo "6. Idempotent: exact same config A again..."
echo "   Expected: SUCCESS (no-op, already upgraded)"
RESP_D=$(curl -s -X POST "$HOST/$IDX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "star_tree_A",
    "ordered_dimensions": [{"name": "category"}, {"name": "qty"}],
    "metrics": [{"name": "price", "stats": ["sum", "value_count"]}],
    "config": {"max_leaf_docs": 10000, "build_mode": "off_heap"}
  }
}')
echo "   Response: $RESP_D"
FAILED_D=$(echo "$RESP_D" | python3 -c "import sys,json;print(json.load(sys.stdin)['_shards']['failed'])")
echo "   Failed shards: $FAILED_D"

echo ""
echo "=============================================="
# Cleanup
curl -s -X DELETE "$HOST/$IDX" > /dev/null
rm -f /tmp/double_upgrade.ndjson
