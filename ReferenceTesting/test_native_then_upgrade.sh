#!/bin/bash
# Test: Index created WITH star tree natively, then try to upgrade with different config
# Expected: Should be REJECTED (index already has a star tree in its mapping)
set -euo pipefail
HOST="localhost:9200"
IDX="test_native_then_upgrade"

echo "=============================================="
echo "  Test: Native Star Tree + Attempt Upgrade"
echo "=============================================="

# Cleanup
curl -s -X DELETE "$HOST/$IDX" > /dev/null 2>&1 || true

# 1. Create index WITH star tree enabled natively
echo "1. Creating index WITH native star tree (at creation time)..."
curl -s -X PUT "$HOST/$IDX" -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "index.composite_index": true
  },
  "mappings": {
    "properties": {
      "category": {"type": "integer"},
      "region": {"type": "integer"},
      "price": {"type": "double"},
      "qty": {"type": "integer"}
    },
    "composite": {
      "native_star_tree": {
        "type": "star_tree",
        "config": {
          "ordered_dimensions": [{"name": "category"}, {"name": "region"}],
          "metrics": [{"name": "price", "stats": ["sum", "value_count", "min", "max"]}],
          "max_leaf_docs": 10000
        }
      }
    }
  }
}' | python3 -c "import sys,json;d=json.load(sys.stdin);print('   Created:', d.get('acknowledged'))"

# 2. Ingest some docs
echo ""
echo "2. Ingesting 1000 docs..."
python3 -c "
import json, random
with open('/tmp/native_upgrade.ndjson', 'w') as f:
    for i in range(1000):
        f.write(json.dumps({'index': {}}) + '\n')
        f.write(json.dumps({'category': random.randint(0,4), 'region': random.randint(0,2), 'price': round(random.uniform(1,100),2), 'qty': random.randint(1,20)}) + '\n')
"
curl -s -X POST "$HOST/$IDX/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/native_upgrade.ndjson > /dev/null
curl -s -X POST "$HOST/$IDX/_refresh" > /dev/null
COUNT=$(curl -s "$HOST/$IDX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")
echo "   Doc count: $COUNT"

# 3. Verify native star tree works
echo ""
echo "3. Verify native star tree is active..."
AGG=$(curl -s -X POST "$HOST/$IDX/_search?size=0" -H 'Content-Type: application/json' -d '{"aggs":{"s":{"sum":{"field":"price"}}}}')
EARLY=$(echo "$AGG" | python3 -c "import sys,json;print(json.load(sys.stdin).get('terminated_early', False))")
echo "   terminated_early=$EARLY (True=star tree active)"

# 4. Try upgrade with DIFFERENT config
echo ""
echo "4. Try upgrade API with different star tree config..."
echo "   Expected: REJECTED (index already has star tree 'native_star_tree' in mapping)"
RESP=$(curl -s -X POST "$HOST/$IDX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "different_star_tree",
    "ordered_dimensions": [{"name": "qty"}, {"name": "category"}],
    "metrics": [{"name": "price", "stats": ["sum"]}, {"name": "qty", "stats": ["sum"]}],
    "config": {"max_leaf_docs": 5000, "build_mode": "off_heap"}
  }
}')
echo "   Response: $RESP"

# 5. Try upgrade with same name but different dims
echo ""
echo "5. Try upgrade with same name (native_star_tree) but different dims..."
echo "   Expected: REJECTED (can't modify existing star tree config)"
RESP2=$(curl -s -X POST "$HOST/$IDX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "native_star_tree",
    "ordered_dimensions": [{"name": "qty"}, {"name": "region"}],
    "metrics": [{"name": "price", "stats": ["sum", "min"]}],
    "config": {"max_leaf_docs": 10000, "build_mode": "off_heap"}
  }
}')
echo "   Response: $RESP2"

echo ""
echo "=============================================="
# Cleanup
curl -s -X DELETE "$HOST/$IDX" > /dev/null
rm -f /tmp/native_upgrade.ndjson
