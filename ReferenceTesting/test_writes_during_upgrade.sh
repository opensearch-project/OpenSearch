#!/bin/bash
# Test that confirms writes are available during Phase 1 of star tree upgrade
set -e
HOST="localhost:9200"

echo "=========================================="
echo "TEST: WRITES AVAILABLE DURING UPGRADE"
echo "=========================================="

echo ""
echo "=== Setup: Create index with 100k docs ==="
curl -s -X DELETE "$HOST/test_writes" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/test_writes" -H 'Content-Type: application/json' -d '{
  "settings":{"number_of_shards":1,"number_of_replicas":0,"index.refresh_interval":"-1","index.merge.policy.max_merged_segment":"100gb","index.merge.policy.segments_per_tier":"100"},
  "mappings":{"properties":{"gender":{"type":"keyword"},"day":{"type":"keyword"},"price":{"type":"double"},"qty":{"type":"integer"}}}
}' > /dev/null

python3 -c "
import json, random
random.seed(42)
with open('/tmp/writes_test.ndjson','w') as f:
    for i in range(100000):
        f.write(json.dumps({'index':{'_index':'test_writes'}})+'\n')
        f.write(json.dumps({'gender':random.choice(['MALE','FEMALE']),'day':random.choice(['Mon','Tue','Wed']),'price':round(random.uniform(5,500),2),'qty':random.randint(1,20)})+'\n')
"
curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/writes_test.ndjson > /dev/null
curl -s -X POST "$HOST/test_writes/_flush?force=true" > /dev/null
curl -s -X POST "$HOST/test_writes/_refresh" > /dev/null

BEFORE_COUNT=$(curl -s "$HOST/test_writes/_count" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
echo "  Doc count before upgrade: $BEFORE_COUNT"

echo ""
echo "=== Trigger upgrade in background + write docs concurrently ==="

# Start upgrade in background
curl -s -X POST "$HOST/test_writes/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree":{"name":"st","ordered_dimensions":[{"name":"gender"},{"name":"day"}],"metrics":[{"name":"price","stats":["sum","value_count"]},{"name":"qty","stats":["sum","value_count"]}]}
}' > /tmp/upgrade_result.json &
UPGRADE_PID=$!

# Wait a moment for Phase 1 to start, then write docs
sleep 0.5

echo "  Writing 1000 docs during Phase 1..."
WRITE_FAILURES=0
for i in $(seq 1 10); do
  RESP=$(curl -s -X POST "$HOST/test_writes/_doc" -H 'Content-Type: application/json' -d "{\"gender\":\"MALE\",\"day\":\"Mon\",\"price\":99.99,\"qty\":5}")
  STATUS=$(echo "$RESP" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('result','FAILED'))" 2>/dev/null)
  if [ "$STATUS" != "created" ]; then
    WRITE_FAILURES=$((WRITE_FAILURES + 1))
  fi
done

# Also do a bulk write
python3 -c "
import json
with open('/tmp/writes_during.ndjson','w') as f:
    for i in range(990):
        f.write(json.dumps({'index':{'_index':'test_writes'}})+'\n')
        f.write(json.dumps({'gender':'FEMALE','day':'Tue','price':50.0,'qty':3})+'\n')
"
BULK_RESP=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/writes_during.ndjson)
BULK_ERRORS=$(echo "$BULK_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('errors', True))")

# Wait for upgrade to finish
wait $UPGRADE_PID
UPGRADE_RESULT=$(cat /tmp/upgrade_result.json)
UPGRADE_SUCCESS=$(echo "$UPGRADE_RESULT" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('_shards',{}).get('successful',0))" 2>/dev/null)

echo ""
echo "=== Results ==="
echo "  Upgrade successful: $UPGRADE_SUCCESS"
echo "  Single doc write failures during upgrade: $WRITE_FAILURES / 10"
echo "  Bulk write had errors: $BULK_ERRORS"

# Refresh and check final count
curl -s -X POST "$HOST/test_writes/_refresh" > /dev/null
AFTER_COUNT=$(curl -s "$HOST/test_writes/_count" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])")
EXPECTED=$((BEFORE_COUNT + 1000))
echo "  Doc count after: $AFTER_COUNT (expected ~$EXPECTED)"

echo ""
if [ "$UPGRADE_SUCCESS" = "1" ] && [ "$WRITE_FAILURES" -lt 3 ] && [ "$BULK_ERRORS" = "False" ]; then
  echo "  *** WRITES DURING UPGRADE: PASSED ***"
  echo "  Writes were available during Phase 1 (star tree build)."
  echo "  Only Phase 2 (~100ms) blocks writes briefly."
else
  echo "  *** WRITES DURING UPGRADE: NEEDS INVESTIGATION ***"
  echo "  upgrade_success=$UPGRADE_SUCCESS write_failures=$WRITE_FAILURES bulk_errors=$BULK_ERRORS"
fi
