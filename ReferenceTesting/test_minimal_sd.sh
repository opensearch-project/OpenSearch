#!/bin/bash
set -e
HOST="localhost:9200"

echo "=== Create index ==="
curl -s -X DELETE "$HOST/test_sd" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/test_sd" -H 'Content-Type: application/json' -d '{
  "settings":{"number_of_shards":1,"number_of_replicas":0,"index.refresh_interval":"-1","index.merge.policy.max_merged_segment":"100gb","index.merge.policy.segments_per_tier":"100"},
  "mappings":{"properties":{"gender":{"type":"keyword"},"price":{"type":"double"},"qty":{"type":"integer"}}}
}' > /dev/null

echo "=== Index 100 docs ==="
python3 -c "
import json
with open('/tmp/sd_test.ndjson','w') as f:
    for i in range(100):
        f.write(json.dumps({'index':{'_index':'test_sd','_id':str(i)}})+'\n')
        f.write(json.dumps({'gender':'MALE','price':10.5,'qty':i})+'\n')
"
curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/sd_test.ndjson > /dev/null

echo "=== Flush ==="
curl -s -X POST "$HOST/test_sd/_flush?force=true" > /dev/null
curl -s -X POST "$HOST/test_sd/_refresh" > /dev/null

echo "=== Delete 10 docs ==="
curl -s -X POST "$HOST/test_sd/_delete_by_query?refresh=true" -H 'Content-Type: application/json' -d '{
  "query":{"range":{"qty":{"lte":9}}}
}' | python3 -c "import sys,json; print('  deleted:', json.load(sys.stdin).get('deleted'))"

echo "=== Flush to commit soft deletes ==="
curl -s -X POST "$HOST/test_sd/_flush?force=true" > /dev/null

echo "=== Segments ==="
curl -s "$HOST/test_sd/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                print(f'  {seg_name}: docs={seg[\"num_docs\"]}, del={seg.get(\"deleted_docs\",0)}, compound={seg.get(\"compound\",False)}')
"

echo "=== UPGRADE ==="
RESULT=$(curl -s -X POST "$HOST/test_sd/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree":{"name":"st","ordered_dimensions":[{"name":"gender"}],"metrics":[{"name":"price","stats":["sum","value_count"]}]}
}')
echo "$RESULT" | python3 -c "
import sys,json
r=json.load(sys.stdin)
print(f'  success={r[\"_shards\"][\"successful\"]}, failed={r[\"_shards\"][\"failed\"]}')
if r['_shards']['failed'] > 0:
    print('  ERROR:', json.dumps(r['_shards']['failures'][0]['reason'], indent=2)[:500])
"
