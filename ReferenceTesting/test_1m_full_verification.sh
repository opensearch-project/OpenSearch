#!/bin/bash
# Full verification: 1M docs + 50k deletes + correctness check + reads during upgrade
set -e
HOST="localhost:9200"
INDEX="ecom_1m_verify"

echo "=== Setup: 1M docs, 50k deletes ==="
curl -s -X DELETE "$HOST/$INDEX" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/$INDEX" -H 'Content-Type: application/json' -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null

python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/verify_1m.ndjson', 'w') as f:
    for i in range(1000000):
        f.write(json.dumps({'index': {'_index': '$INDEX', '_id': str(i)}}) + '\n')
        dt = base + datetime.timedelta(hours=random.randint(0, 8760))
        f.write(json.dumps({
            'customer_gender': random.choice(genders),
            'currency': random.choice(currencies),
            'day_of_week': random.choice(days),
            'order_date': dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'taxful_total_price': round(random.uniform(5, 500), 2),
            'taxless_total_price': round(random.uniform(4, 450), 2),
            'total_quantity': random.randint(1, 20),
            'total_unique_products': random.randint(1, 10),
            'day_of_week_i': random.randint(0, 6),
            'customer_id': 'cust_' + str(random.randint(1,1000)),
            'order_id': 'ord_' + str(i),
            'type': 'order',
            'user': 'user_' + str(random.randint(1,500))
        }) + '\n')
print('Generated 1M docs')
"
TOTAL=$(wc -l < /tmp/verify_1m.ndjson)
OFF=1; BATCH=50000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/verify_1m.ndjson > /tmp/_vb.ndjson
  curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/_vb.ndjson > /dev/null
  OFF=$((END + 1))
done
curl -s -X POST "$HOST/$INDEX/_flush?force=true" > /dev/null

# Delete 50k
python3 -c "
import json, random
random.seed(99)
ids = random.sample(range(1000000), 50000)
with open('/tmp/verify_del.ndjson', 'w') as f:
    for i in ids:
        f.write(json.dumps({'delete': {'_index': '$INDEX', '_id': str(i)}}) + '\n')
"
OFF=1; DEL_BATCH=10000; DEL_TOTAL=$(wc -l < /tmp/verify_del.ndjson)
while [ $OFF -le $DEL_TOTAL ]; do
  END=$((OFF + DEL_BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/verify_del.ndjson > /tmp/_vd.ndjson
  curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/_vd.ndjson > /dev/null
  OFF=$((END + 1))
done
curl -s -X POST "$HOST/$INDEX/_flush?force=true" > /dev/null
curl -s -X POST "$HOST/$INDEX/_refresh" > /dev/null
echo "  Done. Doc count: $(curl -s "$HOST/$INDEX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")"

echo ""
echo "=== Baseline aggregation (BEFORE upgrade) ==="
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}}}}}}'
BASELINE=$(curl -s -X POST "$HOST/$INDEX/_search" -H 'Content-Type: application/json' -d "$QUERY")
EXPECTED_MALE=$(echo "$BASELINE" | python3 -c "import sys,json;d=json.load(sys.stdin);b=[x for x in d['aggregations']['by_gender']['buckets'] if x['key']=='MALE'][0];print(f\"{b['doc_count']}|{b['revenue']['value']}\")")
EXPECTED_FEMALE=$(echo "$BASELINE" | python3 -c "import sys,json;d=json.load(sys.stdin);b=[x for x in d['aggregations']['by_gender']['buckets'] if x['key']=='FEMALE'][0];print(f\"{b['doc_count']}|{b['revenue']['value']}\")")
echo "  MALE: count=$(echo $EXPECTED_MALE | cut -d'|' -f1), revenue=$(echo $EXPECTED_MALE | cut -d'|' -f2)"
echo "  FEMALE: count=$(echo $EXPECTED_FEMALE | cut -d'|' -f1), revenue=$(echo $EXPECTED_FEMALE | cut -d'|' -f2)"

echo ""
echo "=== Starting background reads ==="
LOGFILE="/tmp/verify_1m_reads.log"
> "$LOGFILE"
(
  MALE_EXPECTED_COUNT=$(echo $EXPECTED_MALE | cut -d'|' -f1)
  FEMALE_EXPECTED_COUNT=$(echo $EXPECTED_FEMALE | cut -d'|' -f1)
  while true; do
    RESULT=$(curl -s -X POST "$HOST/$INDEX/_search" -H 'Content-Type: application/json' -d "$QUERY" 2>&1)
    echo "$RESULT" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    buckets = {b['key']: b for b in d.get('aggregations',{}).get('by_gender',{}).get('buckets',[])}
    mc = buckets.get('MALE',{}).get('doc_count',0)
    fc = buckets.get('FEMALE',{}).get('doc_count',0)
    total = mc + fc
    if total == 0:
        print('EMPTY')
    elif mc == $MALE_EXPECTED_COUNT and fc == $FEMALE_EXPECTED_COUNT:
        print('MATCH')
    else:
        print(f'MISMATCH male={mc} female={fc}')
except:
    print('ERROR')
" >> "$LOGFILE" 2>&1
    sleep 0.2
  done
) &
READER_PID=$!
sleep 1

echo ""
echo "=== Triggering upgrade (TIMED) ==="
START=$(python3 -c "import time; print(time.time())")
RESULT=$(curl -s -X POST "$HOST/$INDEX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "ecom_star_tree",
    "ordered_dimensions": [{"name": "customer_gender"},{"name": "currency"},{"name": "day_of_week"},{"name": "order_date"}],
    "metrics": [
      {"name": "taxful_total_price", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "taxless_total_price", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "total_quantity", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "total_unique_products", "stats": ["sum", "avg", "value_count"]}
    ]
  }
}')
END=$(python3 -c "import time; print(time.time())")
echo "  Result: $RESULT"
echo "  Upgrade time: $(python3 -c "print(f'{$END - $START:.3f}')")s"

sleep 2
kill $READER_PID 2>/dev/null || true
wait $READER_PID 2>/dev/null || true

echo ""
echo "=== Read results during upgrade ==="
TOTAL_READS=$(wc -l < "$LOGFILE")
MATCH_READS=$(grep -c "^MATCH" "$LOGFILE" || echo 0)
EMPTY_READS=$(grep -c "^EMPTY" "$LOGFILE" || echo 0)
MISMATCH_READS=$(grep -c "^MISMATCH" "$LOGFILE" || echo 0)
ERROR_READS=$(grep -c "^ERROR" "$LOGFILE" || echo 0)
echo "  Total: $TOTAL_READS"
echo "  Correct (MATCH): $MATCH_READS"
echo "  Empty (engine swap): $EMPTY_READS"
echo "  WRONG values (MISMATCH): $MISMATCH_READS"
echo "  Errors: $ERROR_READS"

if [ "$MISMATCH_READS" != "0" ]; then
  echo "  !!! WRONG VALUES DETECTED !!!"
  grep "^MISMATCH" "$LOGFILE" | head -3
fi

echo ""
echo "=== Post-upgrade aggregation ==="
POST=$(curl -s -X POST "$HOST/$INDEX/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$POST" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  terminated_early: {d.get(\"terminated_early\", False)}')
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
"

echo ""
echo "=== Correctness comparison (before vs after) ==="
python3 -c "
import json
before = json.loads('$BASELINE')
after = json.loads('$(echo $POST | sed "s/'/\\\\'/g")')
bb = {b['key']: b for b in before['aggregations']['by_gender']['buckets']}
ab = {b['key']: b for b in after['aggregations']['by_gender']['buckets']}
all_match = True
for key in bb:
    if bb[key]['doc_count'] != ab[key]['doc_count']:
        print(f'  MISMATCH {key} count: {bb[key][\"doc_count\"]} vs {ab[key][\"doc_count\"]}')
        all_match = False
    if abs(bb[key]['revenue']['value'] - ab[key]['revenue']['value']) > 0.01:
        print(f'  MISMATCH {key} revenue: {bb[key][\"revenue\"][\"value\"]:.2f} vs {ab[key][\"revenue\"][\"value\"]:.2f}')
        all_match = False
if all_match:
    print('  ALL VALUES MATCH (before == after)')
else:
    print('  !!! VALUES DO NOT MATCH !!!')
"

echo ""
echo "=== Done ==="
