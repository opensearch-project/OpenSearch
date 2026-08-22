#!/bin/zsh
CH=${CLICKHOUSE:-/tmp/clickhouse}
NAT=${OTEL_NATIVE:-/tmp/otel_10m.parquet}
JSN=${OTEL_JSON:-/tmp/otel_10m_json.parquet}

run() {  # label, file, sql  -> median of 5 in ms
  local label=$1 file=$2 sql=$3
  local -a t
  for i in 1 2 3 4 5; do
    local s=$(( $(date +%s%N) ))
    $CH local -q "$sql" --path /tmp/chtmp > /dev/null 2>&1
    local e=$(( $(date +%s%N) ))
    t+=$(( (e - s) / 1000000 ))
  done
  printf "%-46s %6s ms\n" "$label" "$(print -l ${t[@]} | sort -n | sed -n 3p)"
}

echo "=== 1. Presence check over all 10M rows ==="
run "  ours (parallel LIST)" $NAT "SELECT count(*), sum(length(\`Events.Name\`)>0), sum(length(\`Links.SpanId\`)>0) FROM file('$NAT',Parquet)"
run "  json strings" $JSN "SELECT count(*), sum(JSONLength(events)>0), sum(JSONLength(links)>0) FROM file('$JSN',Parquet)"

echo "=== 2. First link fields, LIMIT 100 ==="
run "  ours (parallel LIST)" $NAT "SELECT SpanId, \`Links.TraceId\`[1], \`Links.SpanId\`[1], \`Links.Attributes\`[1]['link.kind'] FROM file('$NAT',Parquet) WHERE length(\`Links.SpanId\`)>0 LIMIT 100"
run "  json strings" $JSN "SELECT SpanId, JSONExtractString(links,1,'traceId'), JSONExtractString(links,1,'spanId'), JSONExtractString(JSONExtractString(links,1,'attributes'),'link.kind') FROM file('$JSN',Parquet) WHERE JSONLength(links)>0 LIMIT 100"

echo "=== 3. First event fields, LIMIT 100 ==="
run "  ours (parallel LIST)" $NAT "SELECT \`Events.Name\`[1], \`Events.Attributes\`[1]['exception.type'] FROM file('$NAT',Parquet) WHERE length(\`Events.Name\`)>0 LIMIT 100"
run "  json strings" $JSN "SELECT JSONExtractString(events,1,'name'), JSONExtractString(JSONExtractString(events,1,'attributes'),'exception.type') FROM file('$JSN',Parquet) WHERE JSONLength(events)>0 LIMIT 100"

echo "=== 4. Explode all events, group by name ==="
run "  ours (parallel LIST)" $NAT "SELECT name, count(*) FROM file('$NAT',Parquet) ARRAY JOIN \`Events.Name\` AS name GROUP BY name ORDER BY 2 DESC"
run "  json strings" $JSN "SELECT JSONExtractString(e,'name') AS name, count(*) FROM file('$JSN',Parquet) ARRAY JOIN JSONExtractArrayRaw(events) AS e GROUP BY name ORDER BY 2 DESC"

echo "=== 5. Element-scoped: event named exception WITH exception.type=IOError ==="
run "  ours (parallel LIST)" $NAT "SELECT count(DISTINCT SpanId) FROM file('$NAT',Parquet) ARRAY JOIN \`Events.Name\` AS n, \`Events.Attributes\` AS a WHERE n='exception' AND a['exception.type']='IOError'"
run "  json strings" $JSN "SELECT count(DISTINCT SpanId) FROM file('$JSN',Parquet) ARRAY JOIN JSONExtractArrayRaw(events) AS e WHERE JSONExtractString(e,'name')='exception' AND JSONExtractString(JSONExtractString(e,'attributes'),'exception.type')='IOError'"
