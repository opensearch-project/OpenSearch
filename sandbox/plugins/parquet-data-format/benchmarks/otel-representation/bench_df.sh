#!/bin/zsh
DFJ=${DF_JSON:-/tmp/dfjson/target/release/df-json}
NAT=${OTEL_NATIVE:-/tmp/otel_10m.parquet}
JSN=${OTEL_JSON:-/tmp/otel_10m_json.parquet}

run() {  # label, file, sql -> median of 5 from ELAPSED_MS (excludes startup+registration)
  local label=$1 file=$2 sql=$3
  local -a t
  for i in 1 2 3 4 5; do
    local ms=$($DFJ -p $file -c "$sql" 2>&1 >/dev/null | sed -n 's/.*ELAPSED_MS=\([0-9.]*\).*/\1/p')
    [[ -z "$ms" ]] && { printf "%-46s  ERROR\n" "$label"; $DFJ -p $file -c "$sql" 2>&1 >/dev/null | head -2; return; }
    t+=${ms%.*}
  done
  printf "%-46s %6s ms\n" "$label" "$(print -l ${t[@]} | sort -n | sed -n 3p)"
}

echo "=== 1. Presence check over all 10M rows ==="
run "  ours (parallel LIST)" $NAT "SELECT count(*), sum(CASE WHEN array_length(\"Events.Name\")>0 THEN 1 ELSE 0 END), sum(CASE WHEN array_length(\"Links.SpanId\")>0 THEN 1 ELSE 0 END) FROM traces"
run "  json strings" $JSN "SELECT count(*), sum(CASE WHEN json_length(events)>0 THEN 1 ELSE 0 END), sum(CASE WHEN json_length(links)>0 THEN 1 ELSE 0 END) FROM traces"

echo "=== 2. First link fields, LIMIT 100 ==="
run "  ours (parallel LIST)" $NAT "SELECT \"SpanId\", \"Links.TraceId\"[1], \"Links.SpanId\"[1] FROM traces WHERE array_length(\"Links.SpanId\")>0 LIMIT 100"
run "  json strings" $JSN "SELECT \"SpanId\", json_get_str(links,0,'traceId'), json_get_str(links,0,'spanId') FROM traces WHERE json_length(links)>0 LIMIT 100"

echo "=== 3. First event fields, LIMIT 100 ==="
run "  ours (parallel LIST)" $NAT "SELECT \"Events.Name\"[1] FROM traces WHERE array_length(\"Events.Name\")>0 LIMIT 100"
run "  json strings" $JSN "SELECT json_get_str(events,0,'name') FROM traces WHERE json_length(events)>0 LIMIT 100"

echo "=== 4. Explode all events, group by name ==="
run "  ours (parallel LIST)" $NAT "SELECT name, count(*) FROM (SELECT unnest(\"Events.Name\") AS name FROM traces) GROUP BY name ORDER BY 2 DESC"
run "  json strings" $JSN "SELECT json_get_str(e,'name') AS name, count(*) FROM (SELECT unnest(json_get_array(events)) AS e FROM traces) GROUP BY name ORDER BY 2 DESC"

echo "=== 5. Element-scoped: name=exception AND exception.type=IOError ==="
run "  ours (parallel LIST)" $NAT "SELECT count(DISTINCT \"SpanId\") FROM (SELECT \"SpanId\", unnest(\"Events.Name\") AS n, unnest(\"Events.Attributes\") AS a FROM traces) WHERE n='exception' AND map_extract(a,'exception.type')[1]='IOError'"
run "  json strings" $JSN "SELECT count(DISTINCT \"SpanId\") FROM (SELECT \"SpanId\", unnest(json_get_array(events)) AS e FROM traces) WHERE json_get_str(e,'name')='exception' AND json_get_str(json_get(e,'attributes'),'exception.type')='IOError'"
