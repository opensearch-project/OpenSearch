import subprocess, re, os

CH   = os.environ.get("CLICKHOUSE", "/tmp/clickhouse")
DF   = os.environ.get("DATAFUSION_CLI", "datafusion-cli")
DF55 = os.environ.get("DATAFUSION_CLI_55", "datafusion-cli")
MAIN = os.environ.get("OTEL_PARQUET", "/tmp/otel_traces.parquet")
RAG  = os.environ.get("OTEL_RAGGED", "/tmp/ragged.parquet")

def run_ch(sql, src):
    pre = f"CREATE VIEW spans AS SELECT * FROM file('{src}', Parquet);\n"
    p = subprocess.run([CH, "local", "--format", "TSV", "-q", pre + sql], capture_output=True, text=True)
    out = (p.stdout or p.stderr).strip()
    m = re.search(r"Code: (\d+).*?\((\w+)\)", out, re.S)
    return f"ERROR {m.group(2)}" if m else out

def run_df(sql, src, dialect=None):
    pre = ("set datafusion.sql_parser.dialect = databricks;\n" if dialect else "")
    pre += f"CREATE EXTERNAL TABLE spans STORED AS PARQUET LOCATION '{src}';\n"
    open("/tmp/_c.sql","w").write(pre + sql + ";\n")
    p = subprocess.run([DF55 if dialect else DF, "--format","tsv","-q","-f","/tmp/_c.sql"],
                       capture_output=True, text=True)
    out = (p.stdout or p.stderr).strip()
    if p.returncode != 0 or re.match(r"^(Error|.*Exception:)", out):
        return "ERROR " + re.sub(r"\s+"," ",out)[:70]
    lines = [l for l in out.splitlines() if l.strip() and not l.startswith(("+","|")) and "row(s)" not in l]
    return "\n".join(lines[1:]) if lines else ""   # strip datafusion's TSV header

def norm(s):
    s = s.strip().replace("\\N","NULL").replace("null","NULL")
    s = re.sub(r"\bT(\d\d:)", r" \1", s)
    s = re.sub(r"(\d)\.0+\b", r"\1", s)
    s = re.sub(r"(:\d\d)\.0+\b", r"\1", s)
    return re.sub(r"\s+"," ",s).strip()

# (label, ch_sql, df_sql, src, dialect, expectation)
CASES = [
 ("Q2  sample span",
  "SELECT TraceId,SpanId,ServiceName,Duration,arrayStringConcat(`Events.Name`,',') FROM spans ORDER BY SpanId LIMIT 1",
  "SELECT \"TraceId\",\"SpanId\",\"ServiceName\",\"Duration\",array_to_string(\"Events.Name\",',') FROM spans ORDER BY \"SpanId\" LIMIT 1", MAIN, None, "match"),
 ("Q9  trace lookup",
  "SELECT count(*),arrayStringConcat(groupArray(SpanId),',') FROM (SELECT SpanId FROM spans WHERE TraceId='trace-01' ORDER BY Timestamp)",
  "SELECT count(*),array_to_string(array_agg(\"SpanId\" ORDER BY \"Timestamp\"),',') FROM spans WHERE \"TraceId\"='trace-01'", MAIN, None, "match"),
 ("Q10 trace lookup bounded",
  "WITH (SELECT min(Timestamp) FROM spans WHERE TraceId='trace-01') AS s,(SELECT max(Timestamp) FROM spans WHERE TraceId='trace-01') AS e SELECT count(*) FROM spans WHERE TraceId='trace-01' AND Timestamp>=s AND Timestamp<=e",
  "SELECT count(*) FROM spans WHERE \"TraceId\"='trace-01' AND \"Timestamp\">=(SELECT min(\"Timestamp\") FROM spans WHERE \"TraceId\"='trace-01') AND \"Timestamp\"<=(SELECT max(\"Timestamp\") FROM spans WHERE \"TraceId\"='trace-01')", MAIN, None, "match"),
 ("Q11 count+avg by lang",
  "SELECT ResourceAttributes['telemetry.sdk.language'],count(*),avg(Duration) FROM spans WHERE ResourceAttributes['host.name']='host-0' GROUP BY 1",
  "SELECT \"ResourceAttributes\"['telemetry.sdk.language'],count(*),avg(\"Duration\") FROM spans WHERE \"ResourceAttributes\"['host.name']='host-0' GROUP BY 1", MAIN, None, "match"),
 ("Q11b p90",
  "SELECT quantile(0.9)(Duration) FROM spans WHERE ResourceAttributes['host.name']='host-0'",
  "SELECT approx_percentile_cont(\"Duration\",0.9) FROM spans WHERE \"ResourceAttributes\"['host.name']='host-0'", MAIN, None, "diff: exact quantile vs t-digest"),
 ("Q12 attribute keys",
  "SELECT arrayStringConcat(arraySort(groupArrayDistinctArray(mapKeys(ResourceAttributes))),',') FROM spans",
  "SELECT array_to_string(array_sort(array_distinct(flatten(array_agg(map_keys(\"ResourceAttributes\"))))),',') FROM spans", MAIN, None, "match"),
 ("Q13 grafana tags",
  "SELECT SpanId,arrayStringConcat(arrayMap(k -> concat(k,'=',SpanAttributes[k]),mapKeys(SpanAttributes)),',') FROM spans ORDER BY SpanId LIMIT 2",
  "SELECT \"SpanId\",array_to_string(array_agg(concat(k,'=',v)),',') FROM (SELECT \"SpanId\",unnest(map_keys(\"SpanAttributes\")) AS k,unnest(map_values(\"SpanAttributes\")) AS v FROM spans) GROUP BY \"SpanId\" ORDER BY \"SpanId\" LIMIT 2", MAIN, None, "match"),
 ("Q16 total compression",
  f"SELECT sum(c.total_compressed_size),sum(c.total_uncompressed_size) FROM file('{MAIN}',ParquetMetadata) ARRAY JOIN row_groups AS rg ARRAY JOIN rg.columns AS c",
  f"SELECT sum(total_compressed_size),sum(total_uncompressed_size) FROM parquet_metadata('{MAIN}')", MAIN, None, "match"),
 ("Q17 top leaf by size",
  f"SELECT c.path,c.total_compressed_size FROM file('{MAIN}',ParquetMetadata) ARRAY JOIN row_groups AS rg ARRAY JOIN rg.columns AS c ORDER BY c.total_compressed_size DESC, c.name LIMIT 1",
  f"SELECT replace(path_in_schema,'\"',''),total_compressed_size FROM parquet_metadata('{MAIN}') ORDER BY total_compressed_size DESC, path_in_schema LIMIT 1", MAIN, None, "match"),
 ("A   correlated element-scoped",
  "SELECT count(DISTINCT SpanId) FROM spans ARRAY JOIN `Events.Name` AS name,`Events.Attributes` AS attrs WHERE name='retry' AND attrs['exception.type']='IOError'",
  "SELECT count(DISTINCT \"SpanId\") FROM (SELECT \"SpanId\",unnest(\"Events.Name\") AS name,unnest(\"Events.Attributes\") AS attrs FROM spans) WHERE name='retry' AND map_extract(attrs,'exception.type')[1]='IOError'", MAIN, None, "match"),
 ("B   uncorrelated document-scoped",
  "SELECT count(DISTINCT SpanId) FROM spans WHERE has(`Events.Name`,'retry') AND arrayExists(a -> a['exception.type']='IOError',`Events.Attributes`)",
  "SELECT count(DISTINCT \"SpanId\") FROM (SELECT \"SpanId\",\"Events.Name\" AS names,unnest(\"Events.Attributes\") AS attrs FROM spans) WHERE array_has(names,'retry') AND map_extract(attrs,'exception.type')[1]='IOError'", MAIN, None, "match"),
 ("C   correlated lambda",
  "SELECT count(DISTINCT SpanId) FROM spans WHERE arrayExists((n,a) -> n='retry' AND a['exception.type']='IOError',`Events.Name`,`Events.Attributes`)",
  "SELECT count(*) FROM spans WHERE array_any_match(arrays_zip(`Events.Name`,`Events.Attributes`), e -> e['1']='retry' AND map_extract(e['2'],'exception.type')[1]='IOError')", MAIN, "databricks", "match"),
 ("D   positive control",
  "SELECT count(DISTINCT SpanId) FROM spans ARRAY JOIN `Events.Name` AS name,`Events.Attributes` AS attrs WHERE name='exception' AND attrs['exception.type']='IOError'",
  "SELECT count(DISTINCT \"SpanId\") FROM (SELECT \"SpanId\",unnest(\"Events.Name\") AS name,unnest(\"Events.Attributes\") AS attrs FROM spans) WHERE name='exception' AND map_extract(attrs,'exception.type')[1]='IOError'", MAIN, None, "match"),
 ("E   zip made visible",
  "SELECT arrayStringConcat(groupArray(concat(name,':',ifNull(attrs['exception.type'],'-'))),' | ') FROM (SELECT name,attrs FROM spans ARRAY JOIN `Events.Name` AS name,`Events.Attributes` AS attrs WHERE SpanId='span-00')",
  "SELECT array_to_string(array_agg(concat(name,':',coalesce(map_extract(attrs,'exception.type')[1],'-'))),' | ') FROM (SELECT unnest(\"Events.Name\") AS name,unnest(\"Events.Attributes\") AS attrs FROM spans WHERE \"SpanId\"='span-00')", MAIN, None, "match"),
 ("H   Links group correlated",
  "SELECT count(DISTINCT SpanId) FROM spans ARRAY JOIN `Links.SpanId` AS lsid,`Links.Attributes` AS lattrs WHERE lattrs['link.kind']='follows_from'",
  "SELECT count(DISTINCT \"SpanId\") FROM (SELECT \"SpanId\",unnest(\"Links.SpanId\") AS lsid,unnest(\"Links.Attributes\") AS lattrs FROM spans) WHERE map_extract(lattrs,'link.kind')[1]='follows_from'", MAIN, None, "match"),
 ("I   INVARIANT: group lengths equal",
  "SELECT countIf(length(`Events.Name`)!=length(`Events.Attributes`)) AS ragged_events, countIf(length(`Links.SpanId`)!=length(`Links.Attributes`)) AS ragged_links FROM spans",
  "SELECT sum(CASE WHEN array_length(\"Events.Name\")!=array_length(\"Events.Attributes\") THEN 1 ELSE 0 END), sum(CASE WHEN array_length(\"Links.SpanId\")!=array_length(\"Links.Attributes\") THEN 1 ELSE 0 END) FROM spans", MAIN, None, "match"),
 ("J   total element count",
  "SELECT sum(length(`Events.Name`)),sum(length(`Events.Attributes`)) FROM spans",
  "SELECT sum(array_length(\"Events.Name\")),sum(array_length(\"Events.Attributes\")) FROM spans", MAIN, None, "match"),
 ("F   RAGGED element expansion",
  "SELECT arrayStringConcat(groupArray(concat(name,':',ifNull(attrs['exception.type'],'-'))),' | ') FROM spans ARRAY JOIN `Events.Name` AS name,`Events.Attributes` AS attrs",
  "SELECT array_to_string(array_agg(concat(name,':',coalesce(map_extract(attrs,'exception.type')[1],'-'))),' | ') FROM (SELECT unnest(\"Events.Name\") AS name,unnest(\"Events.Attributes\") AS attrs FROM spans)", RAG, None, "diff: CH rejects when both arrays are projected, DF pads with NULL"),
 ("F2  RAGGED count(*) only",
  "SELECT count(*) FROM spans ARRAY JOIN `Events.Name` AS name,`Events.Attributes` AS attrs",
  "SELECT count(*) FROM (SELECT unnest(\"Events.Name\") AS name,unnest(\"Events.Attributes\") AS attrs FROM spans)", RAG, None, "match"),
]
# Dropped: per-leaf max_definition_level / max_repetition_level. ClickHouse exposes them via the
# ParquetMetadata format but neither the aliased nor the positional tuple projection resolves them
# correctly, and DataFusion's parquet_metadata() does not expose them at all -- so it was never a
# cross-engine case.

rows=[]
for label, chq, dfq, src, dialect, expect in CASES:
    c, d = norm(run_ch(chq, src)), norm(run_df(dfq, src, dialect))
    same = (c == d)
    if expect == "match":
        verdict = "MATCH" if same else "FAIL"
    else:
        verdict = "DIFF-OK" if not same else "FAIL"
    rows.append((verdict, label, c, d, expect))

w=max(len(r[1]) for r in rows)
for v,label,c,d,expect in rows:
    print(f"{v:8} {label:<{w}}  CH={c!r}")
    if c != d:
        print(f"{'':8} {'':<{w}}  DF={d!r}" + (f"   [{expect}]" if expect!="match" else ""))
print()
ok=sum(1 for r in rows if r[0] in ("MATCH","DIFF-OK"))
print(f"{ok}/{len(rows)} as expected  ({sum(1 for r in rows if r[0]=='MATCH')} identical, {sum(1 for r in rows if r[0]=='DIFF-OK')} expected-diff, {sum(1 for r in rows if r[0]=='FAIL')} unexpected)")
