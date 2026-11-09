# merge_coverage — 2-shard reduce-correctness suite

Purpose: prove that **sharding the data across two shards does not change the answer** for the
analytics-engine route — i.e. that partial→final aggregation, sort-merge, top-N gather, window
combine, and join shuffle are correct. The on-main `*PplIT` / `*CommandIT` suites stay at 1 shard
(function isolation); this suite is the distributed-stress companion and is purely additive.

## Structure

A single abstract base `TwoShardReduceTestCase` holds all the cross-shard machinery; thin per-category
IT classes only declare which dataset subdirs they cover and which queries are known-broken:

| IT class | dataset subdir(s) | tier |
|---|---|---|
| `TwoShardAggregationIT` | `agg/`, `approx/` | exact differential + approximate-golden |
| `TwoShardScalarIT` | `scalar/` | exact differential |
| `TwoShardShapeIT` | `shape/`, `window/` | exact differential |
| `TwoShardJoinIT` | `join/` | exact differential (all queries muted, see below) |
| `TwoShardCommandIT` | `cmd/` | exact differential |

(`IpFieldMultiShardIT` is a sibling — see the separate `ip_multishard` dataset.)

## How it works

The dataset is provisioned twice from this directory's `mapping.json` + `bulk.json`:

| index | shards | role |
|---|---|---|
| `merge_coverage_1shard` | 1 | baseline / oracle |
| `merge_coverage_2shard` | 2 | system under test (suite asserts both primaries are populated) |

Each query file uses the literal token `%INDEX%` for its source; the harness substitutes the two
index names and runs the query against both. A `merge_coverage_lookup` dimension index backs the
`lookup` command.

### Tiers

- **exact (differential):** the 1-shard result is the oracle — 1-shard must **equal** 2-shard
  (unordered, numeric-tolerant; multi-value `values`/`list` cells compared as multisets). No
  hand-computed expected needed. If `<dir>/expected/<name>.json` exists, the 2-shard result is
  *additionally* pinned to that absolute golden (catches a bug wrong identically at both shard counts).

- **approximate (golden + tolerance):** sketch aggregations (`distinct_count`=HLL, `percentile`=t-digest)
  drift across a partition merge, so differential equality is *not* used. Each ships a **required**
  golden truth in `approx/expected/<name>.json`; the 2-shard result is asserted within tolerance
  (rel 15% / abs 1.0) of it.

## Determinism rule (important)

Any `head N` / `sort … | head` test **must** carry a total ordering with a unique tie-breaker (`id`),
or the 2-shard gather returns a nondeterministic subset and the comparison is meaningless.

## Adding coverage

Drop a `<name>.ppl` in the appropriate subdir using `%INDEX%` as the source — that's the whole test.
Add `<dir>/expected/<name>.json` to also pin the absolute value. Approximate functions go under
`approx/` **with** a golden.

## Dataset shape

30 docs, 3 categories (`A`/`B`/`C`) × 10. Designed so every group spans both shards.

| field | type | notes |
|---|---|---|
| `id` | integer | 0–29, unique — sort tie-breaker |
| `category` | keyword | group key (A/B/C) |
| `region` | keyword | 2nd group key (east/west) |
| `amount` | integer | A:1–10 B:11–20 C:21–30 — int agg target |
| `price` | double | `amount × 1.5` — double agg target |
| `label` | keyword | `lbl(amount%5)` — string fns / dedup / distinct |
| `flag` | boolean | `amount` even |
| `ts` | date | `2024-01-(id+1)` — date fns |
| `opt` | integer | present on even `id` only (15 docs) — null-handling fns |
| `payload` | keyword | JSON string `{"v":amount}` — for `spath` |

## Coverage

- **`agg/` aggregations:** count, count(field), sum, avg, min, max (int + double), stddev_pop,
  stddev_samp, var_pop, var_samp, span; collection aggs `values`/`list` (multiset-normalized).
- **`approx/`:** distinct_count (global, by-group, several columns), `dc`, percentile (50/90),
  percentile_approx, median.
- **`scalar/`:** round, round/2, pow, abs, upper, lower, substring, replace, cast (→double/int/string),
  coalesce, ifnull, isnull, isnotnull, nullif, case, if, date_format.
- **`shape/`:** group-by single/multi-key/multi-agg, filtered group-by, sort/limit top-N with tie-break,
  string sort, dedup.
- **`window/`:** eventstats by group, streamstats running sum.
- **`join/`:** inner / left / semi / anti + join-then-group (self-joined on `%INDEX%`).
- **`cmd/`:** rex, table, spath, rename, fillnull, bin, top, chart, lookup, appendpipe.

## Muted (skipped) queries — single mechanism

Every known-broken query is listed in exactly one place: the overriding IT's `knownIssues()` map
(`<name>` → reason). Listed queries are **skipped** (never run) and logged as `MUTED: <reason>`; there
is no `@AwaitsFix` and no separate directory.

The complete current list (keep in sync with the `knownIssues()` maps):

```
distinct_count_label          TwoShardAggregationIT
distinct_count_by_cat         TwoShardAggregationIT
dc_label                      TwoShardAggregationIT
streamstats_sum               TwoShardShapeIT
join_inner_id_count           TwoShardJoinIT
join_inner_category_count     TwoShardJoinIT
join_left_count               TwoShardJoinIT
join_semi_count               TwoShardJoinIT
join_anti_count               TwoShardJoinIT
join_inner_by_category        TwoShardJoinIT
cmd_search                    TwoShardCommandIT
cmd_append                    TwoShardCommandIT
cmd_regex                     TwoShardCommandIT
cmd_multisearch               TwoShardCommandIT
cmd_appendcols                TwoShardCommandIT
cmd_timechart                 TwoShardCommandIT
cmd_appendpipe                TwoShardCommandIT
```

What's muted and why:

- **distinct_count / dc / distinct_count_by_cat** (`TwoShardAggregationIT`) — HLL cross-shard sketch
  merge over-counts repeated keyword sets: `distinct_count(label)` = 5 at 1 shard, 9 at 2; by-category
  5/5/5 vs 4/7/8. Correct for all-unique (`amount`=30) and low-card (`category`=3). Genuine merge bug.
- **streamstats_sum** (`TwoShardShapeIT`) — cumulative window, order-sensitive; the arrival-ordered
  gather computes the running aggregate over the wrong order (1-shard `1,3,6,10,…` vs 2-shard `216,2,5,9,…`).
- **all 6 join queries** (`TwoShardJoinIT`) and **cmd_appendpipe** (`TwoShardCommandIT`) — since #21867
  ("Lucene as a driving backend for shard-local count fragments") the bare `[ source ]` join arm / union
  branch is lucene-driven while the main arm stays datafusion, and `OpenSearchJoin` / `OpenSearchUnion`
  reject the mixed backends. Reduce-sound before #21867.
- **cmd_search / cmd_append / cmd_regex / cmd_multisearch** (`TwoShardCommandIT`) — carry a residual
  predicate that routes through the DataFusion indexed executor, which at 2 shards **SIGSEGVs the data
  node** during the Arrow variable-width-view C-data export (`upcallLinker.cpp:137`). These *must* be
  skipped rather than run — a crash would break unrelated tests.
- **cmd_appendcols** (not in the PPL grammar) and **cmd_timechart** (needs an `@timestamp` field).

## Not covered — inherently nondeterministic at 2 shards

`take(field, N)` returns the first N values in *arrival* order with no ordering guarantee, so 2 shards
legitimately yield a different subset (same hazard as `head N` without sort). A sort doesn't fix it
(the per-shard partials are still merged arrival-ordered). Not a reduce-correctness signal; left out.
