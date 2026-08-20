# Can `DistributionEnforcementPass` be removed under native top-down planning?

Investigation on `feature/mpp-top-down-traits` (top-down already enabled). The question is deliberately
framed as *remove the pass*, not *add more cost heuristics to work around it*.

**Short answer: YES for roughly two-thirds of it, and the remaining third is blocked by EXECUTION-LAYER
limits, not by anything Volcano structurally cannot express.** Nothing in the pass is impossible for a
cost-based optimizer in principle. That makes full removal a real goal with a concrete blocker list,
rather than an aspiration.

## Classification of every responsibility

| step | what it does | category | removable under top-down? |
|---|---|---|---|
| 1 (peel) | peels CBO's exchange to recover input content + actual distribution | **self-inflicted** | YES — only needed because the pass re-decides placement |
| 1 (QTF exception) | must NOT peel an ER carrying `overrideRowType` (`___ugsi`) | self-inflicted | YES — disappears with the peel |
| 2 (leaf scan) | read the scan's trait | trait algebra | YES — trivially |
| 2b (perPartition Sort) | shard-local top-N rides its child | trait algebra | **ALREADY DONE** — now `OpenSearchSort.passThroughTraits` |
| 3a-pre (transparent) | Project/Filter ride the demand | trait algebra | **ALREADY DONE** — now their `passThroughTraits` |
| 3a-agg (PARTIAL/FINAL preserve) | re-instates a split CBO already placed correctly | **self-inflicted** | YES — it exists to undo step 1's peel |
| 3 (non-aware) | conservatively gathers every child of an unknown op | trait algebra | YES — the `null`-returning `PhysicalNode` default is exactly this |
| 3b (size floor) | distribute only above `min_rows` | **cost policy** | YES — belongs in the cost model, which now credits `inputRows/parallelism` |
| 3b-bcast (preserve broadcast) | re-emit CBO's broadcast rather than re-deriving | self-inflicted | YES — with the `RANDOM(SHARD)` derive gap fixed |
| 3c (SINGLE agg split) | builds PARTIAL/FINAL via `FinalAggCallBuilder` | **plan rewrite** | PARTLY — a rule exists (`OpenSearchAggregateSplitRule`); the *Variant-A no-reshuffle* choice is an execution constraint |
| 3d (shippable producer) | refuses to distribute a join with a gathered input | **execution capability** | NO — see blocker 2 |
| 4 (tier-boundary no-reuse) | never reuse a co-partitioned join input in place | **execution capability** | NO — see blocker 1 |

## The self-inflicted core (removable immediately, ~40% of the pass)

The pass **cannot trust the traits on the tree it is handed**, so it tracks distribution separately in
`Visited` and forces exchanges unconditionally. Measured:

- `buildReducer(` (unconditional gather): **5** call sites
- `buildShuffleExchange(` (unconditional): **1**
- `buildEnforcer(` (satisfies-gated, the honest one): **1**

The comments say so directly — `DistributionEnforcementPass.java:142`, `:450-451`, `:503-504`, `:558`:
*"buildReducer (not buildEnforcer): … trusts the child's stale CBO coordSingleton trait and would skip
the ER"*. Under top-down the trait on each node IS the decision, so all five forced builds collapse back
to the satisfies-gated `buildEnforcer`, and steps 1, 1-QTF, and 3a-agg disappear entirely (3a-agg exists
purely to undo the peel in step 1).

This is the strongest argument for removal: a large fraction of the pass is not distribution logic at
all, it is compensation for operating on a tree whose traits are stale.

### CORRECTION — measured properly, the traits are NOT authoritative (my first probe was too narrow)

My initial claim here ("traits are already authoritative, only 1 disagreement") was WRONG: I had
instrumented only `gatherIfNeeded`. Widening the probe to every forced-gather site across the FULL suite
gives **8 disagreements, and they point BOTH ways**:

```
3x  site=gatherSinkingProjects rel=OpenSearchJoin tracked=HASH[0](WORKER:p=3) onTrait=SINGLETON(COORDINATOR)
1x  site=gatherIfNeeded        rel=OpenSearchJoin tracked=SINGLETON(COORDINATOR) onTrait=HASH[0](WORKER:p=3)
```
(counts after de-duplicating the stderr echo)

The 3 `gatherSinkingProjects` cases are the code's documented failure mode exactly: tracked HASH(WORKER)
but the trait still says COORDINATOR — i.e. **genuinely stale**. Verified by experiment, not inspection:

| change | failures |
|---|---|
| baseline | 10 |
| trust the trait in `gatherIfNeeded` only | **10** (safe) |
| ALSO trust it in `sinkReducerBelowProjects` | **12** (breaks) |

So step 1 is NOT a mechanical deletion. Reverted the second half; back to 10.

### THE COUPLING that makes step 1 unsafe in isolation

`DistributionEnforcementPass.java:464-467` **depends on the staleness working in its favour**:

> "For a join input whose child is a lower distributed join the child's traitSet still carries the CBO
> coordSingleton trait (the pass tracks the derived HASH only in Visited), so buildEnforcer sees
> from=coordSingleton ⊭ HASH and inserts the inter-tier shuffle."

That is the tier-boundary shuffle (step 4) riding on a bug. Making traits authoritative would make
`satisfies()` return true and SKIP the inter-tier shuffle — silently collapsing the cascade. So:

**Step 1 and the `isTierBoundary` blocker are COUPLED and must be fixed together.** The prerequisite is
making the distribution trait TIER-AWARE so a lower join's `HASH(k,N)` does not satisfy a parent's
`HASH(k,N)` demand across a stage boundary (Presto models this with partitioning handles). Until then,
"trust the trait" removes the very inequality the cascade relies on.

Revised: the honest first step is **tier-aware `satisfies()`**, not "delete the workarounds".

### (superseded) earlier claim: under top-down the traits are ALREADY authoritative

Instrumented `gatherIfNeeded` to compare the pass's separately-tracked distribution against the trait
actually on the rel, across the whole `CascadeShuffleProbeTests` suite (26 tests). Result: **exactly one
disagreement**, and it points the OPPOSITE way to the code's premise:

```
PROBE11 STALE: tracked=SINGLETON(COORDINATOR) onTrait=HASH[0](WORKER:p=3) rel=OpenSearchJoin
```

The trait says the join is already WORKER+HASH; the pass's own tracking says COORDINATOR, so the pass
forces a gather the trait says is unnecessary. Under top-down the TRAIT is the better source of truth —
the reverse of the "stale CBO coordSingleton trait" comments at `:142`, `:450-451`, `:503-504`, `:558`.
Those comments describe bottom-up behaviour and are now misleading.

That single mismatch occurs in one of the two already-failing broadcast-under-cascade tests, i.e. it is
plausibly the same defect, not an independent one. So the "traits cannot be trusted" justification for
the separate `Visited` tracking and the 5 forced `buildReducer` calls **no longer holds** — which makes
step 1 of the removal sequence below a mechanical, evidence-backed change rather than a gamble.

## The two genuine blockers — both EXECUTION-layer, both concrete

### Blocker 1: the shuffle transport is hard-wired BINARY

`ShuffleBufferManager.ShuffleBuffer` has literal two-sided state, not an N-keyed map:
`leftData`/`rightData` (`:751-752`), `leftDoneCount`/`rightDoneCount` (`:753-754`),
`leftReady`/`rightReady` (`:757-758`). `ShuffleScanInstructionNode`'s `side` is documented as
`"left"` or `"right"` (`:28`). `GeneralShuffleDAGRewriter.findJoinOverTwoShuffles` (`:340`, `:350-352`)
only promotes an `OpenSearchJoin` whose **both** inputs are `OpenSearchShuffleExchange`.

Consequence: step 4's "a join input must never reuse a co-partitioned child in place" is not a
heuristic — collapsing N joins into one fragment with N shuffle leaves is unrunnable. Removing this
needs **N-ary shuffle transport** (map-keyed buffer slices + an N-input scan instruction + a rewriter
that promotes any-arity shuffle consumers).

### Blocker 2: only a DATA-NODE fragment can ship a shuffle partition

`resolveProducerSink` is invoked only from the shard and worker fragment paths
(`AnalyticsSearchService.java:508`, `:838`). `ReduceStageExecution` is built around a
`ReducingExchangeSink` (`:47`) and has no partitioned-shipping path at all.

Consequence: step 3d's refusal to distribute a join whose input is a gathered sub-stage (decorrelated
subqueries — TPC-H q2/q4/q15/q22) is a real structural limit. Removing it needs a **reduce stage that
can also be a shuffle producer**.

Encouragingly, `AnalyticsSearchService.java:504-507` already notes that shuffle production is
*instruction-driven* and "no dedicated stage type is needed" — a worker fragment can be both consumer
and producer. So blocker 2 is plumbing, not architecture: the reduce path needs the same
instruction-driven sink resolution the data-node paths already have.

### Not a blocker: step 3c Variant-A

Deliberately skipping the aggregate's `HASH(groupKeys)` demand is a consequence of blocker 1: a
group-key reshuffle whose consumer is a PARTIAL aggregate is a **single-input** shuffle edge, and the
rewriter only wires binary join edges. Fix blocker 1 and this becomes ordinary trait algebra.

## Recommended sequence (removal, not more heuristics)

1. **Make traits authoritative.** Delete steps 1 / 1-QTF / 3a-agg; convert the 5 forced `buildReducer`
   calls to satisfies-gated `buildEnforcer`. Pure deletion, no new mechanism. Expect this to shrink the
   pass by ~40% and to remove the whole "stale trait" comment class.
2. **Move the size floor into cost.** `min_rows` is a cost policy; with `inputRows/parallelism` now on
   the join, a small join is *already* not worth distributing. Make it a cost bias, not a veto.
3. **Fix the `RANDOM(SHARD)` derive gap** (with an `MPP_ENABLED` gate — a trait hook has no equivalent
   of a rule's `matches()`), which lets CBO form broadcast-under-cascade natively and retires 3b-bcast.
4. **N-ary shuffle transport** — the big one. Retires step 4's no-reuse rule AND step 3c Variant-A.
5. **Reduce-stage shuffle production** — retires step 3d.

After 1-3 the pass is a thin shim; after 4-5 it is deletable. Steps 1-3 are worth doing regardless,
because they remove compensation code rather than adding heuristics.

## Honest caveat

The pass also owns two *correctness* details that must not be lost in any refactor, both documented
in-code: `gatherSinkingProjects` keeps an aggregate's literal-arg Project in the same fragment as its
consumer (percentile's `50`), and keeps a computed-column Project (`round()` Int32-vs-Float64) from being
stranded below a cut. These are fragment-packaging concerns, not distribution — they need a home
(probably `DAGBuilder`) before the pass goes away.


---

## STEP 1 OUTCOME (commits `1f229655c94`, `c2b1421166f`)

**Done, and the root cause was NOT what either of my hypotheses said.**

### What I got wrong, twice

1. First I claimed traits were already authoritative → disproved by widening the probe (8 disagreements).
2. Then I claimed the fix was TIER-AWARE `satisfies()` → implemented it, regression-free at 10 failures,
   but it did **NOT** unlock trusting the trait (still 10→12). Tier-awareness is correct and worth keeping,
   but it was not the blocker.

### The actual root cause: the pass invalidates its own traits

`copyWithInputs` reuses `node.getTraitSet()`. So when the pass distributes a join it rebuilds it over
`HASH(WORKER)` inputs while the node itself **keeps CBO's `coordSingleton`**. The "stale CBO trait" the
five forced `buildReducer` calls exist to work around is therefore **self-inflicted by the pass**, not
inherited from CBO — the comments at `:142`, `:450`, `:503`, `:558` misattribute it.

Fix: restamp the rebuilt operator with what it now actually produces —
```java
RelNode restamped = out == null ? rebuilt
    : rebuilt.copy(rebuilt.getTraitSet().replace(out), rebuilt.getInputs());
```

### Evidence chain (each step measured, baseline 10 failures)

| change | failures |
|---|---|
| tier-aware `satisfies()` alone | 10 (no regression) |
| tier-aware + trust trait at `sinkReducerBelowProjects` | **12** (tier flag did NOT unlock it) |
| the 2 new failures | window-Project-over-join and non-decomposable-agg-over-join fail to GATHER |
| restamp rebuilt traits | 10 (no regression) |
| restamp + trust trait at BOTH gather sites | **10** — unlocked |

Forced-vs-gated exchange builds in the pass: **5 forced / 1 gated → 3 forced / 3 gated.**

### Why tier-awareness still ships

It replaces an accidental dependency with a stated rule. Step 4's inter-tier shuffle previously worked
*because* the trait was stale (`:464-467` documents relying on `from=coordSingleton ⊭ HASH`). Once traits
are restamped that inequality would vanish, so without the tier flag the cascade would silently collapse.
`OpenSearchDistribution.exchangeMaterialized` (set only by `buildShuffleExchange`) makes
"derived HASH does not satisfy a demand for materialized HASH" explicit. Deliberately NOT added to
`toString()` — that would churn every plan-shape golden and hide the real signal.

### Remaining 3 forced `buildReducer` calls

The root gather in `enforce()` and the PARTIAL/FINAL forced ER in `splitAggregate` — both construct nodes
whose traits the pass has not yet derived, so there is nothing to trust yet. Those need the aggregate
split to move into `OpenSearchAggregateSplitRule` (see Q1: widen its `isPartitioned` to accept
`HASH+WORKER`, drop `OpenSearchJoin` from `childForcesGather`), which is step 3 of the sequence.


---

## STEP 3 OUTCOME (commit `aca15b77fd0`) — half done, and the second half is a DESIGN decision

Goal: make `OpenSearchAggregateSplitRule` produce the PARTIAL/FINAL split so the pass's `splitAggregate`
becomes dead code, retiring 2 of the 3 remaining forced `buildReducer` calls.

### Landed: the two placement predicates are now consistent with the pass

1. `isPartitioned` accepted ONLY `RANDOM_DISTRIBUTED`; the pass's own `isPartitioned` means
   `HASH+WORKER` **exclusively**. The two covered DISJOINT sets, so the rule could never produce the
   agg-over-join split. Now accepts both.
2. `childForcesGather` returned true for `OpenSearchJoin` on the premise that "a join returns infinite
   cost over non-SINGLETON input". Under top-down that premise is **false** — a join is legal at
   `WORKER+HASH`. Now defers to the input's actual trait; `OpenSearchAggregate`'s SINGLE-over-partitioned
   infinite-cost gate remains the correctness backstop.

Measured: full suite still 10 failures, `CascadeShuffleProbeTests` + `AggregateSplitCostTests` = 34 tests
with only the 2 known broadcast goldens failing. Precommit green.

### NOT done: the rule still does not fire, and the blocker is deliberate

Instrumented both sides. `rule.matches()` DOES fire for a SINGLE aggregate over a 3-way distributed join
(`PROBE15 rule.matches agg=157 mode=SINGLE`), but `onMatch` never runs, and the pass's `splitAggregate`
still fires 20x across the suite.

Cause is `OpenSearchAggregate` itself declining the alternative top-down:
- `passThroughTraits` returns null for `SINGLE`
- `getDeriveMode()` returns `PROHIBITED` for non-PARTIAL

with the javadoc stating the intent outright: *"Whether to split it PARTIAL/FINAL is a decision the
post-CBO enforcement pass owns (it also gates on the size floor and the shuffle.aggregate.enabled
toggle), so top-down must not pre-empt it by claiming an alternative here."*

So finishing step 3 means REVERSING that decision: have `OpenSearchAggregate.passThroughTraits` claim the
SINGLE alternative, and move the size floor + `shuffle.aggregate.enabled` toggle into the rule's
`matches()`. That is coupled to **step 2** (floor → cost/matches), so the two should be done together
rather than in the order I originally listed.

Note the `PROHIBITED` derive mode is NOT the thing to relax: it exists because Calcite's default
`LEFT_FIRST` manufactures a `SINGLE`-over-partitioned variant that the cost gate then rejects as a
correctness violation — removing it reintroduced the "not enough rules … cost is still infinite" failure
class earlier in this branch. Only `passThroughTraits` should start claiming the alternative.

### Revised sequence

- ~~1. traits authoritative~~ **DONE** (`1f229655c94`, `c2b1421166f`) — 5 forced/1 gated → 3/3
- ~~3a. widen the rule's placement predicates~~ **DONE** (`aca15b77fd0`)
- **2+3b (do together).** Move `min_rows` + `shuffle.aggregate.enabled` into the rule's `matches()`, and
  have `OpenSearchAggregate.passThroughTraits` claim the SINGLE alternative. Retires `splitAggregate`
  and 2 of the 3 remaining forced gathers.
- 4. N-ary shuffle transport — retires step 4 no-reuse AND step 3c Variant-A.
- 5. Reduce-stage shuffle production — retires step 3d.


---

## STEP 2+3b OUTCOME (commit `434f62c27cd`) — toggle moved; floor and SINGLE-alternative BLOCKED

Attempted the combined change. Only one third of it is safe; the other two are blocked for reasons worth
recording because both look attractive on paper.

### Landed: the sub-toggle moved into the rule's `matches()`

`analytics.mpp.shuffle.aggregate.enabled` now gates `OpenSearchAggregateSplitRule.matches()`, matching how
`OpenSearchHashJoinSplitRule` gates on `MPP_ENABLED`. Full suite unchanged at 10 failures; the
sub-toggle-off test and all of `AggregateSplitCostTests` pass. Precommit green.

### BLOCKED 1: the size floor cannot move into `matches()` as-is → 82 failures

`analytics.mpp.distribute.min_rows` defaults to **1,000,000**, but the plan-shape tests drive the pass with
`minRows` as an **explicit parameter** (`DistributionEnforcementPass.enforce(..., /* minRows */ 1L, ...)`)
and never set the cluster setting. Their `mockTable` scans carry Calcite's default ~100 rows. So a rule
reading the SETTING sees 1,000,000, decides every aggregate is below the floor, and suppresses every split:
**10 → 82 failures**.

This is not merely a test artifact. It says the floor is currently a *caller-supplied* parameter, not a
planner-wide policy, and ~30 tests depend on being able to lower it per-call. Moving it into a rule means
either (a) plumbing an override into `PlannerContext` so tests can lower it the same way, or (b) doing what
the analysis originally recommended and expressing the floor as COST rather than a veto — the join already
credits `inputRows/parallelism`, so a 100-row aggregate is already not worth distributing on cost alone.
(b) is the better end state and removes the setting's veto semantics entirely.

### BLOCKED 2 — CORRECTED: it is 12 failures, not 59, and it is salvageable

**My "59" was measured BEFORE `aca15b77fd0` and `434f62c27cd` landed** (the two commits that widened the
split rule's `isPartitioned`/`childForcesGather` and moved the sub-toggle into `matches()`). Re-measured
at current HEAD with `--rerun-tasks`: **12 failures** (10 baseline + 2 new), reproduced independently
twice. Verified 103 XMLs / 1053 tests / 12 failures / **0 errors**, all `ComparisonFailure`.

A methodology trap worth recording: gradle only rewrites JUnit XML for classes it re-runs, so an
incremental run can leave ONE xml for 1053 tests and silently under-report. Always `--rerun-tasks` before
counting.

### (superseded) original claim: 59 failures

Making SINGLE claim the SINGLETON alternative (so Volcano explores it and gives the rule a chance to fire)
took 10 → **59**. Reverted. The declining javadoc is load-bearing beyond the two gates it cites: claiming
the alternative changes which aggregate shapes Volcano materializes far more broadly than expected. This
needs its own investigation, not a one-line flip.

Measured, for the record: `rule.matches()` DOES fire for a SINGLE aggregate over a distributed join
(`PROBE15 agg=157 mode=SINGLE`), but `onMatch` never runs and the pass's `splitAggregate` still fires 20x.
So the rule remains unable to replace the pass, and the 3 forced `buildReducer` calls stay.

### Where the sequence actually stands

- ~~1. traits authoritative~~ **DONE** — 5 forced/1 gated → 3/3
- ~~3a. widen the rule's placement predicates~~ **DONE**
- ~~3b-i. move the sub-toggle into matches()~~ **DONE**
- **2'. express the size floor as COST — INVALIDATED, see below.**
- **3b-ii. why does claiming the SINGLE alternative cost 49 extra failures?** — needs investigation before
  `splitAggregate` can be retired.
- 4. N-ary shuffle transport.
- 5. Reduce-stage shuffle production.


---

## STEP 2' IS INVALIDATED — cost cannot replace the size floor (measured)

I recommended expressing `min_rows` as cost rather than a veto, on the premise that "with
`inputRows/parallelism` on the join, a small aggregate/join is already not worth distributing on cost
alone". **That premise is false.** Verified two ways.

Arithmetic — the model has no per-STAGE cost, only per-exchange setup:
```
distributing swaps 2 ERs (2 * SETUP_COST 25 = 50 setup)
           for    2 shuffles + 1 ER (2 * 15 + 25 = 55 setup)
=> only +5 fixed cost, while the join term drops from rows/1 to rows/N
```
The parallelism divisor dominates that +5 at EVERY row count. Computed coord-vs-distributed at 100 /
1,000 / 100,000 / 10,000,000 rows: **distributed wins all four.**

Empirical — neutralized the floor (`aboveFloor = true`) and ran the suite:
`testEnforcementPass_smallJoinBelowFloorStaysCoordCentric` **FAILS** (26 tests, 3 failed vs baseline 2).
That test builds three 1,000-row tables with `minRows = 1_000_000` and asserts NO shuffle. So the floor
is the only thing suppressing distribution of a 1,000-row join; cost alone distributes it. Reverted.

**Conclusion: `min_rows` is load-bearing, not redundant.** Making it a cost term requires FIRST adding a
per-stage cost the model does not have — task dispatch, RPC round-trips, native session setup per
fragment — large relative to small row counts. That is a real modelling gap and the honest prerequisite;
`min_rows` is currently a crude proxy for it.

Revised recommendation: do NOT try to delete the floor. Either leave it in the pass, or (if it must move)
plumb a `PlannerContext` override so a rule can read a test-lowered value the way the pass reads its
explicit parameter. Adding a per-stage cost term is the principled fix but is its own project, and it
would change every strategy decision — it needs the TPC-H sweep as a guard, not just unit tests.


---

## STEP 3b-ii RESOLVED (commit `e471603eb9b`) — was a 2-test cost race, not a broad regression

### What the failures actually were

Both new failures are in `AggregateSplitCostTests`
(`testFourPredicateFilterBelowCountByKey_2shard`, `testSevenPredicateFilterBelowAvgByKey_2shard`), and
both are category **(a) different-but-valid plan** — NOT a dead end, NOT semantically wrong:

- The new plan is `OpenSearchAggregate(mode=SINGLE)` directly over `OpenSearchExchangeReducer`, i.e. the
  input IS gathered.
- `OpenSearchAggregate.computeSelfCost:329-331` prices `SINGLE` over non-singleton input at **infinite
  cost**, so an unsplit aggregate can never read partitioned input and under-count. Correctness is not at
  stake in either shape.

Mechanism: claiming the alternative materializes a `SINGLE`-over-`coordSingleton` subset that did not
previously exist, so the split rule re-fires there and emits `singleOnSingleton`. Volcano then costs both
legal plans and picks the cheaper. These are the ONLY two tests in the suite that stub
`when(table.getRowCount()).thenReturn(100d)`, and their own javadoc says the estimate is deliberately
driven to "the 1.0 floor" so that "without the gate the coordinator-PARTIAL wins on cost"
(`AggregateSplitCostTests.java:188-195`). At 1 estimated row a two-phase aggregate is pure overhead, so
the gather legitimately wins by ~1.1 cost units. The edit removed exactly the gate those tests pin.

### Shipped: the principled narrowing

Claim the SINGLETON alternative only when `shouldSkipPartialFinalSplit(this)` — i.e. only for aggregates
that CANNOT be split (STATE_EXPANDING / DISTINCT / cross-family non-prefix). For those, gather-then-run-
SINGLE is the only correct shape, so it can never out-compete a two-phase alternative because none
exists. Measured **10 failures** (baseline), and verified NOT dead code: the branch fires 5x across the
suite.

Deliberately did NOT take the alternative options — updating the 2 goldens would weaken a gate that
exists for a real cost hazard, and the unconditional version buys nothing extra today since the pass's
`splitAggregate` still owns the splittable cases.

### Also invalidated: the "fix it via step 2'" suggestion

The subagent proposed that step 2' (floor as cost) subsumes this. It does not — step 2' is independently
INVALIDATED (see the section above): with the floor neutralized, cost distributes a 1,000-row join and
`testEnforcementPass_smallJoinBelowFloorStaysCoordCentric` fails. The cost model has no per-stage term, so
it cannot express "too small to distribute" at any row count.

---

## STEP 4 OUTCOME (commits `1d3f3bbf31d`, + this one) — transport is N-ary; the no-reuse rule is now a POLICY, not a limit

Step 4 was the largest remaining item and the one the doc called "the big one". It landed in two halves.

### 4a: the transport is N-ary (`1d3f3bbf31d`)

Blocker 1 above described `ShuffleBufferManager.ShuffleBuffer` as having "literal two-sided state, not an
N-keyed map". That is now a slot-keyed map:

| before | after |
|---|---|
| `leftData` / `rightData` | `Map<String, Slot>`, each slot its own list |
| `leftDoneCount` / `rightDoneCount` | per-slot `doneCount` |
| `leftReady` / `rightReady` | per-slot `ready` latch; `awaitReady` waits every DECLARED slot |
| `leftSpill` / `rightSpill` | per-slot spill file, named `<stage>-<partition>-<slot>.spill` |
| `setExpectedSenders(int,int)` | `setExpectedSenders(Map<String,Integer>)` (binary form kept as a shim) |
| `ShuffleWorkerSetupInstructionNode(left,right)` | carries `Map<slot,expectedSenders>` on the wire |
| `WorkerLevel(worker, leftProducer, rightProducer, …)` | `WorkerLevel(worker, List<WorkerInput>, …)` |
| `findJoinOverTwoShuffles` (both inputs shuffles) | `findJoinOverShuffles` — any-arity join TREE whose LEAVES are all shuffles |

`ShuffleSlots` owns the labelling rule, and the load-bearing property is that **arity ≤ 2 still maps to
`left`/`right`** — so every binary payload, buffer key and spill-file name is byte-identical and the 31
existing binary buffer tests pass untouched. Only a 3+-input consumer uses `in<index>`.

Two things blocker 1 listed turned out NOT to be binary at all, which is why 4a was smaller than the doc
implied: the producer wire path (`ShuffleProducerInstructionNode`, `AnalyticsShuffleDataRequest`,
`ShuffleSenderImpl`, `createPartitionedSink`) already treats `side` as an OPAQUE string, and
`DataFusionFragmentConvertor.rewriteStageInputScans` already rewrites N `StageInputScan` leaves. Only the
buffer, the setup node, the `WorkerLevel` record, and the rewriter's promotion predicate were arity-bound.

Note Calcite's `Join` is always binary, so the N-ary shape is not a wide join node — it is a **collapsed
join TREE inside one fragment** (`Join(Shuffle, Join(Shuffle, Shuffle))`), one slot per leaf. Key checking
therefore had to change from position-wise (`info.leftKeys` vs input 0) to set-membership against every
join in the tree: in a collapsed tree a leaf feeds an inner join whose key lists are relative to THAT
join, not the root.

### 4b: the no-reuse rule survives — as a default, not a capability limit

With the transport N-ary, `isTierBoundary = n instanceof OpenSearchJoin` was neutralized to `false` to see
what the rule was actually holding back. Result: **19 failures (baseline 10)**, and all 9 new ones are
deliberate binary-tier shape assertions — e.g. `bushy (A⋈B)⋈(C⋈D) → three binary worker tiers
expected:<3> but was:<1>`. So the relaxation WORKS: a bushy 4-way join keyed entirely on `col0` collapses
3 tiers into 1, which is exactly the win step 4 exists for, and it is key-legal (every join keys on the
same column, so the lower join's output is already partitioned the way its parent needs).

But collapse is a TRADE, not a free win: it removes a shuffle round-trip and in exchange makes ONE worker
run several joins, raising peak memory — and DataFusion's hash-join build does not spill (the hazard behind
`sort_merge_join_min_rows`, and behind q17/q18/q21 at sf=10). JVM tests assert plan shape and cannot settle
that; only a cluster run can. So it ships as `analytics.mpp.collapse_copartitioned_tiers`, **default
false**, matching how `shuffle_column_prune` shipped. Two tests pin both directions
(`testGeneralShuffle_collapseToggleFusesCoPartitionedTiers` → 1 tier / 4 slots / 4 distinct producers, and
`…DefaultsOffKeepingBinaryTiers` → 3 binary tiers with `left`/`right` labels).

The `exchangeMaterialized` tier rule in `satisfies()` did NOT need relaxing: the demand
`OpenSearchDistributionTraitDef.hash()` builds is un-materialized, so a co-partitioned child satisfies it;
only a demand explicitly marked materialized insists on a real shuffle. Its comment is corrected to say
this is now a conservative default rather than a transport limit.

### What this means for the removal sequence

- Step 4's "no-reuse" responsibility is **retired as a limit** — the pass still applies it, but only as the
  default value of a documented toggle, which is a policy the pass is allowed to own.
- Step 3c Variant-A (skipping the aggregate's `HASH(groupKeys)` demand) is **still in force**. 4a removes the
  transport objection, but the remaining blocker is the REWRITER: `GeneralShuffleDAGRewriter` promotes only
  join trees, so a group-key shuffle whose consumer is a PARTIAL aggregate is still an unwired edge. That is
  a rewriter generalization (promote a shuffle-fed AGGREGATE stage), not a transport one — smaller than 4a.
- Step 5 (reduce-stage shuffle production) is untouched and remains the last big item.

---

## SUITE FULLY GREEN — 1987 tests / 0 failures (`659020bb804` … `52812f3e743`)

The 10 residual failures this doc kept describing as "plan-shape / rule-count goldens" are resolved. The
headline correction: **all 10 were REGRESSIONS from this branch, not stale goldens.** Verified by running
each at the branch point `af1e9151c63`, where all 10 pass. Regenerating them — which the earlier triage
recommended — would have written a row-count bug into the expected output.

| group | verdict | fix |
|---|---|---|
| 4 limit-shape | **CORRECTNESS BUG** — bare `LIMIT N` returned N×shards rows | `659020bb804` |
| 2 rule-count profiles | genuine bookkeeping (deleted derive rule) | `8c7cb9d515e` |
| 2 DAGShapeTests | genuine improvement (Project pushed into the shard stage) | `8c7cb9d515e` |
| 2 broadcast-under-cascade | goldens asserted a NON-OPTIMAL shape | `52812f3e743` |

**The limit bug.** `OpenSearchSort.ridesChildDistribution()` tested only collation, so an uncollated
`Sort(fetch=N)` "rode" its child's distribution and executed once per shard with nothing capping the
concatenated result. Riding REPLACED the coordinator's limit instead of adding a shard-local copy beside
it. `LIMIT 10` over 2 shards returned 20 rows — type-correct plan, wrong answers, no exception. Fixing it
forced two follow-ons, both about a Sort under a JOIN: `passThroughTraits` must answer a NON-singleton
demand with the GATHERED shape (declining leaves the subset empty, and no exchange can produce
`RANDOM(SHARD)`), and `getDeriveMode` returns to `LEFT_FIRST` because `deriveTraits` now derives the
gathered variant rather than passing a partitioned child trait through.

**The broadcast pair.** Both tests used the SHARED-key `makeThreeWayJoin`, where broadcast-bottom is
genuinely the wrong plan: a shared-key bottom join already outputs `HASH(k)`, which the top join consumes
with no exchange, so broadcasting the build buys nothing and costs `build × probeNodes`. Measured
broadcast-bottom 40,003,389 vs all-shuffle 40,001,404 — a 1,985-unit loss that is EXACTLY the tiny build
moved 3× instead of 1×. Under a KEY CHANGE the top join must reshuffle its large input either way, so not
moving the 10M probe at the bottom is a ~10M win and CBO picks the mixed shape unprompted. Switched both
to the existing `makeMixedKeyThreeWayJoin`.

Broadcast is NOT suppressed under top-down: 37 broadcast assertions across `JoinStrategyCBOSelectionTests`,
`MppStrategyObservationTests` and `SplitRuleContractTests` are green, and the 2-way small×large
`testEnforcementPass_broadcastWinnerIsPreserved` still picks broadcast.

Also fixed en route (`5831d805b25`): the `RANDOM(SHARD)` derive gap (step 3 of the removal sequence),
gated on `MPP_ENABLED` + `isBroadcastEligible` via `traitDef.getPlannerContext()` and sharing the split
rule's `buildSideFitsBroadcast` byte cap so both formation paths agree; plus a latent crash where
`OpenSearchConvention.enforce` propagated `buildEnforcer`'s UOE instead of returning null (Calcite calls
`enforce` speculatively from `RelSet.addConverters`, so it aborted whole queries with "RANGE exchange not
yet implemented").

**The sf=1 / sf=10 TPC-H sweep is now unblocked** — it was gated on exactly this.

---

## TPC-H sf=1 SWEEP — first live validation of top-down traits (21/22)

Ran `dev-tools/tpch/per_query_stress.py --sf 1 --runs 1` (fresh cluster per query, the authoritative mode)
with this branch's jars deployed. **21/22 correct.**

Deployment notes worth keeping — the sf=1 cluster dirs were a stale **3.8.0** distro while the branch
builds **3.9.0**, so they had to be rebuilt from `testclusters/integTest-0/distro/3.9.0-INTEG_TEST` with
`data/`, `config/opensearch.yml` and `start-node.sh` preserved. Two traps hit on the way:
- `opensearch.yml` carried `analytics.mpp.shuffle.flight.*` from the Flight-shuffle worktree. Those are
  NOT registered on this branch, so every node would have rejected them at boot. Stripped.
- Rebuilding from a distro RESETS `config/jvm.options` to the stock **1g** heap. The sf=1 profile needs
  `-Xmx8g` + `-XX:MaxDirectMemorySize=2g` (see `setup_cluster.sh`). Missing this produced three bogus
  `CircuitBreakingException` failures (q16/q18/q19) that vanished once the heap was restored — a
  deployment error, NOT a branch regression. Re-verified q18/q19 PASS on the corrected cluster.

### Result vs the last full sf=1 baseline (`per_query_stress_sf1_report_20260703-132318.md`, 20/22)

| change | queries |
|---|---|
| **FIXED** | q11, q21 (both FAIL → PASS) |
| **strategy shift** HASH_SHUFFLE → BROADCAST | q7, q20 |
| **strategy shift** COORDINATOR_CENTRIC → BROADCAST | q4 |
| still failing | q16 only |

q16's failure is a NATIVE-pool `CircuitBreakingException` at 16% heap — the documented orthogonal engine
limit (10k result cap / native allocation), not a scheduler defect, and it is the same failure the
pre-branch baseline table records.

**The hash→broadcast shifts are exactly what songkant reported at sf=10 and what the user identified as an
IMPROVEMENT, now reproduced independently at sf=1.** They follow from the parallelism-aware join cost
(`fa0bb79e5f7`): broadcast moves only the small build while the probe stays sharded, so for a small-dim
join it beats shuffling both sides. Strategy mix is healthy and varied — BROADCAST 13, COORDINATOR_CENTRIC
5, HASH_SHUFFLE 4 — so top-down is NOT collapsing everything to one strategy, which was the original fear
when `setTopDownOpt(true)` suppressed all distributed alternatives.

Net: **+2 fixed, 0 regressions, 1 pre-existing orthogonal failure.** sf=10 remains to be run.
