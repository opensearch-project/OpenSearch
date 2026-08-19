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
