# Top-down Volcano trait propagation for MPP distribution

Working plan for `feature/mpp-top-down-traits` (worktree, based on `main` @ `af1e9151c63`).

## Why

Today distribution is formed in TWO places, and the split is a known design compromise
(CLAUDE.md: "CBO not shuffle-cascade-native is OUR design choice"):

1. **Volcano CBO (bottom-up)** emits competing exchange alternatives via the split rules, but its cost
   gate knows only 3 fixed localities, so it gathers every join to `COORDINATOR+SINGLETON`.
2. **`DistributionEnforcementPass` (post-CBO)** then walks that gathered output and *re-places* every
   exchange by the `satisfies()` algebra — the Spark `EnsureRequirements` / Presto `AddExchanges` shape.

Consequences we want to remove:
- CBO never *costs* the distributed plan it ultimately runs. The cost model ranks
  broadcast-vs-shuffle-vs-coord for a shape the enforcement pass may then rewrite, so cost and
  final plan can disagree.
- The enforcement pass must re-derive, by hand, facts Volcano already knows (which is why it tracks
  distribution separately in `Visited` and forces exchanges via `buildReducer`/`buildShuffleExchange`
  rather than the satisfies-gated `buildEnforcer` — see the `[[enforcement-pass-stale-trait]]` note).
- Every new shape needs a gate in the pass (the "only distribute what dispatch can run" list).

Top-down optimization (`setTopDownOpt(true)` + Calcite's `PhysicalNode` `passThrough`/`derive` hooks)
lets each operator answer "given this required distribution, what do I demand of my inputs?" DURING
costing, so exchange placement becomes emergent and priced.

## Scope decision: ADDITIVE, not rip-and-replace

Verified against Songkan Tang's reference branch
(`songkant-aws/feature/mpp-spillable-join-pipelined-shuffle`, commit `a7a66945ed3`): his
`DistributionEnforcementPass` is **650 lines vs our 672** — i.e. essentially untouched. Top-down does
NOT retire the pass; it keeps it for the binary worker-tier boundaries the shuffle transport requires
(two named inputs per worker). We follow the same containment: get traits priced correctly in CBO,
leave the execution-shape guarantees where they are.

Only two rules are deleted in the reference: `OpenSearchDistributionDeriveRule` (138 lines — its job
moves into `deriveTraits`) and `OpenSearchSortSplitRule` (66 lines — becomes a `passThroughTraits`
that demands SINGLETON).

## Current state on main (measured)

- `OpenSearchRelNode` is a plain interface (`OpenSearchRelNode.java:33`) — does NOT extend `PhysicalNode`.
- `OpenSearchConvention` (65 lines) implements `satisfies` / `canConvertConvention` /
  `useAbstractConvertersForConversion` but has **no `enforce()`**; exchange insertion currently rides
  `AbstractConverter.ExpandConversionRule` (`PlannerImpl.java:547`).
- `DistributionAware` implementors: `OpenSearchJoin`, `OpenSearchAggregate`, `OpenSearchProject`,
  `OpenSearchFilter` (4 of ~14 rel nodes).
- Volcano rules registered at `PlannerImpl.java:540-547`; root trait requested at `:565-570`.

## Steps

1. **`OpenSearchRelNode extends PhysicalNode`** with `passThroughTraits`/`deriveTraits` defaulting to
   `null` ("no alternative" — Calcite's contract). This is what keeps the other ~10 rel nodes safe
   without touching them.
2. **`OpenSearchConvention.enforce(input, required)`** → delegate to
   `OpenSearchDistributionTraitDef.buildEnforcer`. Replaces `ExpandConversionRule`.
3. **Per-node hooks**, in dependency order:
   - `OpenSearchProject` / `OpenSearchFilter` — row-transparent: pass the required distribution
     straight down, derive the child's back up.
   - `OpenSearchSort` — `passThroughTraits` demanding SINGLETON (replaces `OpenSearchSortSplitRule`).
     MUST keep the `perPartition` carve-out: a shard-local top-N rides its child (see
     `[[mpp-topn-sort-regression]]`).
   - `OpenSearchAggregate` — PARTIAL rides the child; FINAL demands SINGLETON; decomposable SINGLE
     splits.
   - `OpenSearchJoin` — the substantive one: SINGLETON pass-through, plus derive for
     single-shard-colocated / coordinator / WORKER+HASH-on-matching-keys / broadcast-probe.
     `getDeriveMode() = DeriveMode.BOTH`.
4. **Delete** `OpenSearchDistributionDeriveRule`, `OpenSearchSortSplitRule`; drop
   `ExpandConversionRule`; `setTopDownOpt(true)`.
5. **Rule registration order matters under top-down** — Volcano executes matches in stack order, so
   register the distributed join implementations BEFORE the coordinator fallback, or the fallback
   establishes an upper bound that prunes the cheaper MPP alternative.

## Risks / invariants that must not regress

- **`FieldStorageInfo` alignment** — every `OpenSearch*` leaf feeding a Project/Aggregate must report
  `getOutputFieldStorage().size() == rowType.getFieldCount()`, else "RexInputRef[N] has no matching
  FieldStorageInfo entry" at convert. Assert size==fieldCount in the new tests, not just shape.
- **`copyToCluster` completeness** — any new node needs a `RelNodeUtils.copyToCluster` branch.
- **Cost-model coupling** — `OpenSearchExchangeReducer.computeSelfCost` adds `WIDTH_COST·cols` as an
  ADDITIVE tie-breaker, never `rows·width`. Top-down changes *when* costs are compared, so re-run
  `JoinStrategyCBOSelectionTests` (esp. `testModestAsymmetryStillPicksBroadcast`) and `BroadcastJoinIT`.
- **Infinite re-fire** — split rules gate `matches()` on the join's OWN distribution trait, never
  `instanceof` on inputs (they are `RelSubset`s). Top-down adds trait-request churn, so this gets
  *more* load-bearing.
- **Plan-shape goldens** — `*PlanShapeTests` (JVM) and `qa/.../planshape/**.plan.yaml` (REST IT) both
  pin exchange placement. Expect churn; regenerate the ITs with `-Dplan.generate=write` against a real
  cluster, never by hand.

## Validation ladder

1. `:sandbox:plugins:analytics-engine:test` — unit + plan-shape.
2. `:sandbox:plugins:analytics-engine:precommit` — spotless/forbidden/javadoc.
3. `:sandbox:qa:analytics-engine-rest:integTest` + `integTestPlanShape`.
4. TPC-H sf=1 fresh-cluster-per-query sweep (`dev-tools/tpch-sf1/per_query_stress_sf1.py`) — the
   authoritative correctness gate; 19/22 is the pre-change baseline.
5. sf=10 warm P50 for the latency claim (the whole point is that CBO now prices what it runs).

## Reference, not source

Songkan's branch is a REFERENCE for the shape of the hooks. It predates the `main` merge by 93 commits
(239-conflict merge-tree), so we implement against current `main` rather than rebasing. Notably his
branch does NOT contain our `perPartition` top-N fix, so step 3's Sort carve-out has no counterpart
there and needs its own test.
