/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.exec.join.CascadeShuffleDAGRewriter;
import org.opensearch.analytics.exec.join.CascadeShufflePlanRewriter;
import org.opensearch.analytics.exec.join.DistributedAggOverJoinRewriter;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.PlanAlternativeSelector;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Locks in the Variant A distributed-aggregation-over-cascade-join rewrite (TPC-H q5/q10 enabler).
 *
 * <p>The q5/q10 shape is {@code Aggregate(SINGLE) → Project? → dimension-Join → Join_bottom(shuffle,
 * shuffle)} (the dimension join keys on a DIFFERENT column than the bottom cascade, so it stayed a
 * coordinator reducer). {@link DistributedAggOverJoinRewriter} must:
 * <ol>
 *   <li>recognise the shape ({@code canPushPartial == true});</li>
 *   <li>push {@code Aggregate(PARTIAL)} onto the cascade TOP WORKER stage;</li>
 *   <li>keep {@code Aggregate(FINAL)} on the coordinator reduce stage, with FINAL aggCalls rebuilt
 *       by {@code FinalAggCallBuilder} (argList rebased to the partial-state column);</li>
 *   <li>rewrite the dimension {@code StageInputScan} in the worker fragment into an
 *       {@code OpenSearchBroadcastScan} and surface the dimension stage as a BROADCAST_BUILD;</li>
 * </ol>
 * plus the NEGATIVE gates: {@code count(distinct)} / {@code percentile} over the join stay
 * coordinator-centric, and a bare {@code Aggregate} directly over the cascade (no dimension join)
 * is out of scope (handled by the plain cascade).
 *
 * <p>Structure-only (mirrors {@link CascadeShuffleProbeTests}): the mock backend has no fragment
 * convertor, so {@code rewriteStructure} is exercised, not {@code rewrite}; row-count parity is
 * validated on the live sf=10 cluster.
 */
public class DistributedAggOverJoinTests extends BasePlannerRulesTests {

    private static final int CLUSTER_DATA_NODES = 3;
    private static final long LARGE = 10_000_000L;

    // ── Positive: the q5/q10 shape distributes ─────────────────────────────────────────────

    public void testDistributedAggOverJoin_pushesPartialOntoWorker_finalOnCoordinator() {
        PlannerContext context = buildMppContext();
        RelNode logical = sumByGroupOverDimJoinOverCascade(context);
        Structure s = rewriteQ5Shape(logical, context);

        // 1. Recognised as distributable.
        assertTrue("q5 shape (sum by g over dim-join over cascade) must be distributable", s.canPush);

        // 2. The cascade TOP WORKER carries Aggregate(PARTIAL).
        Stage topWorker = topWorker(s.structure);
        OpenSearchAggregate partial = findAggregate(topWorker.getFragment());
        assertNotNull("top worker fragment must contain an Aggregate", partial);
        assertEquals("top worker aggregate is PARTIAL", AggregateMode.PARTIAL, partial.getMode());
        assertEquals(
            "top worker stage runs as a WORKER_FRAGMENT (has WorkerTargetResolver)",
            StageExecutionType.WORKER_FRAGMENT,
            topWorker.getExecutionType()
        );
        assertEquals("top worker role SHUFFLE_WORKER", Stage.StageRole.SHUFFLE_WORKER, topWorker.getRole());

        // 3. The coordinator reduce carries Aggregate(FINAL).
        Stage root = s.structure.dag().rootStage();
        OpenSearchAggregate finalAgg = findAggregate(root.getFragment());
        assertNotNull("coordinator fragment must contain an Aggregate", finalAgg);
        assertEquals("coordinator aggregate is FINAL", AggregateMode.FINAL, finalAgg.getMode());

        // 4. The dimension input is BROADCAST in the worker fragment (StageInputScan → BroadcastScan),
        // and the dimension stage is surfaced as a BROADCAST_BUILD.
        List<OpenSearchBroadcastScan> broadcastScans = RelNodeUtils.findNodes(topWorker.getFragment(), OpenSearchBroadcastScan.class);
        assertEquals("exactly one dimension broadcast scan in the worker fragment", 1, broadcastScans.size());
        assertEquals("one dimension build surfaced", 1, s.structure.broadcastBuilds().size());
        DistributedAggOverJoinRewriter.BroadcastBuild build = s.structure.broadcastBuilds().get(0);
        assertEquals("build stage tagged BROADCAST_BUILD", Stage.StageRole.BROADCAST_BUILD, build.buildStage().getRole());
        assertEquals(
            "broadcast scan named-input matches the build's named input id",
            build.namedInputId(),
            broadcastScans.get(0).getNamedInputId()
        );
        assertEquals(
            "broadcast scan references the dimension build stage id",
            build.buildStage().getStageId(),
            broadcastScans.get(0).getBuildStageId()
        );

        // The worker still consumes the bottom-join cascade via StageInputScan leaves (NOT broadcast).
        List<OpenSearchStageInputScan> stageInputs = RelNodeUtils.findNodes(topWorker.getFragment(), OpenSearchStageInputScan.class);
        assertFalse("the worker fragment still has shuffle StageInputScan leaves (cascade producers)", stageInputs.isEmpty());

        // The dimension build stage is NOT one of the cascade shuffle workers.
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = s.structure.cascade().buildLevels();
        assertTrue(
            "dimension build stage must not be a lifted cascade worker",
            levels.stream().noneMatch(l -> l.worker().getStageId() == build.buildStage().getStageId())
        );
    }

    /**
     * REGRESSION (sf=10 q5/q10 "RexInputRef[N] has no matching FieldStorageInfo entry"): every
     * {@link OpenSearchRelNode} in the rewritten worker AND coordinator fragments must report
     * per-column {@link org.opensearch.analytics.spi.FieldStorageInfo} for EVERY output column
     * ({@code getOutputFieldStorage().size() == rowType.getFieldCount()}). The original bug:
     * {@link OpenSearchBroadcastScan#getOutputFieldStorage()} returned an empty list, so the dimension
     * input the rewrite substitutes truncated the storage union and a Project/PARTIAL-agg above the
     * dimension join threw at fragment conversion. The previous tests asserted DAG SHAPE but not
     * storage completeness — this is the assertion that would have caught it.
     */
    public void testDistributedAggOverJoin_everyNodeReportsCompleteFieldStorage() {
        PlannerContext context = buildMppContext();
        RelNode logical = sumByGroupOverDimJoinOverCascade(context);
        Structure s = rewriteQ5Shape(logical, context);
        assertTrue("q5 shape must distribute", s.canPush);

        // Walk the worker fragment AND the coordinator (root) fragment.
        Stage worker = topWorker(s.structure);
        assertStorageComplete("worker fragment", worker.getFragment());
        assertStorageComplete("coordinator fragment", s.structure.dag().rootStage().getFragment());

        // Pin the specific node that regressed: the dimension broadcast scan must declare storage for
        // all its columns, not an empty list.
        OpenSearchBroadcastScan dimScan = RelNodeUtils.findNodes(worker.getFragment(), OpenSearchBroadcastScan.class).get(0);
        assertEquals(
            "broadcast scan must report FieldStorageInfo for every output column",
            dimScan.getRowType().getFieldCount(),
            dimScan.getOutputFieldStorage().size()
        );
    }

    /** Asserts every OpenSearchRelNode in {@code root}'s tree reports one FieldStorageInfo per output
     *  column — the invariant RelNodeUtils.resolvePhysicalDeps / OpenSearchAggregate.getOutputFieldStorage
     *  rely on (a short list throws "RexInputRef[N] has no matching FieldStorageInfo entry"). */
    private static void assertStorageComplete(String where, RelNode root) {
        for (RelNode rel : RelNodeUtils.findNodes(root, RelNode.class)) {
            if (!(rel instanceof OpenSearchRelNode osRel)) {
                continue;
            }
            int cols = rel.getRowType().getFieldCount();
            int storage = osRel.getOutputFieldStorage().size();
            assertEquals(where + ": " + rel.getRelTypeName() + " must report FieldStorageInfo for every output column", cols, storage);
        }
    }

    public void testDistributedAggOverJoin_finalAggCallsRebasedByFinalAggCallBuilder() {
        // COUNT decomposes to SUM(partial_count) on FINAL — a sharp signal that the FINAL aggCalls
        // were rebuilt by FinalAggCallBuilder (not the original SINGLE calls). Use count() by g.
        PlannerContext context = buildMppContext();
        RelNode logical = countByGroupOverDimJoinOverCascade(context);
        Structure s = rewriteQ5Shape(logical, context);
        assertTrue("count-by-g over dim-join over cascade must distribute", s.canPush);

        OpenSearchAggregate partial = findAggregate(topWorker(s.structure).getFragment());
        OpenSearchAggregate finalAgg = findAggregate(s.structure.dag().rootStage().getFragment());

        // PARTIAL keeps COUNT; FINAL swaps to SUM (the engine decomposition COUNT → SUM(partial)).
        assertEquals(
            "PARTIAL keeps COUNT",
            "COUNT",
            partial.getAggCallList().get(0).getAggregation().getName().toUpperCase(java.util.Locale.ROOT)
        );
        SqlAggFunction finalFn = finalAgg.getAggCallList().get(0).getAggregation();
        assertEquals(
            "FINAL re-merges COUNT via SUM (FinalAggCallBuilder swap)",
            "SUM",
            finalFn.getName().toUpperCase(java.util.Locale.ROOT)
        );

        // FINAL's arg references the PARTIAL state column (group key at 0, partial-count at groupCount=1).
        int groupCount = finalAgg.getGroupSet().cardinality();
        assertEquals(
            "FINAL arg rebased to the partial-state column (groupCount + 0)",
            groupCount,
            (int) finalAgg.getAggCallList().get(0).getArgList().get(0)
        );
    }

    public void testDistributedAggOverJoin_sumPreservedThroughSplit() {
        PlannerContext context = buildMppContext();
        RelNode logical = sumByGroupOverDimJoinOverCascade(context);
        Structure s = rewriteQ5Shape(logical, context);
        assertTrue(s.canPush);

        OpenSearchAggregate partial = findAggregate(topWorker(s.structure).getFragment());
        OpenSearchAggregate finalAgg = findAggregate(s.structure.dag().rootStage().getFragment());
        // SUM is its own reducer: PARTIAL and FINAL are both SUM, FINAL arg rebased to the state col.
        assertEquals("PARTIAL SUM", "SUM", partial.getAggCallList().get(0).getAggregation().getName().toUpperCase(java.util.Locale.ROOT));
        assertEquals(
            "FINAL re-merges SUM via SUM",
            "SUM",
            finalAgg.getAggCallList().get(0).getAggregation().getName().toUpperCase(java.util.Locale.ROOT)
        );
        int groupCount = finalAgg.getGroupSet().cardinality();
        assertEquals(
            "FINAL SUM arg rebased to partial-state column",
            groupCount,
            (int) finalAgg.getAggCallList().get(0).getArgList().get(0)
        );
    }

    public void testDistributedAggOverJoin_sortAboveAggregateReplayedOnCoordinator() {
        // q5's real root is Sort(Aggregate(SINGLE)(... dim join ... cascade)). The Sort is a
        // coordinator-side post-aggregate op: it must be REPLAYED above the rebuilt FINAL on the
        // coordinator, NOT pushed onto the worker (sorting per partition then merging is wrong for a
        // global sort). Assert PARTIAL on worker, and FINAL + Sort on the coordinator.
        PlannerContext context = buildMppContext();
        RelNode logical = sortedSumByGroupOverDimJoinOverCascade(context);
        Structure s = rewriteQ5Shape(logical, context);
        assertTrue("sort(sum by g over dim-join over cascade) must distribute", s.canPush);

        OpenSearchAggregate partial = findAggregate(topWorker(s.structure).getFragment());
        assertEquals("worker still PARTIAL", AggregateMode.PARTIAL, partial.getMode());

        RelNode coordFragment = s.structure.dag().rootStage().getFragment();
        // The Sort must be on the coordinator (above FINAL), not on the worker.
        assertFalse(
            "the global Sort must stay on the coordinator (replayed above FINAL)",
            RelNodeUtils.findNodes(coordFragment, org.opensearch.analytics.planner.rel.OpenSearchSort.class).isEmpty()
        );
        assertTrue(
            "no Sort on the worker (sorting per partition then merging would be wrong for a global sort)",
            RelNodeUtils.findNodes(topWorker(s.structure).getFragment(), org.opensearch.analytics.planner.rel.OpenSearchSort.class)
                .isEmpty()
        );
        assertEquals("coordinator still FINAL", AggregateMode.FINAL, findAggregate(coordFragment).getMode());
    }

    // ── Negative: shapes that MUST stay coordinator-centric ─────────────────────────────────

    public void testCountDistinctOverJoin_staysCoordinatorCentric() {
        PlannerContext context = buildMppContext();
        RelNode logical = distinctAggByGroupOverDimJoinOverCascade(context);
        QueryDAG dag = toCboDag(logical, context);
        assertTrue("must still be a cascade (the join distributes)", CascadeShuffleDAGRewriter.isCascade(dag));
        assertFalse(
            "count(distinct x) by g over a join must NOT push a PARTIAL (DISTINCT is not decomposable) — stays coordinator-centric",
            DistributedAggOverJoinRewriter.canPushPartial(dag)
        );
    }

    public void testStateExpandingOverJoin_staysCoordinatorCentric() {
        // A STATE_EXPANDING aggregate (collect / percentile_approx / take / listagg) cannot decompose
        // into additively-merged per-partition partials. canPushPartial delegates the safety decision
        // to OpenSearchAggregateSplitRule.shouldSkipPartialFinalSplit — the SAME gate the coord-centric
        // and M3 shuffle paths use — so assert that gate returns true for a STATE_EXPANDING aggregate.
        // (The mock backend's agg-capability set excludes STATE_EXPANDING funcs, so this exercises the
        // shared gate directly rather than end-to-end through the marking rule.)
        PlannerContext context = buildMppContext();
        RelNode cascadeJoin = runPlanner(fourWayDimTop(context), context);
        OpenSearchAggregate collectAgg = new OpenSearchAggregate(
            cascadeJoin.getCluster(),
            cascadeJoin.getTraitSet(),
            cascadeJoin,
            ImmutableBitSet.of(0),
            null,
            List.of(
                AggregateCall.create(
                    SqlStdOperatorTable.COLLECT,
                    false,
                    List.of(1),
                    -1,
                    cascadeJoin,
                    typeFactory.createMultisetType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1),
                    "items"
                )
            ),
            AggregateMode.SINGLE,
            List.of("mock-parquet"),
            Map.of()
        );
        assertTrue(
            "a STATE_EXPANDING aggregate (collect/percentile/take) must skip the PARTIAL/FINAL split (stays coordinator-centric)",
            org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule.shouldSkipPartialFinalSplit(collectAgg)
        );
    }

    public void testAggregateDirectlyOverCascade_noDimension_outOfScope() {
        // Aggregate directly over the cascade join (q3 shape, no dimension join between). The plain
        // cascade already bounds the gather by distributing the join; this rewriter is scoped to the
        // dimension-join shape and must decline (canPushPartial == false) so q3 keeps its path.
        PlannerContext context = buildMppContext();
        RelNode logical = sumByGroupOverThreeWayCascade(context);
        QueryDAG dag = toCboDag(logical, context);
        assertTrue("q3 shape is still a cascade", CascadeShuffleDAGRewriter.isCascade(dag));
        assertFalse(
            "aggregate directly over the cascade (no dimension join) is out of scope for this rewriter",
            DistributedAggOverJoinRewriter.canPushPartial(dag)
        );
    }

    public void testEmptyGroupAggregateOverJoin_distributes() {
        // `stats sum(x)` with NO `by` (the TPC-H q2/q11 scalar-subquery shape: an empty-group SUM/MIN
        // over partsupp⋈supplier⋈nation). The aggregate OUTPUT is one row, but the JOIN OUTPUT feeding
        // it is huge — an 8M-row join gathers to the coordinator before the SINGLE aggregate runs
        // (ReduceSizeExceeded at sf=10). So empty-group over a large join MUST distribute: PARTIAL on the
        // worker (dimension broadcast in), FINAL on the coordinator, mirroring how
        // OpenSearchAggregateSplitRule already distributes empty-group PARTIAL/FINAL.
        PlannerContext context = buildMppContext();
        RelNode fourWay = fourWayDimTop(context);
        // Empty group → SUM infers a NULLABLE result (no rows → null), unlike the grouped case.
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            fourWay,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "total"
        );
        RelNode logical = LogicalAggregate.create(fourWay, List.of(), ImmutableBitSet.of(), null, List.of(sumCall));
        QueryDAG dag = toCboDag(logical, context);
        assertTrue(
            "empty-group aggregate over a large join must distribute (PARTIAL on worker, FINAL on coord) — "
                + "the join output gather is what OOMs, not the one-row aggregate output",
            DistributedAggOverJoinRewriter.canPushPartial(dag)
        );

        DistributedAggOverJoinRewriter.Structure structure = DistributedAggOverJoinRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );
        OpenSearchAggregate partial = findAggregate(topWorker(structure).getFragment());
        assertNotNull("worker fragment carries an Aggregate", partial);
        assertEquals("worker aggregate is PARTIAL", AggregateMode.PARTIAL, partial.getMode());
        assertTrue("PARTIAL keeps the empty group set", partial.getGroupSet().isEmpty());
        OpenSearchAggregate finalAgg = findAggregate(structure.dag().rootStage().getFragment());
        assertNotNull("coordinator fragment carries an Aggregate", finalAgg);
        assertEquals("coordinator aggregate is FINAL", AggregateMode.FINAL, finalAgg.getMode());
        // Storage-completeness invariant for the empty-group FINAL (possibly wrapped in a cast Project).
        assertStorageComplete("empty-group coordinator fragment", structure.dag().rootStage().getFragment());
    }

    public void testAggregateBetweenJoins_notDistributed() {
        // Join(Aggregate(Join(A,B)), C) — the aggregate sits BETWEEN join levels. Already not a cascade
        // (CascadeShuffleProbeTests covers that); canPushPartial must also decline (the root is a join,
        // not an Aggregate(SINGLE) over the cascade).
        PlannerContext context = buildMppContext();
        RelNode logical = joinOverAggregateOverJoin(context);
        QueryDAG dag = toCboDag(logical, context);
        assertFalse(
            "Join(Aggregate(Join)) must not be a distributable agg-over-cascade shape",
            DistributedAggOverJoinRewriter.canPushPartial(dag)
        );
    }

    // ── Seed: agg over a LARGE×SMALL bottom join (TPC-H q2/q11 shape) ────────────────────────

    /**
     * The q2/q11 shape: {@code Aggregate(SINGLE) by g → dimension-Join → Join_bottom(LARGE fact, SMALL
     * dim)}. Unlike q5 (LARGE×LARGE bottom that CBO already shuffles), CBO keeps this large×small bottom
     * join COORDINATOR-CENTRIC (both inputs are reducers) — even with a corrected join cardinality,
     * broadcasting the small side still gathers the large join output to SINGLETON for the parent
     * dimension join, so coord-centric wins the cost race. WITHOUT the seed the bottom join is not
     * shuffle-shaped, so it is not a cascade and {@code canPushPartial} is false → the 8M fact gathers
     * to the coordinator → ReduceSizeExceeded at scale.
     *
     * <p>{@link CascadeShufflePlanRewriter#seedAggOverJoinBottomShuffle} converts the bottom join's
     * reducer inputs to shuffles when a decomposable aggregate sits above the INNER chain, so the join
     * becomes a liftable cascade level and the proven q5/q10 machinery distributes it: PARTIAL on the
     * worker, FINAL on the coordinator, dimension broadcast in.
     */
    public void testSeed_aggOverLargeSmallBottomJoin_becomesDistributable() {
        // a_idx = LARGE fact (partsupp), b_idx = SMALL build (supplier), d_idx = SMALL dimension (nation).
        // c_idx is part of fourWayDimTop's a⋈b⋈c bottom cascade; keep it SMALL too so the bottom JOIN
        // (a⋈b) is the large×small one CBO keeps coord-centric.
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", 100_000L, "c_idx", 100_000L, "d_idx", 25L);
        PlannerContext context = buildMppContext(rowCounts);
        RelNode logical = sumByGroupOverDimJoinOverCascade(context);

        // Baseline: without the seed, CBO keeps the large×small bottom join coord-centric, so it is not a
        // cascade and cannot push a PARTIAL.
        QueryDAG baselineDag = toCboDag(logical, context);
        assertFalse(
            "without the seed, a large×small bottom join stays coord-centric (not a cascade)",
            DistributedAggOverJoinRewriter.canPushPartial(baselineDag)
        );

        // With the seed: the bottom join is shuffled, so the shape becomes the distributable cascade.
        RelNode cbo = runPlanner(logical, context);
        RelNode seeded = CascadeShufflePlanRewriter.seedAggOverJoinBottomShuffle(cbo, CLUSTER_DATA_NODES, /* minRows */ 1L);
        assertNotSame("seed must rewrite the plan (bottom join → shuffle)", cbo, seeded);
        RelNode extended = CascadeShufflePlanRewriter.rewrite(seeded, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(extended, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        assertTrue("seeded q2/q11 shape must be a cascade", CascadeShuffleDAGRewriter.isCascade(dag));
        assertTrue("seeded q2/q11 shape must be distributable (push PARTIAL)", DistributedAggOverJoinRewriter.canPushPartial(dag));

        DistributedAggOverJoinRewriter.Structure structure = DistributedAggOverJoinRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );
        Stage worker = topWorker(structure);
        OpenSearchAggregate partial = findAggregate(worker.getFragment());
        assertNotNull("worker fragment must carry an Aggregate", partial);
        assertEquals("worker aggregate is PARTIAL", AggregateMode.PARTIAL, partial.getMode());
        OpenSearchAggregate finalAgg = findAggregate(structure.dag().rootStage().getFragment());
        assertNotNull("coordinator fragment must carry an Aggregate", finalAgg);
        assertEquals("coordinator aggregate is FINAL", AggregateMode.FINAL, finalAgg.getMode());
        // Storage-completeness invariant (the RexInputRef[N] gap): every node reports per-column storage.
        assertStorageComplete("seeded worker fragment", worker.getFragment());
        assertStorageComplete("seeded coordinator fragment", structure.dag().rootStage().getFragment());
    }

    /**
     * The seed's row floor must keep a SMALL×SMALL bottom join coordinator-centric: when nothing exceeds
     * the floor, gathering the join is bounded and there is no OOM motivation to distribute. The seed
     * must be a no-op (return the same plan) so CBO's coord-centric choice stands.
     */
    public void testSeed_smallBottomJoinBelowFloor_isNoOp() {
        Map<String, Long> rowCounts = Map.of("a_idx", 1_000L, "b_idx", 1_000L, "c_idx", 1_000L, "d_idx", 25L);
        PlannerContext context = buildMppContext(rowCounts);
        RelNode logical = sumByGroupOverDimJoinOverCascade(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode seeded = CascadeShufflePlanRewriter.seedAggOverJoinBottomShuffle(cbo, CLUSTER_DATA_NODES, /* minRows */ 1_000_000L);
        assertSame("seed must be a no-op when the bottom join is below the row floor", cbo, seeded);
    }

    /**
     * Path-safety (codex review finding #1): a non-INNER (LEFT) dimension join ON THE PATH between the
     * aggregate and the bottom INNER join must NOT be seeded. Seeding only validated the bottom join
     * itself, not the path — so it would shuffle the bottom INNER join under a LEFT join, which
     * canPushPartial then rejects, leaving an orphaned shuffle the generic dispatch could mis-lift. The
     * seed must be a no-op (the whole shape stays coord-centric).
     */
    public void testSeed_leftJoinOnPathAboveBottomJoin_isNoOp() {
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", 100_000L, "c_idx", 100_000L, "d_idx", 25L);
        PlannerContext context = buildMppContext(rowCounts);
        // Aggregate over a LEFT top dimension join over the a⋈b⋈c INNER cascade.
        RelNode fourWayLeftTop = fourWayDimTopWithLeftDimJoin(context);
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            fourWayLeftTop,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "total"
        );
        RelNode logical = LogicalAggregate.create(fourWayLeftTop, List.of(), ImmutableBitSet.of(0), null, List.of(sumCall));
        RelNode cbo = runPlanner(logical, context);
        RelNode seeded = CascadeShufflePlanRewriter.seedAggOverJoinBottomShuffle(cbo, CLUSTER_DATA_NODES, /* minRows */ 1L);
        assertSame("seed must be a no-op when a non-INNER join sits on the path above the bottom join", cbo, seeded);
    }

    // ── Harness ─────────────────────────────────────────────────────────────────────────────

    private record Structure(boolean canPush, DistributedAggOverJoinRewriter.Structure structure) {
    }

    /** Runs CBO → cascade plan rewrite → DAGBuilder → fork/select, then asserts canPushPartial and
     *  runs the structural rewrite. */
    private Structure rewriteQ5Shape(RelNode logical, PlannerContext context) {
        QueryDAG dag = toCboDag(logical, context);
        boolean canPush = DistributedAggOverJoinRewriter.canPushPartial(dag);
        if (!canPush) {
            return new Structure(false, null);
        }
        assertTrue("the q5 shape must also be a cascade", CascadeShuffleDAGRewriter.isCascade(dag));
        DistributedAggOverJoinRewriter.Structure structure = DistributedAggOverJoinRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );
        return new Structure(true, structure);
    }

    private QueryDAG toCboDag(RelNode logical, PlannerContext context) {
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        // Mirror DefaultPlanExecutor's plan pipeline EXCEPT convertAll (mock backend has no convertor).
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);
        return dag;
    }

    private static Stage topWorker(DistributedAggOverJoinRewriter.Structure structure) {
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.cascade().buildLevels();
        return levels.get(levels.size() - 1).worker();
    }

    private static OpenSearchAggregate findAggregate(RelNode root) {
        List<OpenSearchAggregate> aggs = RelNodeUtils.findNodes(root, OpenSearchAggregate.class);
        return aggs.isEmpty() ? null : aggs.get(0);
    }

    /** sum(size) by status, over Join_dim( Join_bottom(a⋈b via cascade ⋈ c), reducer→d ). */
    private RelNode sumByGroupOverDimJoinOverCascade(PlannerContext context) {
        RelNode fourWay = fourWayDimTop(context);
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1), // SUM over col1 (size)
            -1,
            fourWay,
            typeFactory.createSqlType(SqlTypeName.INTEGER), // SUM(INTEGER) infers INTEGER NOT NULL in the mock
            "total"
        );
        // GROUP BY col0 (status) — the group key is available in the dim-join output.
        return LogicalAggregate.create(fourWay, List.of(), ImmutableBitSet.of(0), null, List.of(sumCall));
    }

    /** Sort(sum(size) by status descending, ...) — models q5's `... | stats sum(...) by n_name | sort
     *  - revenue`. The Sort is a coordinator post-agg op that must be replayed above FINAL. */
    private RelNode sortedSumByGroupOverDimJoinOverCascade(PlannerContext context) {
        RelNode agg = sumByGroupOverDimJoinOverCascade(context);
        // Sort by the aggregate output column (index 1 = "total") descending.
        org.apache.calcite.rel.RelCollation collation = org.apache.calcite.rel.RelCollations.of(
            new org.apache.calcite.rel.RelFieldCollation(1, org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING)
        );
        return org.apache.calcite.rel.logical.LogicalSort.create(agg, collation, null, null);
    }

    private RelNode countByGroupOverDimJoinOverCascade(PlannerContext context) {
        RelNode fourWay = fourWayDimTop(context);
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            fourWay,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        return LogicalAggregate.create(fourWay, List.of(), ImmutableBitSet.of(0), null, List.of(countCall));
    }

    private RelNode distinctAggByGroupOverDimJoinOverCascade(PlannerContext context) {
        RelNode fourWay = fourWayDimTop(context);
        // MULTI-ARG count(distinct a, b): OpenSearchDistinctCountRule rewrites only single-arg
        // count(distinct) → approx_count_distinct (which IS distributable); a multi-arg residual
        // distinct stays DISTINCT, so shouldSkipPartialFinalSplit's isDistinct() gate fires and the
        // aggregate must stay coordinator-centric.
        AggregateCall distinctCount = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            true, // DISTINCT
            List.of(0, 1),
            -1,
            fourWay,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "dc"
        );
        return LogicalAggregate.create(fourWay, List.of(), ImmutableBitSet.of(2), null, List.of(distinctCount));
    }

    private RelNode sumByGroupOverThreeWayCascade(PlannerContext context) {
        RelNode join = threeWayCascade(context);
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            join,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "total"
        );
        return LogicalAggregate.create(join, List.of(), ImmutableBitSet.of(0), null, List.of(sumCall));
    }

    // ── Join builders (mirror CascadeShuffleProbeTests shapes) ──────────────────────────────

    /** Join(Join(Join(a,b) on col0, c) on col0, d) on col1 — the top dimension join keys on a
     *  DIFFERENT column (col1=size) than the bottom cascade (col0=status). Models q5's left-deep
     *  shape: the dimension (d) stays a coordinator reducer, the a⋈b⋈c bottom join cascades. */
    private RelNode fourWayDimTop(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelNode dScan = stubScan(mockTable("d_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        int aCols = aScan.getRowType().getFieldCount();
        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);

        int abCols = ab.getRowType().getFieldCount();
        RexNode abcCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, abCols)
        );
        RelNode abc = LogicalJoin.create(ab, cScan, List.of(), abcCond, Set.of(), JoinRelType.INNER);

        int abcCols = abc.getRowType().getFieldCount();
        RexNode abcdCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, abcCols + 1)
        );
        return LogicalJoin.create(abc, dScan, List.of(), abcdCond, Set.of(), JoinRelType.INNER);
    }

    /** Like {@link #fourWayDimTop} but the TOP dimension join (over d) is LEFT OUTER. The a⋈b⋈c bottom
     *  cascade is still all-INNER; the seed must refuse because a non-INNER join sits ON THE PATH between
     *  the aggregate and the bottom join (running the lifted bottom join per-partition under a LEFT join
     *  is unsafe). */
    private RelNode fourWayDimTopWithLeftDimJoin(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelNode dScan = stubScan(mockTable("d_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        int aCols = aScan.getRowType().getFieldCount();
        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);

        int abCols = ab.getRowType().getFieldCount();
        RexNode abcCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, abCols)
        );
        RelNode abc = LogicalJoin.create(ab, cScan, List.of(), abcCond, Set.of(), JoinRelType.INNER);

        int abcCols = abc.getRowType().getFieldCount();
        RexNode abcdCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, abcCols + 1)
        );
        return LogicalJoin.create(abc, dScan, List.of(), abcdCond, Set.of(), JoinRelType.LEFT);
    }

    /** Join(Join(a,b) on col0, c) on col0 — three-way cascade, no dimension join. */
    private RelNode threeWayCascade(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        int aCols = aScan.getRowType().getFieldCount();
        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);
        int abCols = ab.getRowType().getFieldCount();
        RexNode abcCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, abCols)
        );
        return LogicalJoin.create(ab, cScan, List.of(), abcCond, Set.of(), JoinRelType.INNER);
    }

    /** Join(Aggregate(Join(a,b) by col0, count), c) — aggregate BETWEEN join levels. */
    private RelNode joinOverAggregateOverJoin(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        int aCols = aScan.getRowType().getFieldCount();
        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            ab,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        RelNode agg = LogicalAggregate.create(ab, List.of(), ImmutableBitSet.of(0), null, List.of(countCall));
        int aggCols = agg.getRowType().getFieldCount();
        RexNode aggcCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aggCols)
        );
        return LogicalJoin.create(agg, cScan, List.of(), aggcCond, Set.of(), JoinRelType.INNER);
    }

    private static List<String> nodeIds(int partitionCount) {
        List<String> ids = new ArrayList<>(partitionCount);
        for (int p = 0; p < partitionCount; p++) {
            ids.add("node-" + (p % CLUSTER_DATA_NODES));
        }
        return ids;
    }

    private PlannerContext buildMppContext() {
        Map<String, Long> rowCounts = new HashMap<>();
        for (String t : List.of("a_idx", "b_idx", "c_idx", "d_idx")) {
            rowCounts.put(t, LARGE);
        }
        return buildMppContext(rowCounts);
    }

    /** Context with explicit per-index row counts — lets a test pin a LARGE×SMALL bottom join (q2/q11). */
    private PlannerContext buildMppContext(Map<String, Long> rowCounts) {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3, "d_idx", 3);
        ClusterState state = mockClusterStateWithDataNodes(shardCounts);
        Settings settings = Settings.builder().put("analytics.mpp.enabled", true).build();
        ToLongFunction<String> rowCountLookup = name -> rowCounts.getOrDefault(name, PlannerContext.UNKNOWN_ROW_COUNT);
        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = FieldStorageResolver::new;
        AnalyticsSearchBackendPlugin shuffleAware = new ShuffleAwareDataFusionBackend(CLUSTER_DATA_NODES);
        CapabilityRegistry registry = new CapabilityRegistry(List.of(shuffleAware, LUCENE), fieldStorageFactory);
        return new PlannerContext(registry, state, settings, rowCountLookup, /* profiling */ false);
    }

    private static ClusterState mockClusterStateWithDataNodes(Map<String, Integer> shardCounts) {
        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(state.metadata()).thenReturn(metadata);

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(state.nodes()).thenReturn(nodes);
        Map<String, DiscoveryNode> dataNodes = new HashMap<>();
        for (int i = 0; i < CLUSTER_DATA_NODES; i++) {
            dataNodes.put("node-" + i, mock(DiscoveryNode.class));
        }
        when(nodes.getDataNodes()).thenReturn(dataNodes);

        for (Map.Entry<String, Integer> entry : shardCounts.entrySet()) {
            String indexName = entry.getKey();
            int shardCount = entry.getValue();
            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(indexMetadata.getIndex()).thenReturn(new Index(indexName, indexName + "-uuid"));
            when(indexMetadata.getNumberOfShards()).thenReturn(shardCount);
            MappingMetadata mappingMetadata = mock(MappingMetadata.class);
            when(mappingMetadata.sourceAsMap()).thenReturn(Map.of("properties", intFields()));
            when(indexMetadata.mapping()).thenReturn(mappingMetadata);
            when(indexMetadata.getSettings()).thenReturn(
                Settings.builder()
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene")
                    .build()
            );
            when(metadata.index(indexName)).thenReturn(indexMetadata);
        }
        return state;
    }

    private static class ShuffleAwareDataFusionBackend extends MockDataFusionBackend {
        private final int parallelism;

        ShuffleAwareDataFusionBackend(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public int defaultShuffleParallelism(ClusterState state) {
            return parallelism;
        }
    }
}
