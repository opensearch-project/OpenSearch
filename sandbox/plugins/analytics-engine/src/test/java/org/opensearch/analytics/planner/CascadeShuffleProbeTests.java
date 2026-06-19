/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.exec.join.CascadeShuffleDAGRewriter;
import org.opensearch.analytics.exec.join.CascadeShufflePlanRewriter;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.PlanAlternativeSelector;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.dag.WorkerTargetResolver;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Locks in the cascaded hash-shuffle plan shape for a 3-way INNER equi-join
 * {@code Join(Join(A,B), C)}:
 * <ol>
 *   <li>{@link CascadeShufflePlanRewriter} turns the outer join's coordinator-gathered inputs into
 *       hash-shuffle inputs (post-CBO), so {@code DAGBuilder} cuts a nested-shuffle DAG.</li>
 *   <li>{@link CascadeShuffleDAGRewriter#rewriteStructure} lifts each join level into its own worker
 *       tier — the top join into a NEW worker, the inner join into a consume-and-produce worker.</li>
 * </ol>
 * Validates the structure (worker count, execution types, producer wiring, coordinator gather)
 * without the convert pipeline (the mock backend has no fragment convertor; conversion + actual
 * row-count parity are exercised on the live cluster).
 */
public class CascadeShuffleProbeTests extends BasePlannerRulesTests {

    private static final int CLUSTER_DATA_NODES = 3;
    private static final long LARGE = 10_000_000L;

    public void testCascadeShuffleShape_threeWayJoin() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeThreeWayJoin(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);

        // Mirror DefaultPlanExecutor's plan pipeline EXCEPT convertAll — the mock backend has no
        // fragment convertor. forkAll + selectAll populate plan alternatives' backendId, which is all
        // the structural rewrite reads; convertAll (real backend) is exercised by the live cluster.
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        // Now drive the recursive DAG rewriter (worker-tier surgery) and assert the cascade shape.
        assertTrue("expected a cascade DAG (>1 join-shuffle stage)", CascadeShuffleDAGRewriter.isCascade(dag));

        CascadeShuffleDAGRewriter.Structure structure = CascadeShuffleDAGRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );

        // Structural assertions: 2 join levels → 2 worker tiers; both run as WORKER_FRAGMENT.
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.buildLevels();
        assertEquals("two join levels → two worker tiers", 2, levels.size());
        for (CascadeShuffleDAGRewriter.WorkerLevel level : levels) {
            Stage worker = level.worker();
            assertEquals(
                "worker stage must be WORKER_FRAGMENT (has WorkerTargetResolver)",
                StageExecutionType.WORKER_FRAGMENT,
                worker.getExecutionType()
            );
            assertTrue("worker resolver is WorkerTargetResolver", worker.getTargetResolver() instanceof WorkerTargetResolver);
            assertEquals("worker role SHUFFLE_WORKER", Stage.StageRole.SHUFFLE_WORKER, worker.getRole());
        }
        // The deepest worker (level 0) consumes two leaf shard scans; the top worker (level 1)
        // consumes the deepest worker + a leaf scan.
        CascadeShuffleDAGRewriter.WorkerLevel deepest = levels.get(0);
        CascadeShuffleDAGRewriter.WorkerLevel top = levels.get(1);
        assertEquals(
            "deepest worker left producer is a shard fragment",
            StageExecutionType.SHARD_FRAGMENT,
            deepest.leftProducer().getExecutionType()
        );
        assertEquals(
            "top worker's one producer is the deepest worker (consume-and-produce)",
            deepest.worker().getStageId(),
            // exactly one of top's producers is the intermediate worker
            (top.leftProducer().getStageId() == deepest.worker().getStageId() ? top.leftProducer() : top.rightProducer()).getStageId()
        );
        // Root reduce stage gathers the top worker via its single child; the top worker's exchange
        // is SINGLETON (gathers to coordinator). The root stage itself is the coordinator terminal
        // (null exchange info — nothing consumes it).
        Stage rootStage = structure.dag().rootStage();
        assertEquals("root reduce has exactly one child = top worker", 1, rootStage.getChildStages().size());
        Stage topWorkerInDag = rootStage.getChildStages().get(0);
        assertEquals("top worker id matches", top.worker().getStageId(), topWorkerInDag.getStageId());
        assertEquals(
            "top worker gathers SINGLETON to coordinator",
            RelDistribution.Type.SINGLETON,
            topWorkerInDag.getExchangeInfo().distributionType()
        );
    }

    /**
     * Positive: {@code Aggregate(Join(Join(A,B), C))} — an Aggregate sits ABOVE the topmost join
     * (the TPC-H q3 shape: `… join … join … | stats … by …`). The aggregate runs on the COORDINATOR
     * after the worker join (only the join is lifted into a worker), so it must NOT block cascade
     * detection. Regression guard: an earlier version checked the whole stage-fragment ROOT for
     * partition-preservation and wrongly rejected this — q3 then mis-routed to the single-level
     * HashShuffleDispatch and failed "could not locate consumer stage". The agg-ABOVE-join case is
     * safe; only agg/sort BETWEEN join levels is not (covered by the negative tests).
     */
    public void testCascadeDetected_aggregateAboveTopJoin() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeAggregateOverThreeWayJoin(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);

        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        assertTrue(
            "Aggregate ABOVE the top join (q3 shape) must still be detected as a cascade — the agg runs"
                + " on the coordinator after the worker join, not per-partition",
            CascadeShuffleDAGRewriter.isCascade(dag)
        );
        // And it has the two worker tiers (inner + outer join).
        CascadeShuffleDAGRewriter.Structure structure = CascadeShuffleDAGRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );
        assertEquals("two join levels → two worker tiers", 2, structure.buildLevels().size());
    }

    /**
     * Negative: {@code Join(Aggregate(Join(A,B)), C)} — an Aggregate sits BETWEEN the two join
     * levels. Cascading it would run the GROUP BY once per hash partition (partitioned by the inner
     * join key, not the group key) → per-partition partial groups, silently wrong. The cascade-safe
     * gate must keep this shape OUT of the cascade (it stays on its CBO-chosen coord-centric path).
     */
    public void testCascadeRejected_aggregateBetweenJoins() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeJoinOverAggregateOverJoin(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);

        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        // The outer join must NOT be cascaded — the Aggregate between the levels makes per-partition
        // execution unsafe. isCascade requires >1 join-over-shuffles stage with a partition-preserving
        // root-to-join chain; the aggregate breaks that chain, so at most the inner join shuffles.
        assertFalse(
            "Join(Aggregate(Join)) must NOT be detected as a cascade — aggregate between joins is not partition-safe",
            CascadeShuffleDAGRewriter.isCascade(dag)
        );
    }

    /**
     * Negative (depth 4): {@code Join(Join(Aggregate(Join(A,B)), D), E)} — the Aggregate sits two
     * join levels down. An earlier guard returned true for ANY nested join, so the OUTERMOST join's
     * reducer input (the MIDDLE join) was accepted as "an OpenSearchJoin" even though that middle
     * join was never converted (its Aggregate side blocked it). That left an unlifted reduce stage
     * the dispatcher would enrich as a producer that never ships partitions (hang). The fixed guard
     * accepts a nested join only when it is itself an INNER join over two shuffles, so this whole
     * shape must stay OUT of the cascade. (codex R4 blocker #1)
     */
    public void testCascadeRejected_aggregateTwoLevelsDown() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "d_idx", 3, "e_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "d_idx", LARGE, "e_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeJoinOverJoinOverAggregateOverJoin(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);

        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        // No cascade: the aggregate between the lowest two join levels breaks the partition-preserving
        // chain, so the middle join is never a validated cascade level and the outer join must not lift.
        assertFalse(
            "Join(Join(Aggregate(Join))) must NOT cascade — aggregate breaks the chain at the middle join",
            CascadeShuffleDAGRewriter.isCascade(dag)
        );
    }

    /**
     * The TPC-H q5/q10 shape: a join-over-two-shuffles ({@code Join_bottom}) NESTED in the root
     * fragment UNDER a coordinator dimension join that keys on a DIFFERENT column (so the dimension
     * stayed a reducer, not a shuffle). Models q5's left-deep
     * {@code ((((cust⋈ord)⋈lineitem)⋈supplier)⋈nation)⋈region} reduced to one nesting level:
     * {@code Join_dim( Join_bottom(A⋈B⋈… via shuffle), reducer→D )}.
     *
     * <p>This is the case the {@code findNodes(...).size() != 2} guard wrongly rejected (the root
     * fragment has THREE stage-inputs: stage2 + stage3 feeding {@code Join_bottom}'s shuffles, plus
     * stage4 feeding the dimension reducer). The fix must:
     * <ul>
     *   <li>detect the cascade ({@code isCascade == true}) — recognize the nested join-over-two-
     *       shuffles even with extra reducer-fed stage-inputs;</li>
     *   <li>lift the NESTED {@code Join_bottom} (not the topmost {@code Join_dim}, whose right input
     *       is a reducer) into a worker tier;</li>
     *   <li>KEEP the dimension stage (stage4) as a coordinator-reduce child of the rebuilt root — NOT
     *       orphan it — so the dimension join runs on the coordinator over the worker's SINGLETON
     *       output.</li>
     * </ul>
     */
    public void testCascadeExtend_dimensionJoinAboveNestedShuffleJoin() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3, "d_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE, "d_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeFourWayDimTop(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        // The root fragment has 3 stage-inputs (stage2, stage3 under Join_bottom; stage4 under the
        // dimension reducer) — the old size()!=2 guard rejected this. The fix must still detect it.
        assertTrue(
            "a join-over-two-shuffles nested under a coordinator dimension join must be detected as a cascade",
            CascadeShuffleDAGRewriter.isCascade(dag)
        );

        // Capture the root stage's pre-rewrite dimension child stage ids (SINGLETON-reduce children of
        // the root that feed the dimension join, NOT the two shuffle children of Join_bottom).
        Stage preRoot = dag.rootStage();
        List<Integer> dimChildIdsBefore = preRoot.getChildStages()
            .stream()
            .filter(s -> s.getExchangeInfo() != null && s.getExchangeInfo().distributionType() == RelDistribution.Type.SINGLETON)
            .map(Stage::getStageId)
            .toList();
        assertEquals("expected exactly one reducer-fed dimension child stage on the root", 1, dimChildIdsBefore.size());
        int dimStageId = dimChildIdsBefore.get(0);

        CascadeShuffleDAGRewriter.Structure structure = CascadeShuffleDAGRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );

        // Two join levels lifted into workers: the inner A⋈B join (stage2) and Join_bottom.
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.buildLevels();
        assertEquals("two lifted join levels → two worker tiers", 2, levels.size());
        for (CascadeShuffleDAGRewriter.WorkerLevel level : levels) {
            assertEquals(
                "lifted join level must be a WORKER_FRAGMENT",
                StageExecutionType.WORKER_FRAGMENT,
                level.worker().getExecutionType()
            );
            assertEquals("lifted join level role SHUFFLE_WORKER", Stage.StageRole.SHUFFLE_WORKER, level.worker().getRole());
        }

        // The rebuilt root must keep the dimension stage as a coordinator-reduce child AND gain the
        // top worker. It must NOT be just [topWorker] (that would orphan the dimension stage).
        Stage newRoot = structure.dag().rootStage();
        CascadeShuffleDAGRewriter.WorkerLevel top = levels.get(1);
        assertEquals(
            "rebuilt root has two children: the top worker + the (kept) dimension reduce stage",
            2,
            newRoot.getChildStages().size()
        );
        assertTrue(
            "the lifted top worker must be a child of the rebuilt root",
            newRoot.getChildStages().stream().anyMatch(s -> s.getStageId() == top.worker().getStageId())
        );
        Stage keptDim = newRoot.getChildStages().stream().filter(s -> s.getStageId() == dimStageId).findFirst().orElse(null);
        assertTrue("the reducer-fed dimension stage must be KEPT as a root child, not orphaned", keptDim != null);
        assertEquals(
            "the kept dimension stage stays a coordinator-reduce (SINGLETON gather), not lifted",
            RelDistribution.Type.SINGLETON,
            keptDim.getExchangeInfo().distributionType()
        );
        // And the dimension stage is NOT one of the lifted workers (it stays coordinator-side).
        assertTrue(
            "the dimension stage must NOT be a lifted worker",
            levels.stream().noneMatch(l -> l.worker().getStageId() == dimStageId)
        );
    }

    /**
     * Like {@link #testCascadeExtend_dimensionJoinAboveNestedShuffleJoin} but with TWO stacked
     * coordinator dimension joins above {@code Join_bottom} (the real q5 stacks supplier/nation/
     * region — three). Exercises {@code isCoordinatorPathTo} walking through MULTIPLE coordinator
     * joins on the root→liftable-join path, and {@code rebuild} keeping BOTH dimension reduce stages
     * as coordinator-side children of the rebuilt root. Only {@code Join_bottom} (+ its lower cascade
     * level) is lifted into workers.
     */
    public void testCascadeExtend_twoStackedDimensionJoins() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3, "d_idx", 3, "e_idx", 3);
        Map<String, Long> rowCounts = new HashMap<>();
        for (String t : List.of("a_idx", "b_idx", "c_idx", "d_idx", "e_idx")) {
            rowCounts.put(t, LARGE);
        }
        PlannerContext context = buildMppContext(shardCounts, rowCounts);
        RelNode logical = makeFiveWayTwoDimsTop(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        assertTrue(
            "nested join-over-two-shuffles under TWO stacked dimension joins must cascade",
            CascadeShuffleDAGRewriter.isCascade(dag)
        );

        List<Integer> dimChildIds = dag.rootStage()
            .getChildStages()
            .stream()
            .filter(s -> s.getExchangeInfo() != null && s.getExchangeInfo().distributionType() == RelDistribution.Type.SINGLETON)
            .map(Stage::getStageId)
            .toList();
        assertEquals("two reducer-fed dimension children on the root", 2, dimChildIds.size());

        CascadeShuffleDAGRewriter.Structure structure = CascadeShuffleDAGRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.buildLevels();
        assertEquals("only the bottom cascade lifts → two worker tiers", 2, levels.size());

        Stage newRoot = structure.dag().rootStage();
        // 1 top worker + 2 kept dimension stages = 3 children, none orphaned.
        assertEquals("rebuilt root keeps both dimension stages + the top worker", 3, newRoot.getChildStages().size());
        for (int dimId : dimChildIds) {
            Stage kept = newRoot.getChildStages().stream().filter(s -> s.getStageId() == dimId).findFirst().orElse(null);
            assertTrue("dimension stage " + dimId + " must be kept, not orphaned", kept != null);
            assertEquals(
                "kept dimension stage " + dimId + " stays SINGLETON coordinator-reduce",
                RelDistribution.Type.SINGLETON,
                kept.getExchangeInfo().distributionType()
            );
            assertTrue(
                "dimension stage " + dimId + " must NOT be a lifted worker",
                levels.stream().noneMatch(l -> l.worker().getStageId() == dimId)
            );
        }
    }

    private RelNode makeFiveWayTwoDimsTop(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelNode dScan = stubScan(mockTable("d_idx", "status", "size"));
        RelNode eScan = stubScan(mockTable("e_idx", "status", "size"));
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
        RelNode abcd = LogicalJoin.create(abc, dScan, List.of(), abcdCond, Set.of(), JoinRelType.INNER);
        int abcdCols = abcd.getRowType().getFieldCount();
        RexNode abcdeCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, abcdCols + 1)
        );
        return LogicalJoin.create(abcd, eScan, List.of(), abcdeCond, Set.of(), JoinRelType.INNER);
    }

    /**
     * The faithful TPC-H q10 root shape: {@code Aggregate(Join_dim(Join_bottom(shuffle,shuffle),
     * reducer→dim))} — an Aggregate sits ABOVE the dimension join, which sits above the nested
     * {@code Join_bottom}. Matches the REAL captured q10 DAG (verified from the sf=10 cluster log:
     * {@code Project(Sort(Sort(Sort(Aggregate(Project(Join_nation(Join_bottom(Shuffle→2, Shuffle→3),
     * Reducer→4)))))))}). The Agg/Sort/Project all run on the coordinator AFTER the worker gathers
     * {@code Join_bottom}'s output; only {@code Join_bottom} (+ its lower cascade level) lifts. Guards
     * against a regression where the coordinator-side Agg/Sort/Project above the dim join blocks
     * cascade detection.
     */
    public void testCascadeExtend_aggregateAboveNestedDimensionJoin() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3, "d_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE, "d_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);
        RelNode fourWay = makeFourWayDimTop(context);
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            fourWay,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        RelNode logical = LogicalAggregate.create(fourWay, List.of(), ImmutableBitSet.of(0), null, List.of(countCall));
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        assertTrue(
            "Aggregate above a nested dimension join over a cascade (real q10 shape) must still cascade",
            CascadeShuffleDAGRewriter.isCascade(dag)
        );
        // Capture the pre-rewrite dimension child stage id (the reducer-fed SINGLETON child that is
        // NOT one of Join_bottom's two HASH shuffle producers).
        int dimStageId = dag.rootStage()
            .getChildStages()
            .stream()
            .filter(s -> s.getExchangeInfo() != null && s.getExchangeInfo().distributionType() == RelDistribution.Type.SINGLETON)
            .map(Stage::getStageId)
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected a reducer-fed dimension child stage on the root"));

        CascadeShuffleDAGRewriter.Structure structure = CascadeShuffleDAGRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.buildLevels();
        assertEquals("only the bottom cascade lifts → two worker tiers", 2, levels.size());
        // The dimension reduce stage stays a coordinator child of the rebuilt root (kept, not
        // orphaned) and is NOT lifted into a worker.
        Stage newRoot = structure.dag().rootStage();
        Stage keptDim = newRoot.getChildStages().stream().filter(s -> s.getStageId() == dimStageId).findFirst().orElse(null);
        assertTrue("the dimension reduce stage must be kept on the coordinator, not orphaned", keptDim != null);
        assertTrue(
            "the dimension reduce stage must NOT be a lifted worker",
            levels.stream().noneMatch(l -> l.worker().getStageId() == dimStageId)
        );
    }

    /** Builds Join(Join(Join(a,b) on col0, c) on col0, d) on col1 (=size) — the top dimension join
     *  keys on a DIFFERENT column than the bottom cascade. Models q5's left-deep shape. */
    private RelNode makeFourWayDimTop(PlannerContext context) {
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
        // Outer join on col1 (size) = d.col1 — DIFFERENT key from the cascade (col0).
        RexNode abcdCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, abcCols + 1)
        );
        return LogicalJoin.create(abc, dScan, List.of(), abcdCond, Set.of(), JoinRelType.INNER);
    }

    private static List<String> nodeIds(int partitionCount) {
        List<String> ids = new java.util.ArrayList<>(partitionCount);
        for (int p = 0; p < partitionCount; p++) {
            ids.add("node-" + (p % CLUSTER_DATA_NODES));
        }
        return ids;
    }

    /** Builds Join(Join(a,b) on a.0=b.0, c) on (join.0 = c.0), all INNER equi. */
    private RelNode makeThreeWayJoin(PlannerContext context) {
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

    /** Builds Aggregate(Join(Join(a,b), c) by col0, count) — an Aggregate ABOVE the top join (the
     *  TPC-H q3 shape). The agg runs on the coordinator after the worker join, so cascade detection
     *  must NOT be blocked by it. */
    private RelNode makeAggregateOverThreeWayJoin(PlannerContext context) {
        RelNode join = makeThreeWayJoin(context);
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            join,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        return LogicalAggregate.create(join, List.of(), ImmutableBitSet.of(0), null, List.of(countCall));
    }

    /** Builds Join(Aggregate(Join(a,b) by col0, count), c) — an Aggregate between the join levels.
     *  Models PPL {@code a join b | stats count() by g | join c}. */
    private RelNode makeJoinOverAggregateOverJoin(PlannerContext context) {
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

        // GROUP BY col0, COUNT() — collapses A⋈B to one row per group key.
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

        // Outer join: agg's group-key column (output index 0) = c.col0.
        int aggCols = agg.getRowType().getFieldCount();
        RexNode aggcCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aggCols)
        );
        return LogicalJoin.create(agg, cScan, List.of(), aggcCond, Set.of(), JoinRelType.INNER);
    }

    /** Builds Join(Join(Aggregate(Join(a,b)), d), e) — the Aggregate is two join levels down, so the
     *  MIDDLE join sits over a non-partition-preserving (aggregate) side. Models
     *  {@code a join b | stats count() by g | join d | join e}. */
    private RelNode makeJoinOverJoinOverAggregateOverJoin(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode dScan = stubScan(mockTable("d_idx", "status", "size"));
        RelNode eScan = stubScan(mockTable("e_idx", "status", "size"));
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

        // Middle join: agg ⋈ d on group-key = d.col0.
        int aggCols = agg.getRowType().getFieldCount();
        RexNode aggdCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aggCols)
        );
        RelNode mid = LogicalJoin.create(agg, dScan, List.of(), aggdCond, Set.of(), JoinRelType.INNER);

        // Outer join: mid ⋈ e on col0 = e.col0.
        int midCols = mid.getRowType().getFieldCount();
        RexNode mideCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, midCols)
        );
        return LogicalJoin.create(mid, eScan, List.of(), mideCond, Set.of(), JoinRelType.INNER);
    }

    private PlannerContext buildMppContext(Map<String, Integer> shardCounts, Map<String, Long> rowCounts) {
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
