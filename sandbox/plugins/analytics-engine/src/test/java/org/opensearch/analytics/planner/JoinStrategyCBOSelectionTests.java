/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
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
 * The load-bearing spike validator: does Volcano pick the right join strategy from row
 * counts and cluster topology? This is what answers M2's go/no-go question.
 *
 * <p>Each scenario constructs an {@link OpenSearchJoin} over two indices with explicit
 * row counts, runs the full {@link PlannerImpl#runAllOptimizations} pipeline (HEP marking
 * → CBO), then walks the result tree to find which exchange type Volcano materialized:
 * <ul>
 *   <li>{@link OpenSearchBroadcastExchange} above the build side → BROADCAST won.</li>
 *   <li>{@link OpenSearchShuffleExchange} above each side → HASH_SHUFFLE won.</li>
 *   <li>Plain {@code OpenSearchExchangeReducer} above each side → COORDINATOR_CENTRIC won.</li>
 * </ul>
 *
 * <p>The 3 scenarios + 2 contract enforcers map to M2's strategy table:
 * <ol>
 *   <li>small × large, mpp.enabled=true → BROADCAST (small as build).</li>
 *   <li>large × large, mpp.enabled=true → HASH_SHUFFLE.</li>
 *   <li>small × small, mpp.enabled=true → BROADCAST (small build cheap to replicate).</li>
 *   <li>any equi, mpp.enabled=false → COORDINATOR_CENTRIC.</li>
 *   <li>theta, any setting → COORDINATOR_CENTRIC.</li>
 * </ol>
 */
public class JoinStrategyCBOSelectionTests extends BasePlannerRulesTests {

    /** Row counts that should make the cost model pick each strategy clearly. */
    private static final long SMALL = 1_000L;
    private static final long LARGE = 10_000_000L;
    private static final int CLUSTER_DATA_NODES = 3;

    // ── Scenario 1: small × large ──────────────────────────────────────────

    public void testSmallByLargeMppEnabledPicksBroadcast() {
        PlannerContext context = buildMppContext(
            Map.of("small_idx", 3, "large_idx", 3),
            Map.of("small_idx", SMALL, "large_idx", LARGE),
            /* mppEnabled */ true
        );
        RelNode result = runPlanner(makeJoin(context, "small_idx", "large_idx", JoinRelType.INNER, /* equi */ true), context);

        assertContainsBroadcastExchange("small × large with mpp.enabled must pick BROADCAST", result);
        assertDoesNotContainShuffleExchange("small × large must not shuffle", result);
    }

    // ── Scenario 2: large × large ──────────────────────────────────────────

    public void testLargeByLargeMppEnabledPicksHashShuffle() {
        PlannerContext context = buildMppContext(
            Map.of("big_left", 3, "big_right", 3),
            Map.of("big_left", LARGE, "big_right", LARGE),
            /* mppEnabled */ true
        );
        RelNode result = runPlanner(makeJoin(context, "big_left", "big_right", JoinRelType.INNER, /* equi */ true), context);

        assertContainsShuffleExchange("large × large with mpp.enabled must pick HASH_SHUFFLE", result);
        assertDoesNotContainBroadcastExchange("large × large must not broadcast (would replicate 10M rows × 3 nodes)", result);
    }

    // ── Scenario 3: asymmetric sizes ───────────────────────────────────────

    public void testHighlyAsymmetricFavorsBroadcast() {
        // The cost-model contract: when one side is ≪ N× smaller than the other, BROADCAST
        // wins because replicating the small side costs less than shuffling both sides. This
        // is the canonical fact-dim join shape (small dim, large fact).
        //
        // Quantitative: broadcast cost ≈ small_rows × N; hash cost ≈ small_rows + large_rows.
        // Broadcast wins iff small_rows × (N - 1) < large_rows. With small=1k, large=10M, N=3:
        // broadcast ≈ 3k vs hash ≈ 10M+1k → broadcast wins decisively.
        //
        // The "small × small" case (1k vs 1k) is intentionally NOT tested as a strategy
        // assertion because the choice depends on cost-coefficient tuning rather than
        // architectural correctness. Either pick is defensible at that scale.
        PlannerContext context = buildMppContext(
            Map.of("dim_idx", 3, "fact_idx", 3),
            Map.of("dim_idx", SMALL, "fact_idx", LARGE),
            /* mppEnabled */ true
        );
        RelNode result = runPlanner(makeJoin(context, "dim_idx", "fact_idx", JoinRelType.INNER, /* equi */ true), context);

        assertContainsBroadcastExchange("dim (small) × fact (large) must pick BROADCAST", result);
    }

    /**
     * MODEST asymmetry (the {@code BroadcastJoinIT} shape: dim≈5 rows, fact≈30 rows — only a ~6×
     * ratio, not the 10,000× of the other scenarios). This is the case that regressed when the
     * upstream merge added {@code widthFactor} to {@code OpenSearchExchangeReducer.computeSelfCost}
     * but NOT to the broadcast/shuffle exchanges: the distorted cost ratio made CBO fall back to
     * coordinator-centric for the small-dim × large-fact join. With widthFactor applied uniformly
     * across all three exchanges the pre-merge ratio is restored and BROADCAST wins again. The big-
     * ratio scenarios above passed throughout (broadcast wins by orders of magnitude regardless), so
     * they did NOT catch this — this modest-ratio case is the guard.
     */
    public void testModestAsymmetryStillPicksBroadcast() {
        PlannerContext context = buildMppContext(
            Map.of("dim_small", 1, "fact_large", 5),
            Map.of("dim_small", 5L, "fact_large", 30L),
            /* mppEnabled */ true
        );
        RelNode result = runPlanner(makeJoin(context, "dim_small", "fact_large", JoinRelType.INNER, /* equi */ true), context);

        assertContainsBroadcastExchange(
            "modest 6x asymmetry (dim=5 × fact=30, the BroadcastJoinIT shape) must still pick BROADCAST",
            result
        );
        assertDoesNotContainShuffleExchange("modest asymmetry must not shuffle a tiny dim", result);
    }

    // ── Mixed equi + residual non-equi (TPC-H q14 shape) ───────────────────

    /**
     * A join with an equi key AND a residual non-equi predicate (q14: l_partkey=p_partkey AND
     * l_shipdate BETWEEN …) must STILL route through an MPP strategy on its equi key — NOT fall back
     * to coordinator-centric. Before the fix the split rules gated on {@code !info.isEqui()}, so the
     * residual predicate forced the whole (large) join to gather to the coordinator → ReduceSizeExceeded
     * at scale. small × large picks BROADCAST; the equi key partitions, the residual rides as a join
     * filter.
     */
    public void testMixedEquiResidualJoinStillUsesMpp() {
        PlannerContext context = buildMppContext(
            Map.of("small_idx", 3, "large_idx", 3),
            Map.of("small_idx", SMALL, "large_idx", LARGE),
            /* mppEnabled */ true
        );
        RelNode result = runPlanner(makeMixedEquiResidualJoin(context, "small_idx", "large_idx"), context);

        boolean broadcast = containsNodeOfType(result, OpenSearchBroadcastExchange.class);
        boolean shuffle = containsNodeOfType(result, OpenSearchShuffleExchange.class);
        assertTrue(
            "mixed equi+residual join must route via an MPP strategy (broadcast or shuffle), not coord-centric:\n"
                + org.apache.calcite.plan.RelOptUtil.toString(result),
            broadcast || shuffle
        );
    }

    /**
     * Symmetric guard: a PURE-theta join (no equi key at all) must still route COORDINATOR-CENTRIC —
     * the relaxation only admits joins that HAVE an equi key. Without an equi key there is nothing to
     * hash-partition / no key for the broadcast probe, so neither MPP rule may fire.
     */
    public void testPureThetaStillCoordCentricAfterRelax() {
        PlannerContext context = buildMppContext(
            Map.of("big_left", 3, "big_right", 3),
            Map.of("big_left", LARGE, "big_right", LARGE),
            /* mppEnabled */ true
        );
        RelNode result = runPlanner(makeJoin(context, "big_left", "big_right", JoinRelType.INNER, /* equi */ false), context);

        assertDoesNotContainBroadcastExchange("pure theta must NOT broadcast (no equi key)", result);
        assertDoesNotContainShuffleExchange("pure theta must NOT shuffle (no equi key)", result);
    }

    // ── Contract: mpp.enabled=false ────────────────────────────────────────

    public void testMppDisabledForcesCoordCentric() {
        PlannerContext context = buildMppContext(
            Map.of("big_left", 3, "big_right", 3),
            Map.of("big_left", LARGE, "big_right", LARGE),
            /* mppEnabled */ false
        );
        RelNode result = runPlanner(makeJoin(context, "big_left", "big_right", JoinRelType.INNER, /* equi */ true), context);

        // With mpp.enabled=false, neither broadcast nor hash rule fires; only coord-centric
        // produces an alternative. The result has plain ER over each side, no broadcast or
        // shuffle exchange anywhere.
        assertDoesNotContainBroadcastExchange("mpp.enabled=false must NOT produce broadcast", result);
        assertDoesNotContainShuffleExchange("mpp.enabled=false must NOT produce shuffle", result);
    }

    // ── Contract: theta join ───────────────────────────────────────────────

    public void testThetaJoinAlwaysCoordCentric() {
        // Even with mpp.enabled=true, theta routes coord-centric — broadcast and hash rules
        // refuse to fire on non-equi predicates.
        PlannerContext context = buildMppContext(
            Map.of("big_left", 3, "big_right", 3),
            Map.of("big_left", LARGE, "big_right", LARGE),
            /* mppEnabled */ true
        );
        RelNode result = runPlanner(makeJoin(context, "big_left", "big_right", JoinRelType.INNER, /* equi */ false), context);

        assertDoesNotContainBroadcastExchange("theta join must NOT broadcast (only coord-centric is legal)", result);
        assertDoesNotContainShuffleExchange("theta join must NOT shuffle (only coord-centric is legal)", result);
    }

    // ── Outer joins (TPC-H q13 shape) ──────────────────────────────────────

    /**
     * A LEFT OUTER equi-join over two large sides must hash-shuffle, NOT gather to the coordinator
     * (TPC-H q13: customer LEFT JOIN orders → ReduceSizeExceeded when coord-centric at scale). The
     * split rules carry no INNER-only gate, and hash-partitioning a LEFT equi-join on the join key is
     * correct: each preserved-side row and its matches land in one partition, so null-fill is
     * partition-local (standard Spark/Presto behavior). The worker join keeps joinType=LEFT verbatim;
     * DataFusion's HashJoinExec does the partition-local null-fill.
     */
    public void testLeftOuterJoinLargeLargeShuffles() {
        PlannerContext context = buildMppContext(
            Map.of("big_left", 3, "big_right", 3),
            Map.of("big_left", LARGE, "big_right", LARGE),
            /* mppEnabled */ true
        );
        RelNode result = runPlanner(makeJoin(context, "big_left", "big_right", JoinRelType.LEFT, /* equi */ true), context);

        assertContainsShuffleExchange("large LEFT OUTER equi-join must hash-shuffle, not gather to coordinator", result);
    }

    // ── Aggregate ABOVE a join (TPC-H q2/q11 shape) ───────────────────────

    /**
     * DIAGNOSTIC (#32): a large-fact × small-dim INNER equi-join FEEDING an aggregate
     * ({@code … join … | stats sum(x) by key}) — the TPC-H q11 bottom-join shape. At sf=10 this
     * gathers the 8M-row fact to the coordinator (ReduceSizeExceeded). The bare join (no agg) picks
     * BROADCAST; this test checks whether the aggregate ABOVE the join suppresses that.
     */
    public void testAggregateOverLargeSmallJoin_strategy() {
        PlannerContext context = buildMppContext(
            Map.of("fact_large", 3, "dim_small", 3),
            Map.of("fact_large", LARGE, "dim_small", SMALL),
            /* mppEnabled */ true
        );
        RelNode join = makeJoin(context, "fact_large", "dim_small", JoinRelType.INNER, /* equi */ true);
        RelNode aggOverJoin = makeAggregate(join, sumCall(join));
        RelNode result = runPlanner(aggOverJoin, context);

        boolean broadcast = containsNodeOfType(result, OpenSearchBroadcastExchange.class);
        boolean shuffle = containsNodeOfType(result, OpenSearchShuffleExchange.class);
        // Diagnostic assertion: an agg over a large×small join should STILL distribute the join
        // (broadcast the dim, or shuffle) — not gather the 8M fact to the coordinator.
        assertTrue(
            "agg over large×small join must distribute the join (broadcast or shuffle), not gather the fact:\n"
                + org.apache.calcite.plan.RelOptUtil.toString(result),
            broadcast || shuffle
        );
    }

    /**
     * DIAGNOSTIC (#32): a 3-way fact ⋈ dim1 ⋈ dim2 INNER join feeding an aggregate — the TPC-H q11
     * structure (partsupp ⋈ supplier ⋈ nation | stats sum by key). Checks whether the multi-way shape
     * (vs the 2-way above) is what stops the bottom join distributing on the cluster.
     */
    public void testAggregateOverThreeWayJoin_strategy() {
        PlannerContext context = buildMppContext(
            Map.of("fact_large", 3, "dim_a", 3, "dim_b", 3),
            Map.of("fact_large", LARGE, "dim_a", SMALL, "dim_b", SMALL),
            /* mppEnabled */ true
        );
        RelNode fact = stubScan(mockTable("fact_large", "status", "size"));
        RelNode dimA = stubScan(mockTable("dim_a", "status", "size"));
        RelNode dimB = stubScan(mockTable("dim_b", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        int factCols = fact.getRowType().getFieldCount();
        RexNode cond1 = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, factCols)
        );
        RelNode j1 = LogicalJoin.create(fact, dimA, List.of(), cond1, Set.of(), JoinRelType.INNER);
        int j1Cols = j1.getRowType().getFieldCount();
        RexNode cond2 = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, j1Cols)
        );
        RelNode j2 = LogicalJoin.create(j1, dimB, List.of(), cond2, Set.of(), JoinRelType.INNER);
        RelNode aggOverJoin = makeAggregate(j2, sumCall(j2));
        RelNode result = runPlanner(aggOverJoin, context);

        boolean broadcast = containsNodeOfType(result, OpenSearchBroadcastExchange.class);
        boolean shuffle = containsNodeOfType(result, OpenSearchShuffleExchange.class);
        assertTrue(
            "agg over 3-way (fact×dim×dim) join must distribute, not gather the fact to coordinator:\n"
                + org.apache.calcite.plan.RelOptUtil.toString(result),
            broadcast || shuffle
        );
    }

    // ── helpers ────────────────────────────────────────────────────────────

    /** Build a planner context with explicit row counts + multi-data-node cluster + custom
     *  shuffle-aware backend. The row counts feed {@code IndexNameTable.getRowCount()}
     *  via PlannerContext.tableRowCounts; the cluster nodes feed broadcast probe-count
     *  estimation; the shuffle-aware backend's defaultShuffleParallelism makes the hash
     *  rule's probeNodes > 1 gate pass. */
    private PlannerContext buildMppContext(Map<String, Integer> shardCounts, Map<String, Long> rowCounts, boolean mppEnabled) {
        ClusterState state = mockClusterStateWithDataNodes(shardCounts);
        Settings settings = Settings.builder().put("analytics.mpp.enabled", mppEnabled).build();
        ToLongFunction<String> rowCountLookup = name -> rowCounts.getOrDefault(name, PlannerContext.UNKNOWN_ROW_COUNT);
        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = FieldStorageResolver::new;
        // Use a shuffle-aware DataFusion backend so OpenSearchHashJoinSplitRule's partitionCount
        // > 1 gate passes.
        AnalyticsSearchBackendPlugin shuffleAware = new ShuffleAwareDataFusionBackend(CLUSTER_DATA_NODES);
        CapabilityRegistry registry = new CapabilityRegistry(List.of(shuffleAware, LUCENE), fieldStorageFactory);
        return new PlannerContext(registry, state, settings, rowCountLookup, /* profiling */ false);
    }

    /** Build a LogicalJoin between two indices with the given join shape, filtered through
     *  PlannerImpl. The join condition is either an equi (col0 == col0) or theta (col0 LT col0)
     *  predicate. */
    private RelNode makeJoin(PlannerContext context, String leftIdx, String rightIdx, JoinRelType joinType, boolean equi) {
        // Field names must match BasePlannerRulesTests.intFields() ("status", "size") — that's
        // what the mock cluster state's index mappings declare.
        RelNode leftScan = stubScan(mockTable(leftIdx, "status", "size"));
        RelNode rightScan = stubScan(mockTable(rightIdx, "status", "size"));
        int leftCols = leftScan.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = equi
            ? rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(intType, 0),
                rexBuilder.makeInputRef(intType, leftCols)
            )
            : rexBuilder.makeCall(
                SqlStdOperatorTable.LESS_THAN,
                rexBuilder.makeInputRef(intType, 0),
                rexBuilder.makeInputRef(intType, leftCols)
            );
        return LogicalJoin.create(leftScan, rightScan, List.of(), condition, Set.of(), joinType);
    }

    /** Build an INNER join whose condition is an equi key AND a residual non-equi predicate:
     *  {@code AND(left.col0 = right.col0, left.col1 < right.col1)}. This is the TPC-H q14 shape
     *  (l_partkey=p_partkey AND l_shipdate BETWEEN …). JoinInfo.analyzeCondition() yields non-empty
     *  leftKeys but isEqui()=false; the MPP split rules must still fire on the equi key. */
    private RelNode makeMixedEquiResidualJoin(PlannerContext context, String leftIdx, String rightIdx) {
        RelNode leftScan = stubScan(mockTable(leftIdx, "status", "size"));
        RelNode rightScan = stubScan(mockTable(rightIdx, "status", "size"));
        int leftCols = leftScan.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode equi = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftCols)
        );
        RexNode residual = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, leftCols + 1)
        );
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.AND, equi, residual);
        return LogicalJoin.create(leftScan, rightScan, List.of(), condition, Set.of(), JoinRelType.INNER);
    }

    /** Build a ClusterState with multiple stubbed data nodes (so probeNodes > 1) and per-index
     *  metadata for the requested shard counts. */
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

    /** Recursively walk the result tree to find any node of the given type. */
    private static boolean containsNodeOfType(RelNode node, Class<? extends RelNode> type) {
        RelNode unwrapped = RelNodeUtils.unwrapHep(node);
        if (type.isInstance(unwrapped)) {
            return true;
        }
        for (RelNode input : unwrapped.getInputs()) {
            if (containsNodeOfType(input, type)) return true;
        }
        return false;
    }

    private static void assertContainsBroadcastExchange(String message, RelNode tree) {
        assertTrue(
            message + "\nactual plan:\n" + org.apache.calcite.plan.RelOptUtil.toString(tree),
            containsNodeOfType(tree, OpenSearchBroadcastExchange.class)
        );
    }

    private static void assertDoesNotContainBroadcastExchange(String message, RelNode tree) {
        assertFalse(
            message + "\nactual plan:\n" + org.apache.calcite.plan.RelOptUtil.toString(tree),
            containsNodeOfType(tree, OpenSearchBroadcastExchange.class)
        );
    }

    private static void assertContainsShuffleExchange(String message, RelNode tree) {
        assertTrue(
            message + "\nactual plan:\n" + org.apache.calcite.plan.RelOptUtil.toString(tree),
            containsNodeOfType(tree, OpenSearchShuffleExchange.class)
        );
    }

    private static void assertDoesNotContainShuffleExchange(String message, RelNode tree) {
        assertFalse(
            message + "\nactual plan:\n" + org.apache.calcite.plan.RelOptUtil.toString(tree),
            containsNodeOfType(tree, OpenSearchShuffleExchange.class)
        );
    }

    /** Subclass of MockDataFusionBackend that opts into MPP shuffle by overriding
     *  {@code defaultShuffleParallelism} to return the cluster's data-node count. */
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
