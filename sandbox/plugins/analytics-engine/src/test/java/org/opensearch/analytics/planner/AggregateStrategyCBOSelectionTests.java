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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
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
 * The aggregate-side counterpart of {@link JoinStrategyCBOSelectionTests}: does Volcano
 * pick the right aggregate strategy from row counts and cluster topology?
 *
 * <p>Each scenario constructs a {@code GROUP BY} aggregate over a single index with explicit
 * row counts, runs the full {@link PlannerImpl#runAllOptimizations} pipeline (HEP marking
 * → CBO), then walks the result tree to find which exchange type Volcano materialized:
 * <ul>
 *   <li>{@link OpenSearchShuffleExchange} between PARTIAL and FINAL → HASH-shuffle path won
 *       ({@link org.opensearch.analytics.planner.rules.OpenSearchAggregateShuffleSplitRule}).</li>
 *   <li>Plain {@link OpenSearchExchangeReducer} between PARTIAL and FINAL → coord-centric path
 *       won ({@link org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule}).</li>
 * </ul>
 *
 * <p>The discriminator is the per-exchange cost: {@code OpenSearchExchangeReducer.computeSelfCost
 * = SETUP + rows} (gather-then-coord-final), {@code OpenSearchShuffleExchange.computeSelfCost
 * = rows + SETUP_PER_PARTITION × N} (shuffle path). Without per-partition row-count attribution
 * the FINAL aggregates cost the same — so the choice is driven purely by the exchange row term
 * and the constant setups. With raw rows ≫ N×SETUP_PER_PARTITION (high-cardinality groupings)
 * the shuffle setup is amortized; for tiny inputs SETUP wins.
 *
 * <p>Scenarios:
 * <ol>
 *   <li>large GROUP BY, mpp.enabled=true → HASH-shuffle.</li>
 *   <li>tiny GROUP BY, mpp.enabled=true → coord-centric (shuffle setup not worth it).</li>
 *   <li>GROUP BY, mpp.enabled=false → coord-centric (kill switch).</li>
 *   <li>no GROUP BY (empty groupSet), mpp.enabled=true → coord-centric (no shuffle key).</li>
 * </ol>
 */
public class AggregateStrategyCBOSelectionTests extends BasePlannerRulesTests {

    private static final long LARGE = 10_000_000L;
    private static final long TINY = 100L;
    private static final int CLUSTER_DATA_NODES = 3;
    private static final int SHARDS = 3;

    // ── Scenario 1: large GROUP BY ─────────────────────────────────────────

    public void testLargeGroupByMppEnabledPicksHashShuffle() {
        PlannerContext context = buildMppContext(LARGE, /* mppEnabled */ true);
        RelNode result = runPlanner(makeGroupedAggregate(), context);

        assertContainsShuffleExchange("large GROUP BY with mpp.enabled must pick HASH-shuffle", result);
    }

    // ── Scenario 2: tiny GROUP BY ──────────────────────────────────────────

    public void testTinyGroupByMppEnabledPrefersCoordCentric() {
        // At 100 rows × 3 shards = 300 partial rows total, the shuffle's per-partition setup
        // (5 × 3 = 15) plus rows (300) = 315 vs ER's setup (25) + rows (300) = 325. They're
        // close but ER wins by 10. Documents the cost-tuning contract: shuffle should only
        // win when input is large enough that its lower per-row coefficient amortizes the
        // higher setup. A regression where shuffle fires on tiny input would falsely flag
        // here.
        PlannerContext context = buildMppContext(TINY, /* mppEnabled */ true);
        RelNode result = runPlanner(makeGroupedAggregate(), context);

        assertDoesNotContainShuffleExchange("tiny GROUP BY must NOT shuffle (setup not amortized)", result);
    }

    // ── Scenario 3: kill switch ────────────────────────────────────────────

    public void testMppDisabledForcesCoordCentric() {
        PlannerContext context = buildMppContext(LARGE, /* mppEnabled */ false);
        RelNode result = runPlanner(makeGroupedAggregate(), context);

        // With mpp.enabled=false, OpenSearchAggregateShuffleSplitRule's matches() returns
        // false; only the coord-centric split rule fires, even at 10M rows.
        assertDoesNotContainShuffleExchange("mpp.enabled=false must NOT produce shuffle", result);
    }

    // ── Scenario 4: no GROUP BY ────────────────────────────────────────────

    public void testNoGroupByNeverShuffles() {
        // Empty groupSet → no shuffle key. The shuffle rule's groupSet.isEmpty() gate must
        // refuse to fire even at high cardinality — there's nothing to hash on.
        PlannerContext context = buildMppContext(LARGE, /* mppEnabled */ true);
        RelNode result = runPlanner(makeUngroupedAggregate(), context);

        assertDoesNotContainShuffleExchange("aggregate without GROUP BY must NEVER shuffle", result);
    }

    // ── Scenario 5: non-shard-local child (GROUP BY over a JOIN) ────────────

    public void testGroupByOverJoinNeverShuffles() {
        // `join … | stats … by …` shape: the aggregate's child is a JOIN whose output gathers
        // to COORDINATOR, not a shard-local producer — even though OpenSearchTableScans exist
        // beneath it. OpenSearchAggregateShuffleSplitRule.matches() must gate on the child's
        // SHARD locality trait, NOT merely "a scan exists somewhere below". A looser structural
        // gate would let the shuffle fire, and DAGBuilder.cutShuffle would mis-target one index's
        // shards / drop producer output. Single-shard indices guarantee the join itself emits no
        // shuffle, so any OpenSearchShuffleExchange in the tree could only come from the aggregate
        // rule — making this a clean isolation of the gate. Row counts are LARGE so the shuffle
        // WOULD win on cost if the gate ever let it fire (proving the trait gate, not cost, blocks
        // it).
        PlannerContext context = buildJoinMppContext(LARGE, /* mppEnabled */ true);
        RelNode result = runPlanner(makeGroupedAggregateOverJoin(), context);

        assertDoesNotContainShuffleExchange("GROUP BY over a (non-shard-local) JOIN must NOT shuffle", result);
    }

    // ── helpers ────────────────────────────────────────────────────────────

    /** Build a planner context with explicit row count + multi-data-node cluster + shuffle-aware
     *  backend. The row count feeds {@code IndexNameTable.getRowCount()}; the cluster nodes
     *  drive {@code defaultShuffleParallelism}; the backend's override makes
     *  {@code MppShufflePartitions.resolve} return N>1 so the shuffle rule's gate passes. */
    private PlannerContext buildMppContext(long rowCount, boolean mppEnabled) {
        ClusterState state = mockClusterStateWithDataNodes();
        Settings settings = Settings.builder().put("analytics.mpp.enabled", mppEnabled).build();
        ToLongFunction<String> rowCountLookup = name -> "test_index".equals(name) ? rowCount : PlannerContext.UNKNOWN_ROW_COUNT;
        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = FieldStorageResolver::new;
        AnalyticsSearchBackendPlugin shuffleAware = new ShuffleAwareDataFusionBackend(CLUSTER_DATA_NODES);
        CapabilityRegistry registry = new CapabilityRegistry(List.of(shuffleAware, LUCENE), fieldStorageFactory);
        return new PlannerContext(registry, state, settings, rowCountLookup, /* profiling */ false);
    }

    /** {@code SELECT status, SUM(size) FROM test_index GROUP BY status} */
    private RelNode makeGroupedAggregate() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "total_size"
        );
        return LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(sum));
    }

    /** {@code SELECT COUNT(*) FROM test_index} — empty groupSet. COUNT(*) has no field args
     *  so it sidesteps the SUM-arg type-inference path that's tricky for ungrouped aggregates. */
    private RelNode makeUngroupedAggregate() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        return LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(count));
    }

    /** {@code SELECT status, SUM(size) FROM left_index JOIN right_index ON … GROUP BY status} —
     *  an aggregate whose child is a JOIN. The join gathers its inputs to COORDINATOR, so the
     *  aggregate's child is NOT shard-local even though table scans exist beneath it. Equi-join on
     *  column 0 keeps it on the supported-predicate whitelist. */
    private RelNode makeGroupedAggregateOverJoin() {
        RelNode leftScan = stubScan(mockTable("left_index", "status", "size"));
        RelNode rightScan = stubScan(mockTable("right_index", "status", "size"));
        int leftCols = leftScan.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftCols)
        );
        RelNode join = LogicalJoin.create(leftScan, rightScan, List.of(), condition, Set.of(), JoinRelType.INNER);
        // Aggregate over the join output: GROUP BY col 0 (left status), SUM(col 1, left size).
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            join,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "total_size"
        );
        return LogicalAggregate.create(join, List.of(), ImmutableBitSet.of(0), null, List.of(sum));
    }

    /** Like {@link #buildMppContext} but registers the two single-shard indices the join shape
     *  reads ({@code left_index}, {@code right_index}). Single shard per index ensures the JOIN
     *  itself never produces a shuffle, isolating the aggregate rule under test. */
    private PlannerContext buildJoinMppContext(long rowCount, boolean mppEnabled) {
        ClusterState state = mockClusterStateWithIndices(Map.of("left_index", 1, "right_index", 1));
        Settings settings = Settings.builder().put("analytics.mpp.enabled", mppEnabled).build();
        ToLongFunction<String> rowCountLookup = name -> Map.of("left_index", rowCount, "right_index", rowCount)
            .getOrDefault(name, PlannerContext.UNKNOWN_ROW_COUNT);
        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = FieldStorageResolver::new;
        AnalyticsSearchBackendPlugin shuffleAware = new ShuffleAwareDataFusionBackend(CLUSTER_DATA_NODES);
        CapabilityRegistry registry = new CapabilityRegistry(List.of(shuffleAware, LUCENE), fieldStorageFactory);
        return new PlannerContext(registry, state, settings, rowCountLookup, /* profiling */ false);
    }

    /** ClusterState with CLUSTER_DATA_NODES data nodes and one SHARDS-shard test_index. */
    private static ClusterState mockClusterStateWithDataNodes() {
        return mockClusterStateWithIndices(Map.of("test_index", SHARDS));
    }

    /** ClusterState with CLUSTER_DATA_NODES data nodes and per-index shard counts. */
    private static ClusterState mockClusterStateWithIndices(Map<String, Integer> shardCounts) {
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
