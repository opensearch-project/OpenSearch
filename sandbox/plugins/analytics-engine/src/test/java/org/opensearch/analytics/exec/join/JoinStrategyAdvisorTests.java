/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.planner.MockDataFusionBackend;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
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
import java.util.function.ToLongFunction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Validates {@link JoinStrategyAdvisor}'s observation-only API against post-CBO DAGs:
 * <ul>
 *   <li>{@link JoinStrategyAdvisor#containsJoin} — true iff some stage's fragment contains an
 *       {@code OpenSearchJoin}; used to gate join-strategy metrics.</li>
 *   <li>{@link JoinStrategyAdvisor#observe} — reads the strategy CBO + DAGBuilder produced
 *       (BROADCAST when a stage tagged BROADCAST_BUILD exists; HASH_SHUFFLE when a stage
 *       carries a HASH_DISTRIBUTED ExchangeInfo; otherwise COORDINATOR_CENTRIC).</li>
 *   <li>{@link JoinStrategyAdvisor#findBroadcastBuild} / {@link JoinStrategyAdvisor#findBroadcastProbe}
 *       — locator helpers for {@code DefaultPlanExecutor.dispatchBroadcast}.</li>
 * </ul>
 *
 * <p>The advisor is observation-only: it must NOT make routing decisions, NOT mutate stage
 * roles, and NOT touch the DAG. The job is just "what did CBO + DAGBuilder pick, so the
 * dispatcher knows which path to invoke?"
 */
public class JoinStrategyAdvisorTests extends BasePlannerRulesTests {

    private static final long SMALL = 1_000L;
    private static final long LARGE = 10_000_000L;
    private static final int CLUSTER_DATA_NODES = 3;

    public void testNonJoinQueryReturnsCoordinatorCentric() {
        PlannerContext context = buildMppContext(Map.of("idx", 1), Map.of("idx", SMALL), /* mppEnabled */ true, /* shuffleEnabled */ true);
        RelNode marked = PlannerImpl.createPlan(stubScan(mockTable("idx", "status", "size")), context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());

        assertFalse("scan-only DAG must not be counted as a join", JoinStrategyAdvisor.containsJoin(dag));
        assertEquals(JoinStrategy.COORDINATOR_CENTRIC, JoinStrategyAdvisor.observe(dag));
        assertNull(JoinStrategyAdvisor.findBroadcastBuild(dag));
        assertNull(JoinStrategyAdvisor.findBroadcastProbe(dag));
    }

    public void testJoinWithMppDisabledReportsCoordinatorCentric() {
        // mpp.enabled=false → broadcast and hash split rules don't fire; only coord-centric
        // alternative survives. Advisor must report COORDINATOR_CENTRIC.
        PlannerContext context = buildMppContext(
            Map.of("a", 3, "b", 3),
            Map.of("a", LARGE, "b", LARGE),
            /* mppEnabled */ false,
            /* shuffleEnabled */ true
        );
        RelNode join = makeInnerEquiJoin("a", "b");
        QueryDAG dag = buildDag(join, context);

        assertTrue(JoinStrategyAdvisor.containsJoin(dag));
        assertEquals(JoinStrategy.COORDINATOR_CENTRIC, JoinStrategyAdvisor.observe(dag));
        assertNull(JoinStrategyAdvisor.findBroadcastBuild(dag));
    }

    public void testSmallByLargeJoinReportsBroadcast() {
        PlannerContext context = buildMppContext(
            Map.of("dim", 3, "fact", 3),
            Map.of("dim", SMALL, "fact", LARGE),
            /* mppEnabled */ true,
            /* shuffleEnabled */ true
        );
        QueryDAG dag = buildDag(makeInnerEquiJoin("dim", "fact"), context);

        assertTrue(JoinStrategyAdvisor.containsJoin(dag));
        assertEquals(JoinStrategy.BROADCAST, JoinStrategyAdvisor.observe(dag));

        Stage build = JoinStrategyAdvisor.findBroadcastBuild(dag);
        Stage probe = JoinStrategyAdvisor.findBroadcastProbe(dag);
        assertNotNull("broadcast DAG must have a BROADCAST_BUILD stage", build);
        assertNotNull("broadcast DAG must have a BROADCAST_PROBE stage", probe);
        assertEquals(Stage.StageRole.BROADCAST_BUILD, build.getRole());
        assertEquals(Stage.StageRole.BROADCAST_PROBE, probe.getRole());
        assertTrue("probe stage must list the build stage as a child", probe.getChildStages().contains(build));
    }

    public void testLargeByLargeJoinReportsHashShuffle() {
        PlannerContext context = buildMppContext(
            Map.of("a", 3, "b", 3),
            Map.of("a", LARGE, "b", LARGE),
            /* mppEnabled */ true,
            /* shuffleEnabled */ true
        );
        QueryDAG dag = buildDag(makeInnerEquiJoin("a", "b"), context);

        assertTrue(JoinStrategyAdvisor.containsJoin(dag));
        assertEquals(JoinStrategy.HASH_SHUFFLE, JoinStrategyAdvisor.observe(dag));
        assertNull("hash-shuffle DAG must NOT have a BROADCAST_BUILD stage", JoinStrategyAdvisor.findBroadcastBuild(dag));
    }

    public void testThetaJoinReportsCoordinatorCentric() {
        // Theta condition keeps broadcast/hash rules dormant; coord-centric is the only legal
        // alternative. Even with mpp.enabled=true, observe() returns COORDINATOR_CENTRIC.
        PlannerContext context = buildMppContext(
            Map.of("a", 3, "b", 3),
            Map.of("a", LARGE, "b", LARGE),
            /* mppEnabled */ true,
            /* shuffleEnabled */ true
        );
        QueryDAG dag = buildDag(makeThetaJoin("a", "b"), context);

        assertTrue(JoinStrategyAdvisor.containsJoin(dag));
        assertEquals(JoinStrategy.COORDINATOR_CENTRIC, JoinStrategyAdvisor.observe(dag));
    }

    // ── helpers ────────────────────────────────────────────────────────────

    private QueryDAG buildDag(RelNode logicalJoin, PlannerContext context) {
        RelNode optimized = PlannerImpl.createPlan(logicalJoin, context);
        return DAGBuilder.build(optimized, context.getCapabilityRegistry(), mockClusterService());
    }

    private RelNode makeInnerEquiJoin(String leftIdx, String rightIdx) {
        return makeJoin(leftIdx, rightIdx, JoinRelType.INNER, /* equi */ true);
    }

    private RelNode makeThetaJoin(String leftIdx, String rightIdx) {
        return makeJoin(leftIdx, rightIdx, JoinRelType.INNER, /* equi */ false);
    }

    private RelNode makeJoin(String leftIdx, String rightIdx, JoinRelType joinType, boolean equi) {
        RelNode left = stubScan(mockTable(leftIdx, "status", "size"));
        RelNode right = stubScan(mockTable(rightIdx, "status", "size"));
        int leftCols = left.getRowType().getFieldCount();
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
        return LogicalJoin.create(left, right, List.of(), condition, Set.of(), joinType);
    }

    /** Mirrors {@code JoinStrategyCBOSelectionTests.buildMppContext} — multi-data-node cluster
     *  + per-index row-count lookup + shuffle-aware backend so the broadcast / hash split rules
     *  pass their probeNodes / partitionCount gates. */
    private PlannerContext buildMppContext(
        Map<String, Integer> shardCounts,
        Map<String, Long> rowCounts,
        boolean mppEnabled,
        boolean shuffleEnabled
    ) {
        ClusterState state = mockClusterStateWithDataNodes(shardCounts);
        Settings settings = Settings.builder()
            .put("analytics.mpp.enabled", mppEnabled)
            .put("analytics.mpp.shuffle_enabled", shuffleEnabled)
            .build();
        ToLongFunction<String> rowCountLookup = name -> rowCounts.getOrDefault(name, PlannerContext.UNKNOWN_ROW_COUNT);
        AnalyticsSearchBackendPlugin shuffleAware = new ShuffleAwareDataFusionBackend(CLUSTER_DATA_NODES);
        CapabilityRegistry registry = new CapabilityRegistry(List.of(shuffleAware, LUCENE), FieldStorageResolver::new);
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
