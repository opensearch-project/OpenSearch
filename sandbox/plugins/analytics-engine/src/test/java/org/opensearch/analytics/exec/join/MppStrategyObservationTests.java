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
 * Validates the strategy a join query resolves to under the GENERAL post-CBO scheduler — ported from the
 * deleted {@code JoinStrategyAdvisorTests} (the {@code JoinStrategyAdvisor.observe()} read of CBO+DAGBuilder
 * output). The general path no longer has a per-strategy advisor; instead {@link DistributionEnforcementPass}
 * decides placement post-CBO and {@code DAGBuilder} tags each stage's {@link Stage.StageRole}, so the
 * "observed" strategy is read directly off the resulting DAG via {@link MppStrategy#containsJoin} +
 * {@link MppStrategy#findBroadcastBuild} + a hash-exchange scan (the same signals {@code DefaultPlanExecutor}
 * uses to pick the {@code /_analytics/_strategies} counter).
 *
 * <p>The contract is the same as the deleted advisor's: a small×large join resolves to BROADCAST (a
 * BROADCAST_BUILD stage appears), a large×large equi-join to HASH_SHUFFLE (a SHUFFLE_SCAN_LEFT/RIGHT pair
 * appears, no broadcast), and theta / mpp-off / scan-only to COORDINATOR_CENTRIC (no broadcast, no shuffle).
 */
public class MppStrategyObservationTests extends BasePlannerRulesTests {

    private static final long SMALL = 1_000L;
    private static final long LARGE = 10_000_000L;
    private static final int CLUSTER_DATA_NODES = 3;

    public void testNonJoinQueryReturnsCoordinatorCentric() {
        PlannerContext context = buildMppContext(Map.of("idx", 1), Map.of("idx", SMALL), /* mppEnabled */ true);
        QueryDAG dag = buildEnforcedDag(stubScan(mockTable("idx", "status", "size")), context);

        assertFalse("scan-only DAG must not be counted as a join", MppStrategy.containsJoin(dag));
        assertEquals(MppStrategy.COORDINATOR_CENTRIC, observe(dag));
        assertNull(MppStrategy.findBroadcastBuild(dag));
    }

    public void testJoinWithMppDisabledReportsCoordinatorCentric() {
        // mpp.enabled=false → the enforcement pass is skipped entirely; only the coord-centric gather
        // survives. The DAG must carry neither a broadcast nor a shuffle.
        PlannerContext context = buildMppContext(Map.of("a", 3, "b", 3), Map.of("a", LARGE, "b", LARGE), /* mppEnabled */ false);
        QueryDAG dag = buildEnforcedDag(makeInnerEquiJoin("a", "b"), context);

        assertTrue(MppStrategy.containsJoin(dag));
        assertEquals(MppStrategy.COORDINATOR_CENTRIC, observe(dag));
        assertNull(MppStrategy.findBroadcastBuild(dag));
    }

    public void testSmallByLargeJoinReportsBroadcast() {
        PlannerContext context = buildMppContext(Map.of("dim", 3, "fact", 3), Map.of("dim", SMALL, "fact", LARGE), /* mppEnabled */ true);
        QueryDAG dag = buildEnforcedDag(makeInnerEquiJoin("dim", "fact"), context);

        assertTrue(MppStrategy.containsJoin(dag));
        assertEquals(MppStrategy.BROADCAST, observe(dag));

        Stage build = MppStrategy.findBroadcastBuild(dag);
        assertNotNull("broadcast DAG must have a BROADCAST_BUILD stage", build);
        assertEquals(Stage.StageRole.BROADCAST_BUILD, build.getRole());
        Stage probe = findStageByRole(dag.rootStage(), Stage.StageRole.BROADCAST_PROBE);
        assertNotNull("broadcast DAG must have a BROADCAST_PROBE stage", probe);
        assertTrue("probe stage must list the build stage as a child", probe.getChildStages().contains(build));
        assertTrue("a broadcast plan must NOT introduce a hash shuffle", !hasHashShuffle(dag));
    }

    public void testLargeByLargeJoinReportsHashShuffle() {
        PlannerContext context = buildMppContext(Map.of("a", 3, "b", 3), Map.of("a", LARGE, "b", LARGE), /* mppEnabled */ true);
        QueryDAG dag = buildEnforcedDag(makeInnerEquiJoin("a", "b"), context);

        assertTrue(MppStrategy.containsJoin(dag));
        assertEquals(MppStrategy.HASH_SHUFFLE, observe(dag));
        assertNull("hash-shuffle DAG must NOT have a BROADCAST_BUILD stage", MppStrategy.findBroadcastBuild(dag));

        // DAGBuilder.cutShuffle role-tags each child stage by which join input it feeds; the two producers
        // must be distinct stages (one left, one right) so the binary shuffle transport binds both inputs.
        Stage left = findStageByRole(dag.rootStage(), Stage.StageRole.SHUFFLE_SCAN_LEFT);
        Stage right = findStageByRole(dag.rootStage(), Stage.StageRole.SHUFFLE_SCAN_RIGHT);
        assertNotNull("hash-shuffle DAG must have a SHUFFLE_SCAN_LEFT stage", left);
        assertNotNull("hash-shuffle DAG must have a SHUFFLE_SCAN_RIGHT stage", right);
        assertNotEquals("left and right producer stages must be distinct", left.getStageId(), right.getStageId());
    }

    public void testThetaJoinReportsCoordinatorCentric() {
        // A theta (non-equi) condition has no hash key, so neither broadcast nor shuffle is shippable; even
        // with mpp.enabled=true and LARGE×LARGE inputs, the pass must gather and run coord-centric.
        PlannerContext context = buildMppContext(Map.of("a", 3, "b", 3), Map.of("a", LARGE, "b", LARGE), /* mppEnabled */ true);
        QueryDAG dag = buildEnforcedDag(makeThetaJoin("a", "b"), context);

        assertTrue(MppStrategy.containsJoin(dag));
        assertEquals(MppStrategy.COORDINATOR_CENTRIC, observe(dag));
        assertNull(MppStrategy.findBroadcastBuild(dag));
    }

    // ── strategy observation (mirrors the deleted JoinStrategyAdvisor.observe contract) ─────────

    /** Reads the strategy CBO + the enforcement pass + DAGBuilder produced: BROADCAST when a BROADCAST_BUILD
     *  stage exists, HASH_SHUFFLE when a SHUFFLE_SCAN_* producer exists, else COORDINATOR_CENTRIC. */
    private static MppStrategy observe(QueryDAG dag) {
        if (MppStrategy.findBroadcastBuild(dag) != null) {
            return MppStrategy.BROADCAST;
        }
        if (hasHashShuffle(dag)) {
            return MppStrategy.HASH_SHUFFLE;
        }
        return MppStrategy.COORDINATOR_CENTRIC;
    }

    private static boolean hasHashShuffle(QueryDAG dag) {
        return findStageByRole(dag.rootStage(), Stage.StageRole.SHUFFLE_SCAN_LEFT) != null
            || findStageByRole(dag.rootStage(), Stage.StageRole.SHUFFLE_SCAN_RIGHT) != null
            || findStageByRole(dag.rootStage(), Stage.StageRole.SHUFFLE_SCAN_AGG) != null
            || findStageByRole(dag.rootStage(), Stage.StageRole.SHUFFLE_WORKER) != null;
    }

    private static Stage findStageByRole(Stage stage, Stage.StageRole role) {
        if (stage == null) {
            return null;
        }
        if (stage.getRole() == role) {
            return stage;
        }
        for (Stage child : stage.getChildStages()) {
            Stage found = findStageByRole(child, role);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    /** CBO → DistributionEnforcementPass (only when MPP is on) → DAGBuilder — the production formation
     *  pipeline, so the resulting roles reflect what the general scheduler would dispatch. */
    private QueryDAG buildEnforcedDag(RelNode logical, PlannerContext context) {
        RelNode plan = PlannerImpl.createPlan(logical, context);
        if (context.getSettings().getAsBoolean("analytics.mpp.enabled", false)) {
            plan = DistributionEnforcementPass.enforce(
                plan,
                context.getDistributionTraitDef(),
                CLUSTER_DATA_NODES,
                /* minRows */ 1L,
                /* shuffleAggregateEnabled */ true
            );
        }
        return DAGBuilder.build(plan, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
    }

    private RelNode makeInnerEquiJoin(String leftIdx, String rightIdx) {
        return makeJoin(leftIdx, rightIdx, /* equi */ true);
    }

    private RelNode makeThetaJoin(String leftIdx, String rightIdx) {
        return makeJoin(leftIdx, rightIdx, /* equi */ false);
    }

    private RelNode makeJoin(String leftIdx, String rightIdx, boolean equi) {
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
        return LogicalJoin.create(left, right, List.of(), condition, Set.of(), JoinRelType.INNER);
    }

    /** Multi-data-node cluster + per-index row-count lookup + shuffle-aware backend so the broadcast / hash
     *  cost competition has real inputs (mirrors CascadeShuffleProbeTests#buildMppContext). */
    private PlannerContext buildMppContext(Map<String, Integer> shardCounts, Map<String, Long> rowCounts, boolean mppEnabled) {
        ClusterState state = mockClusterStateWithDataNodes(shardCounts);
        Settings settings = Settings.builder().put("analytics.mpp.enabled", mppEnabled).build();
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
