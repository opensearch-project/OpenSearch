/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.canmatch.LongSet;
import org.opensearch.analytics.exec.join.BroadcastRuntimeFilters;
import org.opensearch.analytics.exec.join.DistributionEnforcementPass;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;

import java.io.ByteArrayOutputStream;
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
 * Why the broadcast family never fired.
 *
 * <p>Across every cluster measurement its counter stayed at zero, which is the one outcome that cannot be
 * explained by the workload: this family is supposed to be the <em>free</em> half of the design — the build
 * side is already captured on the coordinator before the probe is dispatched, so deriving a key set from it
 * costs one pass over bytes already in hand. A permanent zero therefore means the mechanism declines, not
 * that the queries were unsuitable.
 *
 * <p>These tests drive it through the real planner and a synthetic capture. They establish that the
 * mechanism works for a broadcast-only query — which is also the shape where production skips the shuffle
 * promotion entirely, so this is the real path for such queries. What they deliberately do NOT cover is a
 * broadcast build sitting under a shuffle cascade: the mock cost model here will not produce that shape (it
 * shuffles the small side too), and a fixture that has to be forced into existence would be asserting
 * against a plan the planner does not actually make. That case is investigated on a cluster instead, which
 * is why {@code BroadcastRuntimeFilters} logs a reason on every decline path including the one that used to
 * be silent.
 */
public class BroadcastRuntimeFilterPlanTests extends BasePlannerRulesTests {

    private static final int CLUSTER_DATA_NODES = 3;
    private static final long LARGE = 10_000_000L;
    private static final long SMALL = 1_000L;
    private static final int MAX_VALUES = 4096;

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    /** The plan shape the derivation depends on: is the captured build side a DIRECT input of the join? */
    public void testTheBroadcastScanIsADirectJoinInput() {
        QueryDAG dag = broadcastJoinDag();
        List<OpenSearchBroadcastScan> scans = new ArrayList<>();
        collectScans(dag.rootStage(), scans);
        assertFalse("the fixture must produce a broadcast build at all", scans.isEmpty());

        // BroadcastRuntimeFilters looks only at a join's direct inputs, while the injection path searches the
        // whole fragment. If a real plan wraps the scan, that asymmetry alone would explain the zero.
        assertTrue(
            "a broadcast scan must be reachable as a direct join input, or the derivation cannot see it",
            directJoinInputScanExists(dag.rootStage())
        );
    }

    /** End to end through the real planner: a captured integral key must yield a can-match filter. */
    public void testAnIntegralBuildKeyYieldsACanMatchFilter() throws Exception {
        QueryDAG dag = broadcastJoinDag();
        int buildStageId = broadcastBuildStageId(dag.rootStage());

        int attached = BroadcastRuntimeFilters.attach(dag.rootStage(), Map.of(buildStageId, intStream(1, 2, 3)), allocator, MAX_VALUES);

        assertEquals("exactly one shard stage should carry the derived filter", 1, attached);
        List<LongSet> sets = new ArrayList<>();
        collectLongSets(dag.rootStage(), sets);
        assertEquals("one filter, on the probe-side key column", 1, sets.size());
        assertArrayEquals("the build side's distinct keys, sorted", new long[] { 1, 2, 3 }, sets.get(0).values());
    }

    /** Nothing may be attached when the coordinator holds no capture for that build. */
    public void testNoCaptureYieldsNoFilter() {
        QueryDAG dag = broadcastJoinDag();
        assertEquals(0, BroadcastRuntimeFilters.attach(dag.rootStage(), Map.of(), allocator, MAX_VALUES));
    }

    // ── Fixtures ─────────────────────────────────────────────────────────

    /**
     * {@code large ⋈ small} on {@code status}, with a broadcast budget big enough that CBO takes the
     * broadcast alternative — the shape this family exists for.
     */
    private QueryDAG broadcastJoinDag() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", SMALL);
        PlannerContext context = mppContext(shardCounts, rowCounts);

        RelNode leftScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode rightScan = stubScan(mockTable("b_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftScan.getRowType().getFieldCount())
        );
        RelNode join = LogicalJoin.create(leftScan, rightScan, List.of(), condition, Set.of(), JoinRelType.INNER);

        RelNode enforced = DistributionEnforcementPass.enforce(
            runPlanner(join, context),
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1L,
            /* shuffleAggregateEnabled */ true
        );
        return DAGBuilder.build(enforced, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
    }

    private PlannerContext mppContext(Map<String, Integer> shardCounts, Map<String, Long> rowCounts) {
        Settings settings = Settings.builder().put("analytics.mpp.enabled", true).build();
        ToLongFunction<String> rowCountLookup = name -> rowCounts.getOrDefault(name, PlannerContext.UNKNOWN_ROW_COUNT);
        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = FieldStorageResolver::new;
        CapabilityRegistry registry = new CapabilityRegistry(
            List.of(new ShuffleAwareBackend(CLUSTER_DATA_NODES), LUCENE),
            fieldStorageFactory
        );
        return new PlannerContext(registry, clusterStateWithDataNodes(shardCounts), settings, rowCountLookup, false);
    }

    private static ClusterState clusterStateWithDataNodes(Map<String, Integer> shardCounts) {
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
            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(indexMetadata.getIndex()).thenReturn(new Index(indexName, indexName + "-uuid"));
            when(indexMetadata.getNumberOfShards()).thenReturn(entry.getValue());
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

    private static class ShuffleAwareBackend extends MockDataFusionBackend {
        private final int parallelism;

        ShuffleAwareBackend(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public int defaultShuffleParallelism(ClusterState state) {
            return parallelism;
        }
    }

    /** Arrow IPC with one nullable `int32` column, the shape a captured build side arrives in. */
    private byte[] intStream(int... values) throws Exception {
        Schema schema = new Schema(List.of(new Field("status", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector vector = (IntVector) root.getVector("status");
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                vector.allocateNew(values.length);
                for (int i = 0; i < values.length; i++) {
                    vector.setSafe(i, values[i]);
                }
                root.setRowCount(values.length);
                writer.writeBatch();
                writer.end();
            }
            return out.toByteArray();
        }
    }

    private static void collectScans(Stage stage, List<OpenSearchBroadcastScan> out) {
        if (stage.getFragment() != null) {
            out.addAll(RelNodeUtils.findNodes(stage.getFragment(), OpenSearchBroadcastScan.class));
        }
        for (Stage child : stage.getChildStages()) {
            collectScans(child, out);
        }
    }

    private static boolean directJoinInputScanExists(Stage stage) {
        if (stage.getFragment() != null) {
            for (org.opensearch.analytics.planner.rel.OpenSearchJoin join : RelNodeUtils.findNodes(
                stage.getFragment(),
                org.opensearch.analytics.planner.rel.OpenSearchJoin.class
            )) {
                for (RelNode input : join.getInputs()) {
                    if (RelNodeUtils.unwrapHep(input) instanceof OpenSearchBroadcastScan) {
                        return true;
                    }
                }
            }
        }
        for (Stage child : stage.getChildStages()) {
            if (directJoinInputScanExists(child)) {
                return true;
            }
        }
        return false;
    }

    private static int broadcastBuildStageId(Stage stage) {
        if (stage.getRole() == Stage.StageRole.BROADCAST_BUILD) {
            return stage.getStageId();
        }
        for (Stage child : stage.getChildStages()) {
            int found = broadcastBuildStageId(child);
            if (found >= 0) {
                return found;
            }
        }
        return -1;
    }

    private static void collectLongSets(Stage stage, List<LongSet> out) {
        for (CanMatchFilter filter : stage.getCanMatchFilters()) {
            if (filter instanceof LongSet set) {
                out.add(set);
            }
        }
        for (Stage child : stage.getChildStages()) {
            collectLongSets(child, out);
        }
    }
}
