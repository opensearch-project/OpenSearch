/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.agg;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.planner.MockDataFusionBackend;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
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
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link HashShuffleAggregateDAGRewriter} — the M3 rewrite that lifts the FINAL
 * aggregate of a hash-shuffle plan into a worker-tier stage.
 *
 * <p>Core invariant: the lifted worker FINAL aggregate must go through the SAME backend
 * adaptation ({@code BackendPlanAdapter.adaptAll} → {@code DistributedAggregateRewriter}) that the
 * coordinator-centric FINAL aggregate receives in the normal pipeline. {@code adaptAll} only
 * rewrites a stage's plan-alternative fragments, never the {@code Stage.getFragment()} RelNode the
 * rewriter lifts from — and the rewriter's {@code forkAll} rebuilds alternatives straight from that
 * un-adapted fragment. So the rewriter MUST re-run {@code adaptAll} before {@code convertAll}; if it
 * skips it, the worker FINAL runs the original SINGLE aggregate calls over the wrong columns and
 * returns wrong results once a group's partials span multiple shards.
 *
 * <p>The discriminator used here is {@code COUNT}: {@code DistributedAggregateRewriter} decomposes a
 * FINAL {@code COUNT} into a {@code SUM} over the partial-count state column (COUNT's intermediate
 * field declares {@code reducer=SUM}). After adaptation the worker FINAL call is {@code SUM}; without
 * it the call stays {@code COUNT}.
 */
public class HashShuffleAggregateDAGRewriterTests extends BasePlannerRulesTests {

    private static final long LARGE = 10_000_000L;
    private static final int CLUSTER_DATA_NODES = 3;
    private static final int SHARDS = 3;

    public void testRewriteAdaptsWorkerFinalAggregateCountToSum() {
        PlannerContext context = buildMppContext(LARGE);
        RelNode cbo = runPlanner(makeCountGroupBy(), context);

        QueryDAG dag = DAGBuilder.build(cbo, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        // Mirror the normal pre-dispatch pipeline (DefaultPlanExecutor: forkAll → adaptAll →
        // convertAll). adaptAll here adapts only the plan alternatives, NOT the stage fragments —
        // so the FINAL aggregate the rewriter lifts from the consumer fragment is still un-adapted.
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        BackendPlanAdapter.adaptAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());

        Stage producer = findStageByRole(dag.rootStage(), Stage.StageRole.SHUFFLE_SCAN_AGG);
        assertNotNull("expected a SHUFFLE_SCAN_AGG producer stage — CBO must have picked the shuffle path", producer);
        Stage consumer = findParentOf(dag.rootStage(), producer);
        assertNotNull("expected a consumer stage whose child is the producer", consumer);

        // The consumer's FINAL aggregate carries SUM, not COUNT — the split rule pre-decomposes
        // via DistributedAggregateRewriter.FinalAggCallBuilder.buildFinalCalls so the agg call
        // list already references the partial-count state column. The DAG rewriter then lifts
        // this aggregate into a worker stage and re-runs adaptAll on the rewritten DAG so the
        // worker plan-alternative bytes match.
        OpenSearchAggregate consumerFinal = findFinalAggregate(consumer.getFragment());
        assertNotNull("consumer fragment must hold an OpenSearchAggregate(FINAL)", consumerFinal);
        assertEquals(
            "split rule already adapts FINAL: COUNT pre-decomposed to SUM",
            "SUM",
            consumerFinal.getAggCallList().getFirst().getAggregation().getName()
        );

        int partitionCount = producer.getExchangeInfo().partitionCount();
        List<String> targets = new ArrayList<>(partitionCount);
        for (int p = 0; p < partitionCount; p++) {
            targets.add("node-" + (p % CLUSTER_DATA_NODES));
        }

        HashShuffleAggregateDAGRewriter.Rewritten rewritten = HashShuffleAggregateDAGRewriter.rewrite(
            dag,
            consumer,
            producer,
            targets,
            context.getCapabilityRegistry(),
            /* preferMetadataDriver */ false
        );

        // The worker stage holds the lifted FINAL aggregate. Its plan alternatives must have been
        // re-adapted: COUNT decomposed to SUM over the partial-count state column.
        Stage worker = rewritten.worker();
        assertEquals("worker stage must be SHUFFLE_WORKER", Stage.StageRole.SHUFFLE_WORKER, worker.getRole());
        assertFalse("worker stage must have resolved plan alternatives", worker.getPlanAlternatives().isEmpty());

        OpenSearchAggregate workerFinal = findFinalAggregate(worker.getPlanAlternatives().getFirst().resolvedFragment());
        assertNotNull("worker plan alternative must hold an OpenSearchAggregate(FINAL)", workerFinal);
        AggregateCall workerCall = workerFinal.getAggCallList().getFirst();
        assertEquals(
            "worker FINAL aggregate must be adapted: COUNT decomposed to SUM over the partial-count "
                + "state column (regression guard — skipping adaptAll leaves it as COUNT and yields wrong "
                + "results across shards)",
            "SUM",
            workerCall.getAggregation().getName()
        );
        // The decomposed SUM reads the partial-count state column at position groupCount (= 1 here:
        // one group key at position 0, the count state at position 1), not the original arg.
        assertEquals("decomposed SUM must read the partial state column at index groupCount", List.of(1), workerCall.getArgList());
    }

    // ── helpers ────────────────────────────────────────────────────────────

    /** {@code SELECT status, COUNT(*) FROM test_index GROUP BY status} — COUNT is the discriminator
     *  ({@code DistributedAggregateRewriter} decomposes FINAL COUNT → SUM). */
    private RelNode makeCountGroupBy() {
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
        return LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(count));
    }

    /** Multi-data-node cluster + shuffle-aware backend + LARGE row count so the shuffle aggregate
     *  alternative wins CBO. Mirrors AggregateStrategyCBOSelectionTests#buildMppContext. */
    private PlannerContext buildMppContext(long rowCount) {
        ClusterState state = mockClusterStateWithDataNodes();
        Settings settings = Settings.builder().put("analytics.mpp.enabled", true).build();
        ToLongFunction<String> rowCountLookup = name -> "test_index".equals(name) ? rowCount : PlannerContext.UNKNOWN_ROW_COUNT;
        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = FieldStorageResolver::new;
        AnalyticsSearchBackendPlugin shuffleAware = new ShuffleAwareDataFusionBackend(CLUSTER_DATA_NODES);
        CapabilityRegistry registry = new CapabilityRegistry(List.of(shuffleAware, LUCENE), fieldStorageFactory);
        return new PlannerContext(registry, state, settings, rowCountLookup, /* profiling */ false);
    }

    private static ClusterState mockClusterStateWithDataNodes() {
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

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index("test_index", "test_index-uuid"));
        when(indexMetadata.getNumberOfShards()).thenReturn(SHARDS);
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(Map.of("properties", intFields()));
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
        when(indexMetadata.getSettings()).thenReturn(
            Settings.builder()
                .put("index.composite.primary_data_format", "parquet")
                .putList("index.composite.secondary_data_formats", "lucene")
                .build()
        );
        when(metadata.index("test_index")).thenReturn(indexMetadata);
        return state;
    }

    /** Depth-first search for the first stage carrying {@code role}; null if none. */
    private static Stage findStageByRole(Stage stage, Stage.StageRole role) {
        if (stage == null) return null;
        if (stage.getRole() == role) return stage;
        for (Stage child : stage.getChildStages()) {
            Stage found = findStageByRole(child, role);
            if (found != null) return found;
        }
        return null;
    }

    /** Returns the stage whose direct children include {@code target}; null if none. */
    private static Stage findParentOf(Stage stage, Stage target) {
        if (stage == null) return null;
        for (Stage child : stage.getChildStages()) {
            if (child == target) return stage;
            Stage found = findParentOf(child, target);
            if (found != null) return found;
        }
        return null;
    }

    /** First {@link OpenSearchAggregate}({@code mode=FINAL}) in {@code root}'s subtree; null if none. */
    private static OpenSearchAggregate findFinalAggregate(RelNode root) {
        if (root == null) return null;
        if (root instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.FINAL) {
            return agg;
        }
        for (RelNode input : root.getInputs()) {
            OpenSearchAggregate found = findFinalAggregate(input);
            if (found != null) return found;
        }
        return null;
    }

    /** Subclass of MockDataFusionBackend opting into MPP shuffle via defaultShuffleParallelism, with
     *  a no-op {@link org.opensearch.analytics.spi.FragmentConvertor} so {@code convertAll} (run
     *  inside {@link HashShuffleAggregateDAGRewriter#rewrite}) succeeds. The test asserts on the
     *  stage's {@code resolvedFragment} (set by adaptAll), so the conversion bytes are irrelevant. */
    private static class ShuffleAwareDataFusionBackend extends MockDataFusionBackend {
        private final int parallelism;

        ShuffleAwareDataFusionBackend(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public int defaultShuffleParallelism(ClusterState state) {
            return parallelism;
        }

        @Override
        public org.opensearch.analytics.spi.FragmentConvertor getFragmentConvertor() {
            return new org.opensearch.analytics.spi.FragmentConvertor() {
                @Override
                public byte[] convertFragment(RelNode fragment) {
                    return new byte[] { 1 };
                }

                @Override
                public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
                    return new byte[] { 1 };
                }

                @Override
                public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
                    return new byte[] { 1 };
                }
            };
        }
    }
}
