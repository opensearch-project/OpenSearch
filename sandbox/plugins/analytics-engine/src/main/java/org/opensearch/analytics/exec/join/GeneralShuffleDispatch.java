/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.QueryExecution;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Single dispatch path for the GENERAL post-CBO scheduler (Option B — see
 * {@code MPP-GENERAL-SCHEDULING-DESIGN.md}). Drives the DAG that {@link DistributionEnforcementPass}
 * produced (under {@code analytics.mpp.cbo_native_cascade}), replacing the enumerated shape-routed
 * dispatchers ({@code HashShuffleDispatch} / {@code CascadeShuffleDispatch} /
 * {@code DistributedAggOverJoinDispatch}) with ONE path that works for any join depth / shape / type.
 *
 * <p>Because the enforcement pass already placed every shuffle and pre-split any distributed aggregate
 * into {@code FINAL(ER(PARTIAL(...)))}, this dispatcher does NO shape recognition and NO broadcast
 * build/inject. It only:
 * <ol>
 *   <li>promotes every join-over-two-shuffles stage to a worker tier IN PLACE
 *       ({@link GeneralShuffleDAGRewriter}) — the PARTIAL aggregate, if any, rides on the worker
 *       because it already sits above the join in the same stage fragment;</li>
 *   <li>enriches each worker level's shuffle producer/scan/worker-setup instructions, REUSING
 *       {@link CascadeShuffleDispatch#enrichLevels} verbatim (the per-level wiring is identical);</li>
 *   <li>hands the rewritten DAG to {@link QueryScheduler#execute} for concurrent dispatch.</li>
 * </ol>
 *
 * <p>Validation: the worker-promotion STRUCTURE is JVM-tested (mock backend, no convertAll); execution
 * correctness (the per-partition join + PARTIAL → coordinator FINAL, incl. outer-join tiers) is sf=10-only
 * (B3). The enrichment + transport are the exact code paths the cascade dispatcher already exercises at
 * sf=10.
 *
 * @opensearch.internal
 */
public final class GeneralShuffleDispatch {

    private static final Logger LOGGER = LogManager.getLogger(GeneralShuffleDispatch.class);

    private final QueryScheduler scheduler;
    private final ClusterService clusterService;
    private final CapabilityRegistry capabilityRegistry;
    private final boolean preferMetadataDriver;

    public GeneralShuffleDispatch(
        QueryScheduler scheduler,
        ClusterService clusterService,
        CapabilityRegistry capabilityRegistry,
        boolean preferMetadataDriver
    ) {
        this.scheduler = scheduler;
        this.clusterService = clusterService;
        this.capabilityRegistry = capabilityRegistry;
        this.preferMetadataDriver = preferMetadataDriver;
    }

    /**
     * Promotes the enforced DAG's join-shuffle stages to worker tiers, enriches the shuffle instructions,
     * and dispatches. Every worker blocks on its children's shuffle buffers; the top worker gathers
     * SINGLETON to the coordinator's reduce (the FINAL aggregate, if the query had one).
     */
    public void run(
        QueryContext ctx,
        QueryDAG dag,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        try {
            GeneralShuffleDAGRewriter.Rewritten rewritten = GeneralShuffleDAGRewriter.rewrite(
                dag,
                capabilityRegistry,
                preferMetadataDriver,
                (levelIndex, partitionCount) -> resolveTargetWorkerNodeIds(partitionCount)
            );

            // Reuse the cascade's per-level shuffle enrichment verbatim — producer/scan/worker-setup wiring
            // is identical (an intermediate worker is both a consumer of the level below and a producer to
            // the level above; the top worker only consumes + gathers SINGLETON to the coordinator).
            CascadeShuffleDispatch.enrichLevels(rewritten.levels(), ctx, clusterService, capabilityRegistry);

            LOGGER.debug("[GeneralShuffleDispatch] dispatching query={} with {} worker level(s)", ctx.queryId(), rewritten.levels().size());

            QueryExecution exec = scheduler.execute(ctx.withDag(rewritten.dag()), terminal);
            if (queryExecutionSink != null) {
                queryExecutionSink.accept(exec);
            }
        } catch (Exception e) {
            terminal.onFailure(e);
        }
    }

    /** Round-robins one target worker node per partition across the cluster's data nodes — same policy as
     *  {@link CascadeShuffleDispatch}. Shuffle buffers are keyed by {@code (queryId, stageId, partition)} so
     *  distinct levels reusing the same nodes do not collide. */
    private List<String> resolveTargetWorkerNodeIds(int partitionCount) {
        Map<String, DiscoveryNode> dataNodes = clusterService.state().nodes().getDataNodes();
        if (dataNodes == null || dataNodes.isEmpty()) {
            return List.of();
        }
        List<String> nodeIds = new ArrayList<>(dataNodes.keySet());
        List<String> targets = new ArrayList<>(partitionCount);
        for (int p = 0; p < partitionCount; p++) {
            targets.add(nodeIds.get(p % nodeIds.size()));
        }
        return targets;
    }
}
