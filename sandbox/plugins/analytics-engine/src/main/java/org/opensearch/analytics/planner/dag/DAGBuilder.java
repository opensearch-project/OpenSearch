/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.CapabilityResolutionUtils;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Builds a {@link QueryDAG} from the CBO output by cutting at exchange boundaries.
 *
 * <p>SINGLETON: {@link OpenSearchExchangeReducer} is the boundary. Everything above
 * the reducer becomes the root (coordinator gather/compute) stage. The reducer's input
 * subtree becomes the child (data node) stage with a {@link ShardTargetResolver}.
 *
 * <p>Single-stage (no exchange): one stage with a {@link ShardTargetResolver}.
 * The Scheduler uses a simple {@code RowProducingSink} since {@code exchangeSinkProvider}
 * is null.
 *
 * <p>TODO: implement HASH/RANGE shuffle exchange cutting when joins and shuffle
 * aggregates are added.
 *
 * <p>Stage IDs are assigned bottom-up (leaf stages get lower IDs).
 *
 * @opensearch.internal
 */
public class DAGBuilder {

    private DAGBuilder() {}

    public static QueryDAG build(RelNode cboOutput, CapabilityRegistry registry, ClusterService clusterService) {
        int[] counter = { 0 };
        List<Stage> childStages = new ArrayList<>();
        RelNode rootFragment = sever(cboOutput, counter, childStages, registry, clusterService);

        ExchangeSinkProvider sinkProvider = null;
        TargetResolver rootTargetResolver = null;

        if (childStages.isEmpty()) {
            // Single-stage query — no exchange, dispatch directly to shards.
            // Scheduler uses RowProducingSink (exchangeSinkProvider is null).
            rootTargetResolver = new ShardTargetResolver(rootFragment, clusterService);
        } else {
            // Multi-stage — root is coordinator gather/compute.
            if (cboOutput instanceof OpenSearchRelNode rootRel) {
                List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(
                    registry, rootRel.getViableBackends()
                );
                sinkProvider = registry.getBackend(reduceViable.getFirst()).getExchangeSinkProvider();
            }
        }

        Stage rootStage = new Stage(counter[0]++, rootFragment, childStages, null, sinkProvider, rootTargetResolver);
        return new QueryDAG(UUID.randomUUID().toString(), rootStage);
    }

    private static RelNode sever(
        RelNode node,
        int[] counter,
        List<Stage> childStages,
        CapabilityRegistry registry,
        ClusterService clusterService
    ) {
        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            if (input instanceof OpenSearchExchangeReducer reducer) {
                newInputs.add(cutSingleton(reducer, counter, childStages, clusterService));
            } else {
                newInputs.add(sever(input, counter, childStages, registry, clusterService));
            }
        }
        if (node.getInputs().isEmpty()) return node;
        boolean changed = false;
        for (int i = 0; i < newInputs.size(); i++) {
            if (newInputs.get(i) != node.getInputs().get(i)) { changed = true; break; }
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    private static RelNode cutSingleton(
        OpenSearchExchangeReducer reducer,
        int[] counter,
        List<Stage> parentChildStages,
        ClusterService clusterService
    ) {
        RelNode childFragment = reducer.getInput();
        int childStageId = counter[0]++;
        parentChildStages.add(
            new Stage(childStageId, childFragment, List.of(), ExchangeInfo.singleton(), null,
                new ShardTargetResolver(childFragment, clusterService))
        );

        // Replace the reducer's input with a StageInputScan placeholder.
        // The root fragment ends at the reducer; the child stage fragment starts below it.
        // StageInputScan signals where the Scheduler feeds Arrow batches from the child stage.
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            reducer.getCluster(), reducer.getTraitSet(), childStageId,
            reducer.getInput().getRowType(), reducer.getViableBackends()
        );
        return new OpenSearchExchangeReducer(reducer.getCluster(), reducer.getTraitSet(), stageInput, reducer.getViableBackends());
    }
}
