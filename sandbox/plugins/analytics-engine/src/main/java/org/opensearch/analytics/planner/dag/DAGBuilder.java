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
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rel.OpenSearchValues;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Builds a {@link QueryDAG} from the CBO output by cutting at exchange boundaries.
 * Each {@link OpenSearchExchangeReducer} becomes a stage boundary; the subtree
 * below becomes a child stage and the reducer's own {@link ExchangeInfo} drives
 * the parent stage's input wiring. Stage IDs are assigned bottom-up.
 *
 * @opensearch.internal
 */
public class DAGBuilder {

    private DAGBuilder() {}

    public static QueryDAG build(RelNode cboOutput, CapabilityRegistry registry, ClusterService clusterService) {
        int[] counter = { 0 };
        List<Stage> childStages = new ArrayList<>();

        RelNode rootFragment;
        if (cboOutput instanceof OpenSearchExchangeReducer reducer) {
            // Root IS an ExchangeReducer — pure gather (no compute above the exchange).
            // Cut directly: child stage is the subtree below, root fragment is
            // ExchangeReducer → StageInputScan.
            rootFragment = cutAtExchange(reducer, counter, childStages, registry, clusterService);
        } else {
            rootFragment = sever(cboOutput, counter, childStages, registry, clusterService);
        }

        // Sink provider is needed whenever the root stage runs a backend plan locally —
        // either because it gathers child output (COORDINATOR_REDUCE) or because it's a
        // coord-only compute leaf like LogicalValues (LOCAL_COMPUTE). Pure shard root
        // (single-stage scan) and pure pass-through root (no children, no compute leaf)
        // don't need a backend sink.
        boolean hasComputeLeaf = RelNodeUtils.findNode(rootFragment, OpenSearchValues.class) != null;
        ExchangeSinkProvider sinkProvider = null;
        if (!childStages.isEmpty() || hasComputeLeaf) {
            List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(
                registry,
                ((OpenSearchRelNode) cboOutput).getViableBackends()
            );
            sinkProvider = registry.getBackend(reduceViable.getFirst()).getExchangeSinkProvider();
        }

        // Root needs a shard target only if its fragment actually contains a TableScan.
        // OpenSearchValues (literal-row source) and other coord-only leaves have no scan
        // and run as LOCAL_COMPUTE on the coordinator.
        boolean needsShardResolver = childStages.isEmpty()
            && RelNodeUtils.findNode(rootFragment, OpenSearchTableScan.class) != null;
        TargetResolver rootTargetResolver = needsShardResolver ? new ShardTargetResolver(rootFragment, clusterService) : null;

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
                newInputs.add(cutAtExchange(reducer, counter, childStages, registry, clusterService));
            } else {
                newInputs.add(sever(input, counter, childStages, registry, clusterService));
            }
        }
        if (node.getInputs().isEmpty()) return node;
        boolean changed = false;
        for (int i = 0; i < newInputs.size(); i++) {
            if (newInputs.get(i) != node.getInputs().get(i)) {
                changed = true;
                break;
            }
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    private static RelNode cutAtExchange(
        OpenSearchExchangeReducer reducer,
        int[] counter,
        List<Stage> parentChildStages,
        CapabilityRegistry registry,
        ClusterService clusterService
    ) {
        // Recurse into the child fragment with full sever() so any nested ExchangeReducers
        // (e.g. a Join below a top-level gather Reducer) are also cut into their own child
        // stages rather than being left intact inside the shard-local fragment.
        List<Stage> grandchildren = new ArrayList<>();
        RelNode childFragment = sever(reducer.getInput(), counter, grandchildren, registry, clusterService);

        int childStageId = counter[0]++;
        // A leaf stage (no grandchildren) runs on shards and needs a ShardTargetResolver.
        // An intermediate stage (some grandchildren were cut out below) runs at the
        // coordinator and consumes its grandchildren's outputs via an ExchangeSinkProvider.
        TargetResolver targetResolver = grandchildren.isEmpty() ? new ShardTargetResolver(childFragment, clusterService) : null;
        ExchangeSinkProvider childSinkProvider = null;
        if (!grandchildren.isEmpty()) {
            List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(registry, reducer.getViableBackends());
            childSinkProvider = registry.getBackend(reduceViable.getFirst()).getExchangeSinkProvider();
        }
        // ExchangeInfo comes from the reducer — the reducer is the exchange and carries
        // the distribution intent set by whichever rule introduced it.
        parentChildStages.add(
            new Stage(childStageId, childFragment, grandchildren, reducer.getExchangeInfo(), childSinkProvider, targetResolver)
        );

        // Replace the reducer's input with a StageInputScan placeholder.
        // The root fragment ends at the reducer; the child stage fragment starts below it.
        // StageInputScan signals where the Scheduler feeds Arrow batches from the child stage.
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            reducer.getCluster(),
            reducer.getTraitSet(),
            childStageId,
            reducer.getInput().getRowType(),
            reducer.getViableBackends()
        );
        return new OpenSearchExchangeReducer(
            reducer.getCluster(),
            reducer.getTraitSet(),
            stageInput,
            reducer.getViableBackends(),
            reducer.getExchangeInfo()
        );
    }
}
