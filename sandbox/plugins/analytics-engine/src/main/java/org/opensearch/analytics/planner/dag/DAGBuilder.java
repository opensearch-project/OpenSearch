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
import org.opensearch.analytics.planner.rel.OpenSearchProject;
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

        RelNode rootFragment;
        if (cboOutput instanceof OpenSearchExchangeReducer reducer) {
            // Root IS an ExchangeReducer — pure gather (no compute above the exchange).
            // Cut directly: child stage is the subtree below, root fragment is
            // ExchangeReducer → StageInputScan.
            rootFragment = cutSingleton(reducer, counter, childStages, clusterService);
        } else {
            rootFragment = sever(cboOutput, counter, childStages, registry, clusterService);
        }

        ExchangeSinkProvider sinkProvider = null;
        if (!childStages.isEmpty()) {
            List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(
                registry,
                ((OpenSearchRelNode) cboOutput).getViableBackends()
            );
            sinkProvider = registry.getBackend(reduceViable.getFirst()).getExchangeSinkProvider();
        }

        TargetResolver rootTargetResolver = childStages.isEmpty() ? new ShardTargetResolver(rootFragment, clusterService) : null;

        Stage rootStage = new Stage(counter[0]++, rootFragment, childStages, null, sinkProvider, rootTargetResolver);

        // QTF: if the root fragment's output schema contains __row_id__, the
        // LateMaterializationRule has narrowed the projection to sort/filter
        // columns + __row_id__. Wrap the reduce stage with a late-materialization
        // root that handles the fetch + assembly phase.
        if (isLateMaterializationEligible(rootFragment)) {
            return wrapWithLateMaterialization(counter, rootStage);
        }

        return new QueryDAG(UUID.randomUUID().toString(), rootStage);
    }

    /**
     * Checks whether the {@code LateMaterializationRule} has rewritten the plan for QTF.
     *
     * <p>Detection: the root fragment's top node must be an {@code OpenSearchProject}
     * whose output schema includes {@code __row_id__}. This is a reliable signal because:
     * <ul>
     *   <li>The scan always has {@code __row_id__} in its row type (added by schema builder),
     *       but a normal Project does not reference it — only the QTF-rewritten Project does.</li>
     *   <li>If there is no Project (e.g. {@code SELECT *}), the rule did not fire, so we
     *       should not trigger late materialization.</li>
     * </ul>
     */
    private static boolean isLateMaterializationEligible(RelNode rootFragment) {
        if (rootFragment instanceof OpenSearchProject project) {
            return project.getRowType().getFieldNames().contains("__row_id__");
        }
        return false;
    }

    /**
     * Wraps a 2-stage DAG (shard + reduce) into a 3-stage QTF DAG:
     * <pre>
     *   LateMaterializationStageExecution (root)
     *       └─ LocalStageExecution (COORDINATOR_REDUCE)  — sort + limit
     *             └─ ShardFragmentStageExecution (SHARD_FRAGMENT, injectShardOrdinal=true)
     * </pre>
     */
    private static QueryDAG wrapWithLateMaterialization(int[] counter, Stage reduceStage) {
        // Mark the shard fragment child to inject shard_id into every batch
        for (Stage child : reduceStage.getChildStages()) {
            if (child.getTargetResolver() != null) {
                child.setInjectShardOrdinal(true);
            }
        }

        // The reduce stage becomes a child of the new late-mat root.
        // Late-mat root has no sink provider, no target resolver, and no plan fragment
        // (it is a pure execution stage — position map + fetch + assembly).
        Stage lateMatRoot = new Stage(
            counter[0]++,
            null,   // no plan fragment (pure execution stage)
            List.of(reduceStage),
            null,   // no exchange info (root)
            null,   // no sink provider
            null,   // no target resolver
            true    // lateMaterialization = true
        );

        return new QueryDAG(UUID.randomUUID().toString(), lateMatRoot);
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
            if (newInputs.get(i) != node.getInputs().get(i)) {
                changed = true;
                break;
            }
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    private static RelNode cutSingleton(
        OpenSearchExchangeReducer reducer,
        int[] counter,
        List<Stage> parentChildStages,
        ClusterService clusterService
    ) {
        // Recurse into child fragment to handle nested exchanges.
        // TODO: recurse with full sever() (passing registry) when shuffle/broadcast
        // exchanges are added — not needed for PR2 (pure DF, max 2 stages).
        // TODO: for joins, each side has its own ExchangeReducer cut producing a
        // StageInputScan per join input. cutSingleton handles one side; sever() handles
        // both sides via its input iteration loop.
        List<Stage> grandchildren = new ArrayList<>();
        RelNode childFragment = reducer.getInput();

        int childStageId = counter[0]++;
        parentChildStages.add(
            new Stage(
                childStageId,
                childFragment,
                grandchildren,
                ExchangeInfo.singleton(),
                null,
                new ShardTargetResolver(childFragment, clusterService)
            )
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
        return new OpenSearchExchangeReducer(reducer.getCluster(), reducer.getTraitSet(), stageInput, reducer.getViableBackends());
    }
}
