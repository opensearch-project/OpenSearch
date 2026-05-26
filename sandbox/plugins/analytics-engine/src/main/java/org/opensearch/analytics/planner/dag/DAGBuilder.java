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
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
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
        boolean needsShardResolver = childStages.isEmpty() && RelNodeUtils.findNode(rootFragment, OpenSearchTableScan.class) != null;
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
        List<RelNode> rawInputs = node.getInputs();
        for (int inputIndex = 0; inputIndex < rawInputs.size(); inputIndex++) {
            RelNode input = rawInputs.get(inputIndex);
            if (input instanceof OpenSearchExchangeReducer reducer) {
                newInputs.add(cutAtExchange(reducer, counter, childStages, registry, clusterService));
            } else if (input instanceof OpenSearchShuffleExchange shuffle) {
                newInputs.add(cutShuffle(shuffle, counter, childStages, registry, clusterService, node, inputIndex));
            } else if (input instanceof OpenSearchBroadcastExchange broadcast) {
                newInputs.add(cutBroadcast(broadcast, counter, childStages, registry, clusterService));
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
        // Stage execution location is decided by the fragment's contents, not the grandchild
        // count alone. A fragment with a TableScan runs on shards (ShardTargetResolver) — even
        // when it also has a grandchild stage feeding it, as in the broadcast-probe case where
        // the probe's join takes a TableScan plus a child BROADCAST_BUILD stage's output. A
        // fragment without a TableScan runs at the coordinator and consumes grandchildren via
        // an ExchangeSinkProvider.
        boolean fragmentHasShardScan = containsAnyInput(childFragment, OpenSearchTableScan.class);
        TargetResolver targetResolver = fragmentHasShardScan ? new ShardTargetResolver(childFragment, clusterService) : null;
        ExchangeSinkProvider childSinkProvider = null;
        if (!grandchildren.isEmpty() && !fragmentHasShardScan) {
            List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(registry, reducer.getViableBackends());
            childSinkProvider = registry.getBackend(reduceViable.getFirst()).getExchangeSinkProvider();
        }
        // ExchangeInfo comes from the reducer — the reducer is the exchange and carries
        // the distribution intent set by whichever rule introduced it.
        Stage childStage = new Stage(
            childStageId,
            childFragment,
            grandchildren,
            reducer.getExchangeInfo(),
            childSinkProvider,
            targetResolver
        );
        // Tag broadcast-probe role when the fragment runs on shards AND consumes a build stage's
        // output via an OpenSearchBroadcastScan placeholder. Other shard fragments stay at the
        // default SHARD_SOURCE; coord-only fragments stay default too (DefaultPlanExecutor's
        // dispatch tags COORDINATOR_REDUCE explicitly when needed).
        if (fragmentHasShardScan && containsAnyInput(childFragment, OpenSearchBroadcastScan.class)) {
            childStage.setRole(Stage.StageRole.BROADCAST_PROBE);
        }
        parentChildStages.add(childStage);

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

    /**
     * Cut at a {@link OpenSearchShuffleExchange}. The subtree below the shuffle becomes a child
     * stage; the parent fragment's view of the shuffle is preserved with its input replaced by
     * an {@link OpenSearchStageInputScan} placeholder. Exchange metadata on the child stage
     * carries the shuffle keys and partition count so the dispatcher can size partition buffers
     * and ship per-partition output to the right consumer node.
     *
     * <p>When the parent is an {@link org.opensearch.analytics.planner.rel.OpenSearchJoin} the
     * cutter tags the child stage {@link Stage.StageRole#SHUFFLE_SCAN_LEFT} or
     * {@link Stage.StageRole#SHUFFLE_SCAN_RIGHT} based on whether the shuffle feeds input 0 or 1
     * — {@code HashShuffleDispatch} consumes that tag to assign the {@code "left"} / {@code "right"}
     * side label on each producer's instruction. For other parents (Aggregate, intermediate Project)
     * the role stays at the default {@link Stage.StageRole#SHARD_SOURCE} since there is no
     * join-side semantics to encode.
     */
    private static RelNode cutShuffle(
        OpenSearchShuffleExchange shuffle,
        int[] counter,
        List<Stage> parentChildStages,
        CapabilityRegistry registry,
        ClusterService clusterService,
        RelNode parent,
        int parentInputIndex
    ) {
        // Recurse into the shuffle's input with full sever() so any nested exchanges below the
        // shuffle (e.g. a partial-aggregate that itself reduces) are also cut into their own
        // stages. M2 today only composes shuffle over a shard scan, but the recursion makes the
        // cutter robust to future plan shapes.
        List<Stage> grandchildren = new ArrayList<>();
        RelNode childFragment = sever(shuffle.getInput(), counter, grandchildren, registry, clusterService);

        int childStageId = counter[0]++;
        // Leaf shuffle producers run on shard nodes (the input is a shard scan); intermediate
        // ones run on whatever locality the inner subtree produces. Same logic as cutAtExchange.
        TargetResolver targetResolver = grandchildren.isEmpty() ? new ShardTargetResolver(childFragment, clusterService) : null;
        ExchangeSinkProvider childSinkProvider = null;
        if (!grandchildren.isEmpty()) {
            List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(registry, shuffle.getViableBackends());
            childSinkProvider = registry.getBackend(reduceViable.getFirst()).getExchangeSinkProvider();
        }
        Stage childStage = new Stage(
            childStageId,
            childFragment,
            grandchildren,
            ExchangeInfo.hashDistributed(shuffle.getHashKeys(), shuffle.getPartitionCount()),
            childSinkProvider,
            targetResolver
        );
        // Tag join-side role when the shuffle feeds an OpenSearchJoin's left or right input.
        // HashShuffleDispatch reads this to assemble the {@code side="left"|"right"} label on
        // each producer's ShuffleProducerInstructionNode and to compose the worker fragment's
        // two NamedScans (one per side).
        if (parent instanceof org.opensearch.analytics.planner.rel.OpenSearchJoin) {
            if (parentInputIndex == 0) {
                childStage.setRole(Stage.StageRole.SHUFFLE_SCAN_LEFT);
            } else if (parentInputIndex == 1) {
                childStage.setRole(Stage.StageRole.SHUFFLE_SCAN_RIGHT);
            }
        }
        parentChildStages.add(childStage);

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            shuffle.getCluster(),
            shuffle.getTraitSet(),
            childStageId,
            shuffle.getInput().getRowType(),
            shuffle.getViableBackends()
        );
        return new OpenSearchShuffleExchange(
            shuffle.getCluster(),
            shuffle.getTraitSet(),
            stageInput,
            shuffle.getHashKeys(),
            shuffle.getPartitionCount(),
            shuffle.getViableBackends()
        );
    }

    /**
     * True iff any node in {@code root}'s tree (including all inputs of multi-input nodes
     * like {@link org.opensearch.analytics.planner.rel.OpenSearchJoin}) is an instance of
     * {@code type}. {@link RelNodeUtils#findNode} only walks the first input, which misses
     * the right side of a join — for example a broadcast-probe fragment is
     * {@code Join(BroadcastScan, TableScan)}, where the TableScan sits on input 1.
     */
    private static boolean containsAnyInput(RelNode root, Class<? extends RelNode> type) {
        if (type.isInstance(root)) return true;
        for (RelNode input : root.getInputs()) {
            if (containsAnyInput(input, type)) return true;
        }
        return false;
    }

    /**
     * Cut at an {@link OpenSearchBroadcastExchange}. The subtree below the exchange becomes a
     * standalone build stage tagged {@link Stage.StageRole#BROADCAST_BUILD}. The parent fragment's
     * view of the broadcast is replaced by an {@link OpenSearchBroadcastScan} placeholder that
     * matches by build-stage id — the same id used in the {@link
     * org.opensearch.analytics.spi.BroadcastInjectionInstructionNode} attached at dispatch time
     * and in the data-node-side memtable registration name.
     *
     * <p>Build stages run on shards (the input is a TableScan in M1); a future M2+ shape could
     * produce a coordinator-side build (e.g. broadcast over an aggregate result), at which point
     * the empty-grandchildren branch would need to gain a sink provider just like the other
     * cutters. For now the build always reduces to a leaf shard fragment.
     */
    private static RelNode cutBroadcast(
        OpenSearchBroadcastExchange broadcast,
        int[] counter,
        List<Stage> parentChildStages,
        CapabilityRegistry registry,
        ClusterService clusterService
    ) {
        List<Stage> grandchildren = new ArrayList<>();
        RelNode childFragment = sever(broadcast.getInput(), counter, grandchildren, registry, clusterService);

        int childStageId = counter[0]++;
        TargetResolver targetResolver = grandchildren.isEmpty() ? new ShardTargetResolver(childFragment, clusterService) : null;
        ExchangeSinkProvider childSinkProvider = null;
        if (!grandchildren.isEmpty()) {
            List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(registry, broadcast.getViableBackends());
            childSinkProvider = registry.getBackend(reduceViable.getFirst()).getExchangeSinkProvider();
        }
        Stage buildStage = new Stage(
            childStageId,
            childFragment,
            grandchildren,
            ExchangeInfo.singleton(),
            childSinkProvider,
            targetResolver
        );
        buildStage.setRole(Stage.StageRole.BROADCAST_BUILD);
        parentChildStages.add(buildStage);

        // Replace the broadcast exchange in the parent fragment with a BroadcastScan placeholder
        // keyed by build-stage id. The probe-side handler chain looks up the registered memtable
        // by namedInputId="broadcast-<buildStageId>" at execution time.
        return new OpenSearchBroadcastScan(
            broadcast.getCluster(),
            broadcast.getTraitSet(),
            childStageId,
            broadcast.getInput().getRowType(),
            broadcast.getViableBackends()
        );
    }
}
