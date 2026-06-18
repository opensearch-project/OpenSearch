/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.exec.OrdinalAppendingSink;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.CapabilityResolutionUtils;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rel.OpenSearchValues;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
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
 * <p>TODO move DAGBuilder AFTER PlanForker. Today this runs pre-fork while the
 * CBO output may carry multiple viable backends per operator, so the cut helpers
 * (e.g. {@code cutAtLateMaterialization}) have to pick a backend from the viable
 * list to fetch an {@code ExchangeSinkProvider} — at this point the sink-provider
 * choice is technically ambiguous. Running DAGBuilder after PlanForker would
 * collapse each operator's viable list to a single resolved backend per
 * alternative; the cut helpers could then assert exactly one viable backend and
 * throw an exception if not, instead of silently picking the first. Cleaner
 * separation of concerns: PlanForker does backend resolution, DAGBuilder does
 * stage cuts.
 *
 * @opensearch.internal
 */
public class DAGBuilder {

    private DAGBuilder() {}

    public static QueryDAG build(
        RelNode cboOutput,
        CapabilityRegistry registry,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        int[] counter = { 0 };
        List<Stage> childStages = new ArrayList<>();

        RelNode rootFragment;
        if (cboOutput instanceof OpenSearchExchangeReducer reducer) {
            // Root IS an ExchangeReducer — pure gather (no compute above the exchange).
            // Cut directly: child stage is the subtree below, root fragment is
            // ExchangeReducer → StageInputScan.
            rootFragment = cutAtExchange(reducer, counter, childStages, registry, clusterService, indexNameExpressionResolver);
        } else if (cboOutput instanceof OpenSearchLateMaterialization lm) {
            // LM at root, no above-ops (e.g. `source = t | where ... | sort col | head N`):
            // promote the LM stage to rootStage and skip the synthetic post-LM stage that would
            // wrap a bare StageInputScan placeholder.
            cutAtLateMaterialization(lm, counter, childStages, registry, clusterService, indexNameExpressionResolver);
            assert childStages.size() == 1 : "cutAtLateMaterialization must add exactly one child (the LM stage)";
            return new QueryDAG(newQueryId(), childStages.getFirst());
        } else {
            rootFragment = sever(cboOutput, counter, childStages, registry, clusterService, indexNameExpressionResolver);
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
        TargetResolver rootTargetResolver = needsShardResolver
            ? new ShardTargetResolver(rootFragment, clusterService, indexNameExpressionResolver)
            : null;

        Stage rootStage = new Stage(counter[0]++, rootFragment, childStages, null, sinkProvider, rootTargetResolver);
        return new QueryDAG(newQueryId(), rootStage);
    }

    /**
     * Mints a per-DAG queryId. TODO revisit if uniqueness is relied upon for correctness —
     * random UUID v4 is statistically safe but doesn't coordinate with task / context-id
     * allocation elsewhere in the engine.
     */
    private static String newQueryId() {
        return UUID.randomUUID().toString();
    }

    private static RelNode sever(
        RelNode node,
        int[] counter,
        List<Stage> childStages,
        CapabilityRegistry registry,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        List<RelNode> newInputs = new ArrayList<>();
        List<RelNode> rawInputs = node.getInputs();
        for (int inputIndex = 0; inputIndex < rawInputs.size(); inputIndex++) {
            RelNode input = rawInputs.get(inputIndex);
            if (input instanceof OpenSearchExchangeReducer reducer) {
                newInputs.add(cutAtExchange(reducer, counter, childStages, registry, clusterService, indexNameExpressionResolver));
            } else if (input instanceof OpenSearchShuffleExchange shuffle) {
                newInputs.add(
                    cutShuffle(shuffle, counter, childStages, registry, clusterService, node, inputIndex, indexNameExpressionResolver)
                );
            } else if (input instanceof OpenSearchBroadcastExchange broadcast) {
                newInputs.add(cutBroadcast(broadcast, counter, childStages, registry, clusterService, indexNameExpressionResolver));
            } else if (input instanceof OpenSearchLateMaterialization lm) {
                newInputs.add(cutAtLateMaterialization(lm, counter, childStages, registry, clusterService, indexNameExpressionResolver));
            } else {
                newInputs.add(sever(input, counter, childStages, registry, clusterService, indexNameExpressionResolver));
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

    /**
     * Cuts at an {@link OpenSearchLateMaterialization} wrapper. Two cuts happen:
     *
     * <ol>
     *   <li><b>Reduce child:</b> the wrapper's input subtree (Sort+Limit + ER + scans
     *       below) becomes the LM stage's child stage — a {@code COORDINATOR_REDUCE}
     *       gathering shard scans. QTF only fires multi-shard (single-shard collapse
     *       short-circuits in the rewriter), so the reduce child always has
     *       grandchildren and always gets a sink provider.</li>
     *   <li><b>LM stage itself:</b> a fresh {@link Stage} with fragment
     *       {@code Wrapper ← StageInputScan(reduce-child)}. Returned to the caller
     *       as a {@link OpenSearchStageInputScan} so the caller's parent fragment
     *       slots in a schema-bearing placeholder.</li>
     * </ol>
     *
     * <p>The caller (the {@link #sever} walk for the wrapper's parent) attaches whatever
     * post-LM ops sit above the wrapper on top of the returned StageInputScan. Those ops
     * end up in a vanilla {@code COORDINATOR_REDUCE} stage that runs them via Substrait
     * over the LM stage's stitched output. The LM stage itself runs Java-only
     * scatter/gather/stitch.
     *
     * <p>If the wrapper has no parent ops (the no-above-ops case), the caller's parent
     * fragment will just BE this StageInputScan. {@link #build} promotes the LM stage
     * to root in that case to avoid a degenerate empty COORDINATOR_REDUCE wrapper.
     */
    private static RelNode cutAtLateMaterialization(
        OpenSearchLateMaterialization lm,
        int[] counter,
        List<Stage> parentChildStages,
        CapabilityRegistry registry,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        // 1. Reduce child — Sort+Limit reduce above shard scans. Multi-shard QTF only.
        List<Stage> reduceChildren = new ArrayList<>();
        RelNode reduceFragment = sever(lm.getInput(), counter, reduceChildren, registry, clusterService, indexNameExpressionResolver);
        if (reduceChildren.isEmpty()) {
            throw new IllegalStateException(
                "QTF rewriter fired but the wrapper's input has no ExchangeReducer below it — "
                    + "single-shard collapse should have short-circuited the rewriter."
            );
        }
        int reduceStageId = counter[0]++;
        List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(registry, lm.getViableBackends());
        ExchangeSinkProvider reduceSinkProvider = registry.getBackend(reduceViable.getFirst()).getExchangeSinkProvider();
        Stage reduceStage = new Stage(
            reduceStageId,
            reduceFragment,
            reduceChildren,
            /*exchangeInfo=*/ null,
            reduceSinkProvider,
            /*targetResolver=*/ null
        );
        // Reducer feeds the LM stage. Stamp every shard's batches with their target.ordinal()
        // as ___ugsi BEFORE the backend's reduce sees them so the LM stage can group rows by
        // source shard for fan-out fetches.
        reduceStage.setInputSinkDecorator(
            (sink, allocator) -> new OrdinalAppendingSink(sink, allocator, OpenSearchLateMaterialization.UGSI_FIELD)
        );

        // 2. LM stage itself — fragment is the wrapper rooted at StageInputScan(reduceStage).
        OpenSearchRelNode lmInput = (OpenSearchRelNode) lm.getInput();
        OpenSearchStageInputScan reduceStageInput = new OpenSearchStageInputScan(
            lm.getCluster(),
            lm.getTraitSet(),
            reduceStageId,
            lm.getInput().getRowType(),
            lm.getViableBackends(),
            lmInput.getOutputFieldStorage()
        );
        OpenSearchLateMaterialization lmFragment = new OpenSearchLateMaterialization(
            lm.getCluster(),
            lm.getTraitSet(),
            reduceStageInput,
            lm.getAboveAnchorPhysicalFields(),
            lm.getAboveAnchorPhysicalFieldStorage(),
            lm.getViableBackends()
        );
        int lmStageId = counter[0]++;
        Stage lmStage = new Stage(
            lmStageId,
            lmFragment,
            List.of(reduceStage),
            /*exchangeInfo=*/ null,
            /*sinkProvider=*/ null,
            /*targetResolver=*/ null
        );
        parentChildStages.add(lmStage);

        // 3. Hand back StageInputScan(LM) so post-LM ops end up in their own COORDINATOR_REDUCE.
        // Schema is the wrapper's output rowType (= aboveAnchorPhysicalFields).
        //
        // TODO Stage 3 sink mode. The COORDINATOR_REDUCE that wraps the post-LM ops currently
        // inherits whatever sink the cluster setting selects (streaming vs memtable). For QTF
        // it should be memtable: LM emits a single VSR after full stitch (see Stitcher TODO),
        // so streaming buys nothing and the eager-scheduling deadlock-avoidance the streaming
        // sink is designed for doesn't apply. Once Stitcher supports incremental emission, we
        // can pick streaming for Camp-A post-LM ops (Filter/Project/hash Aggregate) and keep
        // memtable for Camp-B (Sort/TopN/global Aggregate). Detect post-LM op shape here and
        // hand a per-stage hint to the sink-provider selection.
        return new OpenSearchStageInputScan(
            lm.getCluster(),
            lm.getTraitSet(),
            lmStageId,
            lmFragment.getRowType(),
            lm.getViableBackends(),
            lm.getAboveAnchorPhysicalFieldStorage()
        );
    }

    private static RelNode cutAtExchange(
        OpenSearchExchangeReducer reducer,
        int[] counter,
        List<Stage> parentChildStages,
        CapabilityRegistry registry,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        // Recurse into the child fragment with full sever() so any nested ExchangeReducers
        // (e.g. a Join below a top-level gather Reducer) are also cut into their own child
        // stages rather than being left intact inside the shard-local fragment.
        List<Stage> grandchildren = new ArrayList<>();
        RelNode childFragment = sever(reducer.getInput(), counter, grandchildren, registry, clusterService, indexNameExpressionResolver);

        int childStageId = counter[0]++;
        // Stage execution location is decided by the fragment's contents, not the grandchild
        // count alone. A fragment with a TableScan runs on shards (ShardTargetResolver) — even
        // when it also has a grandchild stage feeding it, as in the broadcast-probe case where
        // the probe's join takes a TableScan plus a child BROADCAST_BUILD stage's output. A
        // fragment without a TableScan runs at the coordinator and consumes grandchildren via
        // an ExchangeSinkProvider.
        boolean fragmentHasShardScan = containsAnyInput(childFragment, OpenSearchTableScan.class);
        TargetResolver targetResolver = fragmentHasShardScan
            ? new ShardTargetResolver(childFragment, clusterService, indexNameExpressionResolver)
            : null;
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

        // Source both rowType and FSI from the reducer so they stay aligned 1:1 (its input lacks
        // QTF's ___ugsi entry). No-op for non-QTF reducers.
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            reducer.getCluster(),
            reducer.getTraitSet(),
            childStageId,
            reducer.getRowType(),
            reducer.getViableBackends(),
            reducer.getOutputFieldStorage()
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
        int parentInputIndex,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        // Recurse into the shuffle's input with full sever() so any nested exchanges below the
        // shuffle (e.g. a partial-aggregate that itself reduces) are also cut into their own
        // stages. M2 today only composes shuffle over a shard scan, but the recursion makes the
        // cutter robust to future plan shapes.
        List<Stage> grandchildren = new ArrayList<>();
        RelNode childFragment = sever(shuffle.getInput(), counter, grandchildren, registry, clusterService, indexNameExpressionResolver);

        int childStageId = counter[0]++;
        // Leaf shuffle producers run on shard nodes (the input is a shard scan); intermediate
        // ones run on whatever locality the inner subtree produces. Same logic as cutAtExchange.
        TargetResolver targetResolver = grandchildren.isEmpty()
            ? new ShardTargetResolver(childFragment, clusterService, indexNameExpressionResolver)
            : null;
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
        if (parent instanceof OpenSearchJoin) {
            if (parentInputIndex == 0) {
                childStage.setRole(Stage.StageRole.SHUFFLE_SCAN_LEFT);
            } else if (parentInputIndex == 1) {
                childStage.setRole(Stage.StageRole.SHUFFLE_SCAN_RIGHT);
            }
        } else if (parent instanceof OpenSearchAggregate) {
            // M3 hash-shuffle aggregate: single producer feeding a FINAL aggregate worker.
            // No left/right semantics — just one input stream per partition. The agg dispatcher
            // reads this role to lift the FINAL into a worker stage and attach a single
            // ShuffleScan instruction (vs the join's two).
            childStage.setRole(Stage.StageRole.SHUFFLE_SCAN_AGG);
        }
        parentChildStages.add(childStage);

        OpenSearchRelNode shuffleInput = (OpenSearchRelNode) shuffle.getInput();
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            shuffle.getCluster(),
            shuffle.getTraitSet(),
            childStageId,
            shuffle.getInput().getRowType(),
            shuffle.getViableBackends(),
            shuffleInput.getOutputFieldStorage()
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
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        List<Stage> grandchildren = new ArrayList<>();
        RelNode childFragment = sever(broadcast.getInput(), counter, grandchildren, registry, clusterService, indexNameExpressionResolver);

        int childStageId = counter[0]++;
        TargetResolver targetResolver = grandchildren.isEmpty()
            ? new ShardTargetResolver(childFragment, clusterService, indexNameExpressionResolver)
            : null;
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
