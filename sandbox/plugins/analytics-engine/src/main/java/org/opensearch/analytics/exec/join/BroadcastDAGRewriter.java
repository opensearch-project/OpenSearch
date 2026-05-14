/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.CapabilityResolutionUtils;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.ShardTargetResolver;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites a {@link JoinStrategyAdvisor}-tagged DAG from the M0 coordinator-centric shape into
 * the M1 broadcast shape: the join is pushed down onto a new probe-side stage, and the root
 * becomes a simple gather over the probe stage's joined output.
 *
 * <p>Input DAG (from DAGBuilder after advisor tagging):
 * <pre>
 * root (COORDINATOR_REDUCE)
 *   fragment = OpenSearchJoin(ExchangeReducer(StageInputScan(0)), ExchangeReducer(StageInputScan(1)))
 *   child 0 tagged BROADCAST_BUILD or BROADCAST_PROBE → scan of table A
 *   child 1 tagged BROADCAST_BUILD or BROADCAST_PROBE → scan of table B
 * </pre>
 *
 * <p>Output DAG:
 * <pre>
 * root (COORDINATOR_REDUCE)
 *   fragment = ExchangeReducer(StageInputScan(probe-id))
 *   └── probe stage (BROADCAST_PROBE, shard target over probe index)
 *         fragment = OpenSearchJoin(&lt;probe scan&gt;, OpenSearchBroadcastScan(buildStageId))
 *         └── build stage (BROADCAST_BUILD, shard target over build index)
 *               fragment = &lt;build scan&gt;
 * </pre>
 *
 * <p>The probe stage carries the join; each probe data node executes
 * {@code Join(ShardScan, NamedScan("broadcast-&lt;buildStageId&gt;"))} against its local shard
 * plus the broadcast memtable registered by {@code BroadcastInjectionHandler} from an
 * {@code BroadcastInjectionInstructionNode} on the probe stage's {@code FragmentExecutionRequest}.
 * The build stage remains a plain scan; its output is captured at the coordinator as Arrow IPC
 * bytes and shipped to every probe data node as the broadcast payload.
 *
 * <p>Rewriter is side-effect-free on the input DAG — it constructs a new {@link QueryDAG}
 * with fresh {@link Stage} instances and re-runs the plan-side pipeline
 * ({@link PlanForker#forkAll}, {@link BackendPlanAdapter#adaptAll}; the latter now also
 * drives partial/final aggregate decomposition internally) so each stage has a narrowed
 * plan alternative. Fragment-to-Substrait conversion is left to the caller so planner-side
 * tests can drive the rewriter against mock backends that don't ship a FragmentConvertor.
 *
 * @opensearch.internal
 */
public final class BroadcastDAGRewriter {

    private static final Logger LOGGER = LogManager.getLogger(BroadcastDAGRewriter.class);

    private BroadcastDAGRewriter() {}

    /**
     * Produces the broadcast-shape DAG given an M0 DAG whose child stages have been role-tagged
     * by {@link JoinStrategyAdvisor#tagBroadcastRoles}. The input DAG is not mutated.
     *
     * @throws IllegalStateException if the DAG shape doesn't match the M0 coordinator-centric join
     *         pattern (missing role tags, root isn't a join, etc.). Callers should fall back to
     *         coordinator-centric dispatch in that case.
     */
    public static QueryDAG rewrite(QueryDAG inputDag, CapabilityRegistry registry, ClusterService clusterService) {
        Stage inputRoot = inputDag.rootStage();
        RelNode rootFragment = RelNodeUtils.unwrapHep(inputRoot.getFragment());
        // Capture any unary wrappers above the root join (Project, Sort, Filter) — Calcite often
        // leaves a Project on top, and user queries with `| sort | head` add an OpenSearchSort.
        // We need to keep those wrappers in the rewritten root so the coord performs them on the
        // gathered probe output (correct semantics: a global top-N over all probe outputs).
        List<RelNode> rootWrappers = new ArrayList<>();
        RelNode current = rootFragment;
        while (current instanceof OpenSearchProject || current instanceof OpenSearchSort || current instanceof OpenSearchFilter) {
            rootWrappers.add(current);
            List<RelNode> ins = current.getInputs();
            if (ins.size() != 1) {
                break;
            }
            current = RelNodeUtils.unwrapHep(ins.get(0));
        }
        if (!(current instanceof OpenSearchJoin rootJoin)) {
            throw new IllegalStateException(
                "BroadcastDAGRewriter: expected OpenSearchJoin at root (after unwrapping Project/Sort/Filter), got "
                    + current.getClass().getSimpleName()
            );
        }
        List<Stage> inputChildren = inputRoot.getChildStages();
        if (inputChildren.size() != 2) {
            throw new IllegalStateException(
                "BroadcastDAGRewriter: expected exactly 2 child stages for binary join, got " + inputChildren.size()
            );
        }

        // Locate which child is BUILD vs PROBE. The advisor sets these tags; if they're missing
        // the caller routed a non-broadcast strategy here.
        Stage inputBuild = null;
        Stage inputProbe = null;
        int buildInputIndex = -1; // 0 = left input of rootJoin, 1 = right input
        for (int i = 0; i < inputChildren.size(); i++) {
            Stage c = inputChildren.get(i);
            if (c.getRole() == Stage.StageRole.BROADCAST_BUILD) {
                inputBuild = c;
                buildInputIndex = i;
            } else if (c.getRole() == Stage.StageRole.BROADCAST_PROBE) {
                inputProbe = c;
            }
        }
        if (inputBuild == null || inputProbe == null) {
            throw new IllegalStateException(
                "BroadcastDAGRewriter: both BROADCAST_BUILD and BROADCAST_PROBE child stages required (did the advisor run?)"
            );
        }

        // Allocate fresh stage ids bottom-up so PlanForker's bottom-up walk stays deterministic.
        int buildStageId = 0;
        int probeStageId = 1;
        int rootStageId = 2;

        // Build stage — identical to the input build child's fragment (a plain scan), new stage id.
        Stage newBuild = new Stage(
            buildStageId,
            inputBuild.getFragment(),
            List.of(),
            ExchangeInfo.singleton(),
            null,
            new ShardTargetResolver(inputBuild.getFragment(), clusterService)
        );
        newBuild.setRole(Stage.StageRole.BROADCAST_BUILD);

        // Probe stage — wraps the probe scan + OpenSearchBroadcastScan under a new OpenSearchJoin
        // mirroring the root join's condition, join type, and left/right ordering. Preserve
        // original ordering so condition's input refs resolve against the correct columns.
        RelNode probeScan = inputProbe.getFragment();
        OpenSearchBroadcastScan broadcastScan = new OpenSearchBroadcastScan(
            rootJoin.getCluster(),
            rootJoin.getTraitSet(),
            buildStageId,
            inputBuild.getFragment().getRowType(),
            rootJoin.getViableBackends()
        );

        RelNode probeJoinLeft;
        RelNode probeJoinRight;
        if (buildInputIndex == 0) {
            probeJoinLeft = broadcastScan;
            probeJoinRight = probeScan;
        } else {
            probeJoinLeft = probeScan;
            probeJoinRight = broadcastScan;
        }
        OpenSearchJoin probeJoin = new OpenSearchJoin(
            rootJoin.getCluster(),
            rootJoin.getTraitSet(),
            probeJoinLeft,
            probeJoinRight,
            rootJoin.getCondition(),
            rootJoin.getJoinType(),
            rootJoin.getViableBackends()
        );

        // The probe stage is a shard-fragment stage over the PROBE index. ShardTargetResolver
        // walks the fragment and picks the first OpenSearchTableScan it finds — the probe scan
        // (OpenSearchBroadcastScan extends AbstractRelNode, not TableScan, so it's invisible to
        // target resolution). No code change needed in ShardTargetResolver.
        Stage newProbe = new Stage(
            probeStageId,
            probeJoin,
            List.of(newBuild),
            ExchangeInfo.singleton(),
            null,
            new ShardTargetResolver(probeJoin, clusterService)
        );
        newProbe.setRole(Stage.StageRole.BROADCAST_PROBE);

        // Root stage — gathers joined rows from the probe stage. Construct an
        // ExchangeReducer over a StageInputScan placeholder, same shape DAGBuilder uses for
        // pure-gather root stages.
        OpenSearchStageInputScan rootStageInput = new OpenSearchStageInputScan(
            rootJoin.getCluster(),
            rootJoin.getTraitSet(),
            probeStageId,
            probeJoin.getRowType(),
            rootJoin.getViableBackends()
        );
        RelTraitSet rootTraits = rootStageInput.getTraitSet();
        RelNode rootBody = new OpenSearchExchangeReducer(
            rootJoin.getCluster(),
            rootTraits,
            rootStageInput,
            rootJoin.getViableBackends()
        );

        // Reapply the original root wrappers (Project / Sort / Filter) on top of the gathered
        // probe output, in their original outer-to-inner order, so the coordinator runs them
        // globally over all probe rows — the user's `| sort | head N` must still produce a
        // global top-N, not a per-shard top-N. Iterate from the innermost-captured wrapper
        // outward, calling Calcite's RelNode.copy(traitSet, inputs) which every subclass
        // implements to clone with new inputs while preserving the operator's own state.
        for (int i = rootWrappers.size() - 1; i >= 0; i--) {
            RelNode wrapper = rootWrappers.get(i);
            rootBody = wrapper.copy(wrapper.getTraitSet(), List.of(rootBody));
        }

        // Sink provider — pick from the first reduce-capable backend, mirroring DAGBuilder's
        // logic for multi-stage shapes.
        List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(registry, rootJoin.getViableBackends());
        if (reduceViable.isEmpty()) {
            throw new IllegalStateException(
                "BroadcastDAGRewriter: no reduce-capable backend among " + rootJoin.getViableBackends()
            );
        }
        ExchangeSinkProvider rootSinkProvider = registry.getBackend(reduceViable.getFirst()).getExchangeSinkProvider();

        Stage newRoot = new Stage(rootStageId, rootBody, List.of(newProbe), null, rootSinkProvider, null);
        newRoot.setRole(Stage.StageRole.COORDINATOR_REDUCE);

        QueryDAG rewritten = new QueryDAG(inputDag.queryId(), newRoot);

        // Re-run the plan-side pipeline on the rewritten DAG: forking and adapting.
        // {@code BackendPlanAdapter.adaptAll} now also drives partial/final aggregate
        // decomposition via {@code DistributedAggregateRewriter} (PR #21639 inlined what was
        // previously a separate {@code AggregateDecompositionResolver.resolveAll} pass).
        // {@link FragmentConversionDriver#convertAll} is not called here — callers invoke it
        // immediately after (see DefaultPlanExecutor.dispatchBroadcast). The split keeps the
        // rewriter testable against mock backends that don't ship a FragmentConvertor, while
        // the production dispatch path produces the full Substrait bytes on real backends.
        PlanForker.forkAll(rewritten, registry);
        BackendPlanAdapter.adaptAll(rewritten, registry);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[BroadcastDAGRewriter] rewritten DAG:\n{}", rewritten);
        }
        return rewritten;
    }
}
