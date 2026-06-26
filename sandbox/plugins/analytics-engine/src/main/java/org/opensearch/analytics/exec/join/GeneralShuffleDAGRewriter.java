/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.opensearch.analytics.exec.join.ShuffleEnrichment.WorkerLevel;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.PlanAlternativeSelector;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.WorkerTargetResolver;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * DAG rewriter for the GENERAL post-CBO scheduler (Option B — see {@code MPP-GENERAL-SCHEDULING-DESIGN.md}).
 * Promotes the join-over-two-shuffles stages of the DAG that {@link DistributionEnforcementPass} produces
 * into worker tiers.
 *
 * <p><b>In-place worker promotion.</b> The enforcement pass does binary-tier lowering: it shuffles every
 * distributed join input (even a co-partitioned one — the binary shuffle transport delivers exactly two
 * named inputs per worker), and it pre-splits a distributed aggregate into {@code FINAL(ER(PARTIAL(...)))}.
 * So by the time {@code DAGBuilder} cuts at the exchanges, EVERY shuffle-fed join is ALREADY its own stage
 * below a coordinator reduce (the root ER, or {@code FINAL_Agg(ER(...))}), with its {@code PARTIAL}
 * aggregate (if any) already sitting above it in the SAME stage fragment. Worker promotion is therefore
 * IN-PLACE — rebuild each such stage with a {@link WorkerTargetResolver} + worker sink + instruction-handler
 * factory, keeping its fragment and children unchanged — with no top-lift and no agg-split surgery. That
 * keeps the PARTIAL on the worker for free (it runs per-partition, ships {@code SINGLETON} to the FINAL).
 *
 * <p>Produces {@link ShuffleEnrichment.WorkerLevel} descriptors; {@link UnifiedDispatch} then calls
 * {@link ShuffleEnrichment#enrichLevels} to attach each level's shuffle producer/scan/worker instructions.
 *
 * <p><b>Outer joins.</b> Promotes a join of ANY type over two shuffles — the enforcement pass already
 * established that a hash-partitioned outer/semi/anti join's null-fill / existence test is partition-local
 * (standard Spark/Presto). Worker-side execution of non-INNER tiered hash joins is validated at sf=10.
 *
 * @opensearch.internal
 */
public final class GeneralShuffleDAGRewriter {

    private GeneralShuffleDAGRewriter() {}

    /** Supplies target node ids for a given worker level (one per partition). */
    @FunctionalInterface
    public interface NodeListResolver {
        List<String> resolve(int levelIndex, int partitionCount);
    }

    /** Result of {@link #rewriteStructure}: the rewritten DAG plus a deferred builder for the per-level
     *  worker descriptors (built after the caller runs the convert pipeline so non-top stages carry
     *  resolved plan alternatives). */
    public record Structure(QueryDAG dag, Supplier<List<WorkerLevel>> levelBuilder) {
        public List<WorkerLevel> buildLevels() {
            return levelBuilder.get();
        }
    }

    /** Full result of {@link #rewrite}: the rewritten DAG plus the per-level worker descriptors. */
    public record Rewritten(QueryDAG dag, List<WorkerLevel> levels) {
    }

    /**
     * True iff the (enforcement-pass) DAG has at least one join-over-two-shuffles stage — i.e. the pass
     * distributed at least one join, so this rewriter applies. A DAG the size-floor kept fully
     * coordinator-centric has none and is dispatched as a plain coordinator query.
     */
    public static boolean hasDistributedJoin(QueryDAG dag) {
        if (dag == null || dag.rootStage() == null) {
            return false;
        }
        List<Stage> stages = new ArrayList<>();
        collectJoinShuffleStages(dag.rootStage(), stages);
        return !stages.isEmpty();
    }

    /**
     * Promotes every join-over-two-shuffles stage in {@code dag} to a {@link Stage.StageRole#SHUFFLE_WORKER}
     * tier in place, rebuilds the DAG, re-runs the full {@code forkAll → adaptAll → selectAll → convertAll}
     * pipeline, and returns the worker levels for instruction enrichment.
     */
    public static Rewritten rewrite(
        QueryDAG dag,
        CapabilityRegistry registry,
        boolean preferMetadataDriver,
        NodeListResolver nodeResolver
    ) {
        Structure structure = rewriteStructure(dag, registry, nodeResolver);
        QueryDAG rewrittenDag = structure.dag();
        // forkAll re-expands alternatives from each fragment (discarding prior selection); selectAll's
        // parent-backend constraint MUST re-run or a child keeps a stray backend (the "DAG rewriters must
        // re-run the FULL pipeline" gotcha). adaptAll re-applies scalar-function adaptation forkAll wiped.
        PlanForker.forkAll(rewrittenDag, registry);
        BackendPlanAdapter.adaptAll(rewrittenDag, registry);
        PlanAlternativeSelector.selectAll(rewrittenDag, registry, preferMetadataDriver);
        FragmentConversionDriver.convertAll(rewrittenDag, registry);
        return new Rewritten(rewrittenDag, structure.buildLevels());
    }

    /**
     * The structural half of {@link #rewrite}: in-place worker promotion + DAG rebuild WITHOUT the convert
     * pipeline. Returns the new DAG plus a deferred level builder. Split out so unit tests can validate the
     * worker-tier shape against a mock backend that has no fragment convertor.
     */
    public static Structure rewriteStructure(QueryDAG dag, CapabilityRegistry registry, NodeListResolver nodeResolver) {
        Stage root = dag.rootStage();
        // Collect every join-shuffle stage top-down; the order is irrelevant for in-place conversion but we
        // index levels bottom-up so the node resolver can hand a distinct list per tier.
        List<Stage> joinShuffleStages = new ArrayList<>();
        collectJoinShuffleStages(root, joinShuffleStages);
        if (joinShuffleStages.isEmpty()) {
            throw new IllegalStateException("GeneralShuffleDAGRewriter: no join-shuffle stage found in DAG");
        }

        // Per-stage join analysis (keys + producer child ids + partition count), resolved once.
        Map<Integer, JoinShuffleInfo> descriptors = new HashMap<>();
        for (Stage s : joinShuffleStages) {
            descriptors.put(s.getStageId(), analyze(s));
        }

        // Bottom-up level index (deepest = level 0). joinShuffleStages is top-down (collect visits parent
        // before children), so reverse for bottom-up indexing.
        List<Stage> bottomUp = new ArrayList<>(joinShuffleStages);
        Collections.reverse(bottomUp);
        Map<Integer, Integer> levelIndex = new HashMap<>();
        for (int i = 0; i < bottomUp.size(); i++) {
            levelIndex.put(bottomUp.get(i).getStageId(), i);
        }

        // Resolve each level's target node list EXACTLY ONCE, keyed by stage id, and reuse it for BOTH the
        // worker's WorkerTargetResolver AND the producer instructions' targets — resolving twice risks the
        // two diverging on a cluster-state change (the "resolve once" rule).
        Map<Integer, List<String>> nodesByStageId = new HashMap<>();
        for (Stage s : joinShuffleStages) {
            JoinShuffleInfo d = descriptors.get(s.getStageId());
            nodesByStageId.put(s.getStageId(), nodeResolver.resolve(levelIndex.get(s.getStageId()), d.partitionCount()));
        }

        AnalyticsSearchBackendPlugin backend = registry.getBackend(workerBackendId(joinShuffleStages.get(0)));

        Set<Integer> workerIds = descriptors.keySet();
        Stage newRoot = rebuild(root, workerIds, nodesByStageId, backend);
        QueryDAG rewrittenDag = new QueryDAG(dag.queryId(), newRoot);

        // Deferred level builder — run after the convert pipeline re-creates non-worker stages. Producers
        // are looked up from the rewritten DAG by stage id; an intermediate-worker producer keeps its
        // original id, so the lookup returns its worker version.
        final Map<Integer, JoinShuffleInfo> descRef = descriptors;
        final Map<Integer, List<String>> nodesRef = nodesByStageId;
        final List<Stage> bottomUpRef = bottomUp;
        return new Structure(rewrittenDag, () -> {
            Map<Integer, Stage> rewrittenById = new HashMap<>();
            indexStages(rewrittenDag.rootStage(), rewrittenById);
            List<WorkerLevel> levels = new ArrayList<>();
            for (Stage s : bottomUpRef) {
                JoinShuffleInfo d = descRef.get(s.getStageId());
                Stage worker = rewrittenById.get(s.getStageId());
                Stage leftProducer = rewrittenById.get(d.leftProducerId());
                Stage rightProducer = rewrittenById.get(d.rightProducerId());
                List<String> nodes = nodesRef.get(s.getStageId());
                levels.add(new WorkerLevel(worker, leftProducer, rightProducer, d.leftKeys(), d.rightKeys(), d.partitionCount(), nodes));
            }
            return levels;
        });
    }

    /**
     * Rebuilds {@code stage}'s subtree bottom-up. A join-shuffle stage (id in {@code workerIds}) is rebuilt
     * as a {@link Stage.StageRole#SHUFFLE_WORKER} with a {@link WorkerTargetResolver} — keeping its fragment
     * (the join, plus any PARTIAL aggregate above it) and its children (the producers). Every other stage is
     * copied with rebuilt children, preserving its role / exchange info / sink / resolver. Worker stages get
     * their plan alternatives re-forked by the convert pipeline, so we leave them empty; pass-through stages
     * keep their alternatives (the pipeline re-forks everything anyway, but this matches the cascade
     * rewriter's conservatism).
     */
    private static Stage rebuild(
        Stage stage,
        Set<Integer> workerIds,
        Map<Integer, List<String>> nodesByStageId,
        AnalyticsSearchBackendPlugin backend
    ) {
        List<Stage> rebuiltChildren = new ArrayList<>(stage.getChildStages().size());
        for (Stage child : stage.getChildStages()) {
            rebuiltChildren.add(rebuild(child, workerIds, nodesByStageId, backend));
        }
        if (workerIds.contains(stage.getStageId())) {
            Stage worker = new Stage(
                stage.getStageId(),
                stage.getFragment(),
                rebuiltChildren,
                // Vestigial: the worker SHIPS via its appended ShuffleProducer instruction (intermediate
                // tier) or via the worker sink to the coordinator (top tier); the exchange info itself does
                // not drive shipping. SINGLETON matches the cascade rewriter's intermediate-worker choice.
                ExchangeInfo.singleton(),
                backend.getExchangeSinkProvider(),
                new WorkerTargetResolver(nodesByStageId.get(stage.getStageId()))
            );
            worker.setRole(Stage.StageRole.SHUFFLE_WORKER);
            worker.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());
            return worker;
        }
        Stage copy = new Stage(
            stage.getStageId(),
            stage.getFragment(),
            rebuiltChildren,
            stage.getExchangeInfo(),
            stage.getExchangeSinkProvider(),
            stage.getTargetResolver()
        );
        copy.setRole(stage.getRole());
        copy.setPlanAlternatives(stage.getPlanAlternatives());
        if (stage.getInstructionHandlerFactory() != null) {
            copy.setInstructionHandlerFactory(stage.getInstructionHandlerFactory());
        }
        return copy;
    }

    /** Per-join-shuffle-stage analysis: producer child stage ids + per-side hash keys + partition count. */
    private record JoinShuffleInfo(int leftProducerId, int rightProducerId, List<Integer> leftKeys, List<Integer> rightKeys,
        int partitionCount) {
    }

    /**
     * Extracts the join's two shuffle inputs' producer stage ids + keys. Defense-in-depth: each side's
     * shuffle must be partitioned on THIS join's equi keys, and both sides must agree on the partition count
     * — the enforcement pass guarantees this (it builds each shuffle from the join's per-side keys), but a
     * future DAG-shape change must not silently promote a mis-keyed shuffle into a worker.
     */
    private static JoinShuffleInfo analyze(Stage stage) {
        OpenSearchJoin join = findJoinOverTwoShuffles(stage.getFragment());
        if (join == null) {
            throw new IllegalStateException("GeneralShuffleDAGRewriter: stage " + stage.getStageId() + " has no join over two shuffles");
        }
        OpenSearchShuffleExchange leftShuffle = asShuffle(join.getInput(0), stage.getStageId(), "left");
        OpenSearchShuffleExchange rightShuffle = asShuffle(join.getInput(1), stage.getStageId(), "right");
        JoinInfo info = join.analyzeCondition();
        if (!leftShuffle.getHashKeys().equals(info.leftKeys) || !rightShuffle.getHashKeys().equals(info.rightKeys)) {
            throw new IllegalStateException(
                "GeneralShuffleDAGRewriter: stage "
                    + stage.getStageId()
                    + " shuffle keys do not match join equi keys (leftShuffle="
                    + leftShuffle.getHashKeys()
                    + " leftJoin="
                    + info.leftKeys
                    + ", rightShuffle="
                    + rightShuffle.getHashKeys()
                    + " rightJoin="
                    + info.rightKeys
                    + ")"
            );
        }
        if (leftShuffle.getPartitionCount() != rightShuffle.getPartitionCount()) {
            throw new IllegalStateException(
                "GeneralShuffleDAGRewriter: stage "
                    + stage.getStageId()
                    + " left/right shuffle partition counts disagree ("
                    + leftShuffle.getPartitionCount()
                    + " vs "
                    + rightShuffle.getPartitionCount()
                    + ")"
            );
        }
        int leftId = childStageId(leftShuffle);
        int rightId = childStageId(rightShuffle);
        // The binary shuffle transport gives each worker TWO independent producer streams (one sink per
        // side). The two shuffle inputs MUST therefore be distinct producer stages — DAGBuilder.cutShuffle
        // mints a fresh stage id per shuffle-input cut, so even a self-join (a ⋈ a) yields two stages. A
        // shared id would make enrichLevels enrich one producer as BOTH "left" and "right" against a single
        // sink → the worker's awaitReady never completes for one side → hang. Fail loud (tripwire) rather
        // than hang if a future DAG-shape change ever collapses them. (codex round-3 review.)
        if (leftId == rightId) {
            throw new IllegalStateException(
                "GeneralShuffleDAGRewriter: stage "
                    + stage.getStageId()
                    + " join's two shuffle inputs resolve to the SAME producer stage "
                    + leftId
                    + " — the binary shuffle transport requires two distinct producer stages per worker (one sink per side)."
            );
        }
        return new JoinShuffleInfo(leftId, rightId, leftShuffle.getHashKeys(), rightShuffle.getHashKeys(), leftShuffle.getPartitionCount());
    }

    private static OpenSearchShuffleExchange asShuffle(RelNode input, int stageId, String side) {
        RelNode n = RelNodeUtils.unwrapHep(input);
        if (n instanceof OpenSearchShuffleExchange shuffle) {
            return shuffle;
        }
        throw new IllegalStateException(
            "GeneralShuffleDAGRewriter: stage " + stageId + " join " + side + " input is not a shuffle: " + n.getRelTypeName()
        );
    }

    private static int childStageId(OpenSearchShuffleExchange shuffle) {
        RelNode inner = RelNodeUtils.unwrapHep(shuffle.getInput());
        if (inner instanceof OpenSearchStageInputScan sis) {
            return sis.getChildStageId();
        }
        throw new IllegalStateException("GeneralShuffleDAGRewriter: shuffle input is not a StageInputScan: " + inner.getRelTypeName());
    }

    /** Collects join-shuffle stages top-down (parent before child) — a stage whose fragment contains a join
     *  over two {@link OpenSearchShuffleExchange} inputs. */
    private static void collectJoinShuffleStages(Stage stage, List<Stage> out) {
        if (stage == null) {
            return;
        }
        if (findJoinOverTwoShuffles(stage.getFragment()) != null) {
            out.add(stage);
        }
        for (Stage child : stage.getChildStages()) {
            collectJoinShuffleStages(child, out);
        }
    }

    /**
     * Returns the UNIQUE {@link OpenSearchJoin} in {@code fragment} whose two inputs are both
     * {@link OpenSearchShuffleExchange}, or {@code null} if there is none / more than one. Accepts ANY join
     * type (the enforcement pass co-partitions outer/semi/anti too). A join-over-two-shuffles' inputs are
     * StageInputScan leaves (lower tiers are separate stages), so there is at most one such join per
     * fragment in the enforced DAG; the uniqueness guard rejects a malformed multi-join fragment rather than
     * silently promoting one arbitrarily.
     */
    private static OpenSearchJoin findJoinOverTwoShuffles(RelNode fragment) {
        if (fragment == null) {
            return null;
        }
        Set<OpenSearchJoin> found = Collections.newSetFromMap(new IdentityHashMap<>());
        collect(RelNodeUtils.unwrapHep(fragment), found);
        return found.size() == 1 ? found.iterator().next() : null;
    }

    private static void collect(RelNode node, Set<OpenSearchJoin> out) {
        if (node instanceof OpenSearchJoin join
            && RelNodeUtils.unwrapHep(join.getInput(0)) instanceof OpenSearchShuffleExchange
            && RelNodeUtils.unwrapHep(join.getInput(1)) instanceof OpenSearchShuffleExchange) {
            out.add(join);
            // The shuffle inputs are StageInputScan leaves — no deeper join-over-two-shuffles in THIS
            // fragment (lower tiers are separate child stages).
            return;
        }
        for (RelNode input : node.getInputs()) {
            collect(RelNodeUtils.unwrapHep(input), out);
        }
    }

    /** Backend id to drive worker promotion + the convert pipeline. Reads the first join-shuffle stage's
     *  resolved alternative (the DAG was forked before dispatch). */
    private static String workerBackendId(Stage joinShuffleStage) {
        if (joinShuffleStage.getPlanAlternatives().isEmpty()) {
            throw new IllegalStateException(
                "GeneralShuffleDAGRewriter: join-shuffle stage "
                    + joinShuffleStage.getStageId()
                    + " has no plan alternatives (the DAG must be forked/selected before rewrite)"
            );
        }
        return joinShuffleStage.getPlanAlternatives().getFirst().backendId();
    }

    private static void indexStages(Stage stage, Map<Integer, Stage> out) {
        if (stage == null) {
            return;
        }
        out.put(stage.getStageId(), stage);
        for (Stage child : stage.getChildStages()) {
            indexStages(child, out);
        }
    }
}
