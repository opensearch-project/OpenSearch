/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.opensearch.analytics.exec.join.DistributionEnforcementPass;
import org.opensearch.analytics.exec.join.ShuffleEnrichment;
import org.opensearch.analytics.exec.join.ShuffleEnrichment.WorkerInput;
import org.opensearch.analytics.exec.join.ShuffleEnrichment.WorkerLevel;
import org.opensearch.analytics.exec.join.UnifiedDispatch;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.ShuffleSlots;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
                List<String> nodes = nodesRef.get(s.getStageId());
                // One worker slot per shuffle input, in the join tree's left-to-right leaf order — so a
                // binary join keeps the historical left/right labels (see ShuffleSlots.forInput).
                List<WorkerInput> inputs = new ArrayList<>(d.inputs().size());
                int arity = d.inputs().size();
                for (int i = 0; i < arity; i++) {
                    ShuffleInput in = d.inputs().get(i);
                    inputs.add(new WorkerInput(rewrittenById.get(in.producerStageId()), in.hashKeys(), ShuffleSlots.forInput(i, arity)));
                }
                levels.add(new WorkerLevel(worker, inputs, d.partitionCount(), nodes));
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

    /** Per-join-shuffle-stage analysis: one {@link ShuffleInput} per shuffle-fed join input, plus the
     *  common partition count. */
    private record JoinShuffleInfo(List<ShuffleInput> inputs, int partitionCount) {
    }

    /** One shuffle-fed input of a promoted join: its producer child stage id and its hash keys. */
    private record ShuffleInput(int producerStageId, List<Integer> hashKeys) {
    }

    /**
     * Extracts each shuffle input's producer stage id + keys. Defense-in-depth: every input's shuffle
     * must be partitioned on THIS join's equi keys for that input, and all inputs must agree on the
     * partition count — the enforcement pass guarantees this (it builds each shuffle from the join's
     * per-input keys), but a future DAG-shape change must not silently promote a mis-keyed shuffle into
     * a worker.
     */
    private static JoinShuffleInfo analyze(Stage stage) {
        RelNode consumer = findShuffleConsumer(stage.getFragment());
        if (consumer == null) {
            throw new IllegalStateException("GeneralShuffleDAGRewriter: stage " + stage.getStageId() + " has no shuffle consumer");
        }
        if (consumer instanceof OpenSearchAggregate agg) {
            return analyzeAggregate(stage, agg);
        }
        OpenSearchJoin join = (OpenSearchJoin) consumer;
        // Collect the shuffle leaves of this stage's join TREE, left-to-right. A binary join contributes
        // two; a collapsed N-way tree (Join(Shuffle, Join(Shuffle, Shuffle)) in one fragment) contributes
        // one per leaf, each becoming its own worker slot.
        List<OpenSearchShuffleExchange> shuffles = new ArrayList<>();
        collectShuffleLeaves(join, shuffles, stage.getStageId());
        List<ShuffleInput> inputs = new ArrayList<>(shuffles.size());
        int partitionCount = -1;
        // Every equi key a join in this tree partitions on, so a leaf's keys can be checked against the
        // set the tree actually demands. Position-wise checking doesn't generalize: in a collapsed tree a
        // leaf feeds an INNER join whose own left/right key lists are relative to that join, not the root.
        Set<List<Integer>> treeKeys = new HashSet<>();
        collectJoinKeys(join, treeKeys);
        for (int i = 0; i < shuffles.size(); i++) {
            OpenSearchShuffleExchange shuffle = shuffles.get(i);
            if (!treeKeys.contains(shuffle.getHashKeys())) {
                throw new IllegalStateException(
                    "GeneralShuffleDAGRewriter: stage "
                        + stage.getStageId()
                        + " input "
                        + i
                        + " shuffle keys "
                        + shuffle.getHashKeys()
                        + " match none of the join tree's equi keys "
                        + treeKeys
                );
            }
            if (partitionCount < 0) {
                partitionCount = shuffle.getPartitionCount();
            } else if (partitionCount != shuffle.getPartitionCount()) {
                throw new IllegalStateException(
                    "GeneralShuffleDAGRewriter: stage "
                        + stage.getStageId()
                        + " shuffle partition counts disagree ("
                        + partitionCount
                        + " vs "
                        + shuffle.getPartitionCount()
                        + " on input "
                        + i
                        + ")"
                );
            }
            inputs.add(new ShuffleInput(childStageId(shuffle), shuffle.getHashKeys()));
        }
        // Each worker input is an INDEPENDENT producer stream (one sink per slot), so the shuffle inputs
        // MUST resolve to distinct producer stages — DAGBuilder.cutShuffle mints a fresh stage id per
        // shuffle-input cut, so even a self-join (a ⋈ a) yields two stages. A shared id would make
        // enrichLevels enrich one producer under two slots against a single sink → the worker's awaitReady
        // never completes for one slot → hang. Fail loud (tripwire) rather than hang if a future DAG-shape
        // change ever collapses them. (codex round-3 review.)
        Set<Integer> distinctProducers = new HashSet<>();
        for (ShuffleInput in : inputs) {
            if (!distinctProducers.add(in.producerStageId())) {
                throw new IllegalStateException(
                    "GeneralShuffleDAGRewriter: stage "
                        + stage.getStageId()
                        + " join has two shuffle inputs resolving to the SAME producer stage "
                        + in.producerStageId()
                        + " — the shuffle transport requires one distinct producer stage per worker slot."
                );
            }
        }
        return new JoinShuffleInfo(inputs, partitionCount);
    }

    /**
     * The arity-1 counterpart of {@link #analyze}: a FINAL aggregate over a single group-key shuffle becomes a
     * one-slot worker tier. The shuffle MUST be partitioned on exactly the group keys at their PARTIAL-output
     * positions {@code [0..groupCount)} — a per-partition merge is only complete when every partial of a group
     * lands in one partition, and unlike the join case there is no second input whose keys could disambiguate.
     * An empty group set has no key to hash on and can never reach here.
     */
    private static JoinShuffleInfo analyzeAggregate(Stage stage, OpenSearchAggregate agg) {
        OpenSearchShuffleExchange shuffle = aggregateShuffleInput(agg);
        int groupCount = agg.getGroupSet().cardinality();
        if (groupCount == 0) {
            throw new IllegalStateException(
                "GeneralShuffleDAGRewriter: stage "
                    + stage.getStageId()
                    + " aggregate over a shuffle has an EMPTY group set — there is no key to partition on, so a"
                    + " per-partition merge would split the single group across workers"
            );
        }
        List<Integer> expectedKeys = new ArrayList<>(groupCount);
        for (int i = 0; i < groupCount; i++) {
            expectedKeys.add(i);
        }
        if (!expectedKeys.equals(shuffle.getHashKeys())) {
            throw new IllegalStateException(
                "GeneralShuffleDAGRewriter: stage "
                    + stage.getStageId()
                    + " aggregate expects its shuffle keyed on the fronted group keys "
                    + expectedKeys
                    + " but it is keyed on "
                    + shuffle.getHashKeys()
                    + " — a per-partition merge requires every partial of a group in one partition"
            );
        }
        return new JoinShuffleInfo(List.of(new ShuffleInput(childStageId(shuffle), shuffle.getHashKeys())), shuffle.getPartitionCount());
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

    /**
     * Collects the {@link OpenSearchShuffleExchange} leaves of a join tree, left-to-right. A nested
     * {@link OpenSearchJoin} input is recursed into (that is the collapsed N-way shape); anything else
     * must be a shuffle, else {@link #asShuffle} fails loud.
     */
    private static void collectShuffleLeaves(OpenSearchJoin join, List<OpenSearchShuffleExchange> out, int stageId) {
        for (int i = 0; i < join.getInputs().size(); i++) {
            RelNode input = RelNodeUtils.unwrapHep(join.getInput(i));
            if (input instanceof OpenSearchJoin nested) {
                collectShuffleLeaves(nested, out, stageId);
            } else {
                out.add(asShuffle(input, stageId, "input " + i));
            }
        }
    }

    /** Collects every per-side equi-key list of every join in the tree rooted at {@code join}. */
    private static void collectJoinKeys(OpenSearchJoin join, Set<List<Integer>> out) {
        JoinInfo info = join.analyzeCondition();
        out.add(info.leftKeys);
        out.add(info.rightKeys);
        for (RelNode input : join.getInputs()) {
            if (RelNodeUtils.unwrapHep(input) instanceof OpenSearchJoin nested) {
                collectJoinKeys(nested, out);
            }
        }
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
        if (findShuffleConsumer(stage.getFragment()) != null) {
            out.add(stage);
        }
        for (Stage child : stage.getChildStages()) {
            collectJoinShuffleStages(child, out);
        }
    }

    /**
     * Returns the UNIQUE ROOT {@link OpenSearchJoin} in {@code fragment} whose every leaf input is an
     * {@link OpenSearchShuffleExchange}, or {@code null} if there is none / more than one. Accepts ANY
     * join type (the enforcement pass co-partitions outer/semi/anti too) and any arity: a binary join
     * over two shuffles, or a COLLAPSED N-way tree in one fragment
     * ({@code Join(Shuffle, Join(Shuffle, Shuffle))}) whose leaves each become a worker slot.
     *
     * <p>Only the OUTERMOST qualifying join is returned — a nested join inside the tree is part of the
     * same worker fragment, not a separate promotion. Each shuffle leaf's input is a StageInputScan
     * (lower tiers are separate stages), so a fragment has at most one such tree in the enforced DAG;
     * the uniqueness guard rejects a malformed multi-tree fragment rather than promoting one arbitrarily.
     */
    private static RelNode findShuffleConsumer(RelNode fragment) {
        if (fragment == null) {
            return null;
        }
        Set<RelNode> found = Collections.newSetFromMap(new IdentityHashMap<>());
        collect(RelNodeUtils.unwrapHep(fragment), found);
        return found.size() == 1 ? found.iterator().next() : null;
    }

    private static void collect(RelNode node, Set<RelNode> out) {
        if (node instanceof OpenSearchJoin join && allLeavesAreShuffles(join)) {
            out.add(join);
            // Do NOT descend: a nested join below belongs to THIS tree (one worker fragment), and the
            // shuffle leaves' own inputs are StageInputScans whose lower tiers are separate child stages.
            return;
        }
        if (node instanceof OpenSearchAggregate agg && aggregateShuffleInput(agg) != null) {
            out.add(agg);
            return;
        }
        for (RelNode input : node.getInputs()) {
            collect(RelNodeUtils.unwrapHep(input), out);
        }
    }

    /**
     * The shuffle feeding {@code agg} directly, or {@code null} if its input is anything else. This is the
     * single-input shuffle edge that {@code analytics.mpp.aggregate.group_key_shuffle} produces: a FINAL
     * aggregate whose PARTIAL was shuffled on the group keys. Requiring a DIRECT shuffle input mirrors the
     * join case, whose leaves must be shuffles rather than shuffles-under-a-Project.
     *
     * <p>An aggregate ABOVE a join-over-shuffles is NOT one of these — its input is the join, so the walk
     * descends past it and promotes the join instead, leaving the aggregate riding that worker (the q5/q10
     * PARTIAL-on-the-join-worker shape).
     */
    private static OpenSearchShuffleExchange aggregateShuffleInput(OpenSearchAggregate agg) {
        if (agg.getInputs().size() != 1) {
            return null;
        }
        RelNode input = RelNodeUtils.unwrapHep(agg.getInput(0));
        return input instanceof OpenSearchShuffleExchange shuffle ? shuffle : null;
    }

    /** True if every leaf of {@code join}'s tree (recursing through nested joins) is a shuffle exchange. */
    private static boolean allLeavesAreShuffles(OpenSearchJoin join) {
        for (RelNode input : join.getInputs()) {
            RelNode n = RelNodeUtils.unwrapHep(input);
            if (n instanceof OpenSearchJoin nested) {
                if (!allLeavesAreShuffles(nested)) {
                    return false;
                }
            } else if (!(n instanceof OpenSearchShuffleExchange)) {
                return false;
            }
        }
        return true;
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
