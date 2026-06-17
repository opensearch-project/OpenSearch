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
import org.apache.calcite.rel.core.JoinRelType;
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
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Recursive sibling of {@link HashShuffleDAGRewriter}. Handles the cascaded hash-shuffle shape that
 * {@link CascadeShufflePlanRewriter} + {@code DAGBuilder} produce for a multi-way join: every join
 * level sits over {@link OpenSearchShuffleExchange} inputs, so the DAG is a chain of join-bearing
 * stages, not just one.
 *
 * <pre>
 *   Stage R (root reduce): Sort/Agg/ OuterJoin( Shuffle(SIS→I), Shuffle(SIS→C) )
 *     Stage I (intermediate): InnerJoin( Shuffle(SIS→A), Shuffle(SIS→B) )
 *       Stage A / Stage B (leaf shard scans)
 *     Stage C (leaf shard scan)
 * </pre>
 *
 * <p>The rewrite lifts each join into a data-node worker tier:
 * <ul>
 *   <li><b>Topmost join</b> (in the root stage, possibly under Sort/Agg/Project): lifted into a NEW
 *       {@link Stage.StageRole#SHUFFLE_WORKER} stage so the Sort/Agg/Project stays on the
 *       coordinator. The root becomes a reduce over that new worker — identical to the single-level
 *       {@link HashShuffleDAGRewriter}.</li>
 *   <li><b>Intermediate joins</b> (their stage's fragment root IS the join, since DAGBuilder cut at
 *       the shuffle above them): converted IN-PLACE into a {@code SHUFFLE_WORKER}. The stage keeps
 *       its id and fragment but gains a {@link WorkerTargetResolver}; at execution it CONSUMES its
 *       children's shuffle (ShuffleScan instructions) AND PRODUCES a shuffle to its parent worker
 *       (a ShuffleProducer instruction). This consume-and-produce worker is exactly Presto's
 *       intermediate fragment (RemoteSource leaves + partitioned output root).</li>
 *   <li><b>Leaf scans</b>: unchanged shard stages; they become shuffle producers via instruction
 *       enrichment (same as the single-level path).</li>
 * </ul>
 *
 * <p>This rewriter performs only the STRUCTURAL rewrite (insert/convert worker stages, rebuild the
 * DAG) and re-runs the full {@code forkAll → adaptAll → selectAll → convertAll} pipeline once.
 * Instruction enrichment (which would be wiped by {@code forkAll}) is the dispatcher's job and runs
 * after this returns — see {@link CascadeShuffleDispatch}.
 *
 * @opensearch.internal
 */
public final class CascadeShuffleDAGRewriter {

    private CascadeShuffleDAGRewriter() {}

    /**
     * True iff the DAG is a genuine cascade — more than one stage whose fragment is a join over two
     * shuffles. A single 2-way hash-shuffle join has exactly one such stage (its consumer/root) and
     * is handled by the simpler {@link HashShuffleDispatch}; {@code DefaultPlanExecutor} routes here
     * only when this returns {@code true}.
     */
    public static boolean isCascade(QueryDAG dag) {
        if (dag == null || dag.rootStage() == null) {
            return false;
        }
        List<Stage> joinShuffleStages = new ArrayList<>();
        collectJoinShuffleStages(dag.rootStage(), joinShuffleStages);
        return joinShuffleStages.size() > 1;
    }

    /** One shuffle edge {@code producer → consumerWorker}, carrying the partition keys and side. */
    public record ShuffleEdge(Stage producer, int consumerWorkerStageId, List<Integer> hashKeys, String side) {
    }

    /** A worker tier: the stage that runs a join on data nodes, plus its left/right producer stages
     *  and the keys each producer partitions on. */
    public record WorkerLevel(Stage worker, Stage leftProducer, Stage rightProducer, List<Integer> leftKeys, List<Integer> rightKeys,
        int partitionCount, List<String> targetNodeIds) {
    }

    /** Result of the rewrite: the new DAG plus one {@link WorkerLevel} per join level (bottom-up). */
    public record Rewritten(QueryDAG dag, List<WorkerLevel> levels) {
    }

    /**
     * Rewrites {@code dag} into the cascaded worker-tier shape. {@code nodeListPerLevel} supplies the
     * target node ids for each worker level (one list per join level, each of length
     * {@code partitionCount}); the caller resolves these from the cluster topology.
     *
     * @param dag            the cascade-shaped DAG (every join level over shuffle inputs)
     * @param registry       capability registry, for the worker backend + the fork/convert pipeline
     * @param preferMetadataDriver passed to {@link PlanAlternativeSelector#selectAll}
     * @param nodeResolver   maps a worker's 0-based level index → its target node id list
     * @return the rewritten DAG and the per-level worker descriptors for instruction enrichment
     */
    public static Rewritten rewrite(
        QueryDAG dag,
        CapabilityRegistry registry,
        boolean preferMetadataDriver,
        NodeListResolver nodeResolver
    ) {
        Structure structure = rewriteStructure(dag, registry, nodeResolver);
        QueryDAG rewrittenDag = structure.dag();
        // Full plan pipeline, mirroring DefaultPlanExecutor (forkAll re-expands alternatives from
        // fragments — selectAll's parent-backend constraint must re-run or a child keeps a stray
        // backend; see the "DAG rewriters must re-run the FULL pipeline" gotcha).
        PlanForker.forkAll(rewrittenDag, registry);
        BackendPlanAdapter.adaptAll(rewrittenDag, registry);
        PlanAlternativeSelector.selectAll(rewrittenDag, registry, preferMetadataDriver);
        FragmentConversionDriver.convertAll(rewrittenDag, registry);
        return new Rewritten(rewrittenDag, structure.buildLevels());
    }

    /**
     * The structural half of {@link #rewrite}: performs the worker-tier surgery (insert top worker,
     * convert intermediates in place, rebuild the DAG) WITHOUT running the {@code forkAll → convertAll}
     * pipeline. Returns the new DAG plus a {@link Structure} that can build the {@link WorkerLevel}
     * descriptors once alternatives are resolved. Split out so unit tests can validate the cascade
     * shape against a mock backend that has no fragment convertor.
     */
    public static Structure rewriteStructure(QueryDAG dag, CapabilityRegistry registry, NodeListResolver nodeResolver) {
        Stage root = dag.rootStage();
        // Collect every join-shuffle stage (fragment has a join sitting over shuffle inputs),
        // top-down. The first is the topmost join; the rest are intermediate.
        List<Stage> joinShuffleStages = new ArrayList<>();
        collectJoinShuffleStages(root, joinShuffleStages);
        if (joinShuffleStages.isEmpty()) {
            throw new IllegalStateException("CascadeShuffleDAGRewriter: no join-shuffle stage found in DAG");
        }

        // Map old stage id → stage for child lookups.
        Map<Integer, Stage> byId = new HashMap<>();
        indexStages(root, byId);

        // The topmost join-shuffle stage holds the join we lift into a NEW worker.
        Stage topStage = joinShuffleStages.get(0);
        // All other join-shuffle stages are intermediate → converted in place to workers.
        // Build worker versions for the intermediates first so the rebuild references them.
        Map<Integer, Stage> intermediateWorkers = new HashMap<>();
        int[] nextId = { nextStageId(dag) };

        // Resolve the join descriptor for every join-shuffle stage up front (keys + child ids).
        Map<Integer, JoinShuffle> descriptors = new HashMap<>();
        for (Stage s : joinShuffleStages) {
            descriptors.put(s.getStageId(), analyzeJoinShuffle(s));
        }

        // Assign a 0-based level index bottom-up (deepest intermediate = level 0, top = last) so the
        // caller's nodeResolver can hand out a distinct node list per tier.
        // joinShuffleStages is top-down, so reverse for bottom-up indexing.
        List<Stage> bottomUp = new ArrayList<>(joinShuffleStages);
        Collections.reverse(bottomUp);
        Map<Integer, Integer> levelIndex = new HashMap<>();
        for (int i = 0; i < bottomUp.size(); i++) {
            levelIndex.put(bottomUp.get(i).getStageId(), i);
        }

        AnalyticsSearchBackendPlugin backend = registry.getBackend(topStage.getPlanAlternatives().getFirst().backendId());

        // Resolve each level's target node list EXACTLY ONCE, keyed by stage id. The same list must
        // drive BOTH the worker's WorkerTargetResolver (which task runs partition p) AND the producer
        // instructions' targetNodeIds (where partition p is shipped) — resolving twice risks the two
        // diverging if cluster membership / data-node iteration order changes between calls, sending
        // partition p to a node where no worker task consumes it → shuffle timeout. (codex review R6)
        Map<Integer, List<String>> nodesByStageId = new HashMap<>();
        for (Stage s : joinShuffleStages) {
            JoinShuffle d = descriptors.get(s.getStageId());
            nodesByStageId.put(s.getStageId(), nodeResolver.resolve(levelIndex.get(s.getStageId()), d.partitionCount()));
        }

        // Convert each intermediate join-shuffle stage in place to a worker.
        for (Stage s : joinShuffleStages) {
            if (s.getStageId() == topStage.getStageId()) {
                continue;
            }
            List<String> nodes = nodesByStageId.get(s.getStageId());
            Stage worker = new Stage(
                s.getStageId(),
                s.getFragment(),
                s.getChildStages(),
                ExchangeInfo.singleton(),
                backend.getExchangeSinkProvider(),
                new WorkerTargetResolver(nodes)
            );
            worker.setRole(Stage.StageRole.SHUFFLE_WORKER);
            worker.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());
            intermediateWorkers.put(s.getStageId(), worker);
        }

        // Build the new top worker that runs the topmost join, with the top stage's shuffle-children
        // (rebuilt so intermediate ones are their worker versions) as its producers.
        JoinShuffle topDesc = descriptors.get(topStage.getStageId());
        int topWorkerId = nextId[0]++;
        List<String> topNodes = nodesByStageId.get(topStage.getStageId());
        Stage topLeftChild = rebuild(byId.get(topDesc.leftChildId()), topStage.getStageId(), null, intermediateWorkers, byId);
        Stage topRightChild = rebuild(byId.get(topDesc.rightChildId()), topStage.getStageId(), null, intermediateWorkers, byId);
        Stage topWorker = new Stage(
            topWorkerId,
            topDesc.join(),
            List.of(topLeftChild, topRightChild),
            ExchangeInfo.singleton(),
            backend.getExchangeSinkProvider(),
            new WorkerTargetResolver(topNodes)
        );
        topWorker.setRole(Stage.StageRole.SHUFFLE_WORKER);
        topWorker.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());

        // Rebuild the DAG from the root: at the top join-shuffle stage, replace the join with a
        // StageInputScan(topWorkerId) and make topWorker its only child; elsewhere copy through.
        Stage newRoot = rebuild(root, topStage.getStageId(), topWorker, intermediateWorkers, byId);
        QueryDAG rewrittenDag = new QueryDAG(dag.queryId(), newRoot);

        // Capture everything the deferred level builder needs. Levels are built AFTER the caller runs
        // the convert pipeline (which re-creates non-top stages), so producers are looked up from the
        // rewritten DAG by stage id at build time. The top worker is a fresh object that survives the
        // pipeline (forkAll/convertAll mutate in place, not replace), so we hold it directly.
        final int topId = topStage.getStageId();
        final Stage topWorkerRef = topWorker;
        final Map<Integer, JoinShuffle> descRef = descriptors;
        final Map<Integer, List<String>> nodesByStageIdRef = nodesByStageId;
        final List<Stage> bottomUpRef = bottomUp;
        return new Structure(rewrittenDag, () -> {
            Map<Integer, Stage> rewrittenById = new HashMap<>();
            indexStages(rewrittenDag.rootStage(), rewrittenById);
            List<WorkerLevel> levels = new ArrayList<>();
            for (Stage s : bottomUpRef) {
                JoinShuffle d = descRef.get(s.getStageId());
                boolean isTop = s.getStageId() == topId;
                Stage worker = isTop ? topWorkerRef : rewrittenById.get(s.getStageId());
                Stage leftProducer = rewrittenById.get(d.leftChildId());
                Stage rightProducer = rewrittenById.get(d.rightChildId());
                // Reuse the SAME node list resolved once above — never re-resolve (would risk the
                // worker task / producer placement diverging on a cluster-state change). (codex R6)
                List<String> nodes = nodesByStageIdRef.get(s.getStageId());
                levels.add(new WorkerLevel(worker, leftProducer, rightProducer, d.leftKeys(), d.rightKeys(), d.partitionCount(), nodes));
            }
            return levels;
        });
    }

    /** Result of {@link #rewriteStructure}: the rewritten DAG plus a deferred builder for the
     *  per-level worker descriptors (built after the caller runs the convert pipeline). */
    public record Structure(QueryDAG dag, Supplier<List<WorkerLevel>> levelBuilder) {
        /** Builds the worker-level descriptors. Call only after the convert pipeline has run on
         *  {@link #dag()} (so non-top stages carry resolved plan alternatives). */
        public List<WorkerLevel> buildLevels() {
            return levelBuilder.get();
        }
    }

    /** Supplies target node ids for a given worker level. */
    @FunctionalInterface
    public interface NodeListResolver {
        List<String> resolve(int levelIndex, int partitionCount);
    }

    /** Per-join-shuffle-stage analysis: the join, its left/right child stage ids, and per-side keys. */
    private record JoinShuffle(OpenSearchJoin join, int leftChildId, int rightChildId, List<Integer> leftKeys, List<Integer> rightKeys,
        int partitionCount) {
    }

    /** Extracts the join + its two shuffle inputs' child stage ids and hash keys. */
    private static JoinShuffle analyzeJoinShuffle(Stage stage) {
        OpenSearchJoin join = findJoin(stage.getFragment());
        if (join == null) {
            throw new IllegalStateException("CascadeShuffleDAGRewriter: stage " + stage.getStageId() + " has no join");
        }
        OpenSearchShuffleExchange leftShuffle = asShuffle(join.getInput(0), stage.getStageId(), "left");
        OpenSearchShuffleExchange rightShuffle = asShuffle(join.getInput(1), stage.getStageId(), "right");
        int leftId = childStageId(leftShuffle);
        int rightId = childStageId(rightShuffle);
        // Defense-in-depth (mirrors CascadeShufflePlanRewriter's key check): each side's shuffle must
        // be partitioned on THIS join's equi keys, else the worker join would hash-match on the wrong
        // columns. The plan rewriter already guarantees this, but a future DAG-shape change must not
        // silently lift a mis-keyed shuffle into a worker. (codex review R5 should-fix)
        JoinInfo info = join.analyzeCondition();
        if (!leftShuffle.getHashKeys().equals(info.leftKeys) || !rightShuffle.getHashKeys().equals(info.rightKeys)) {
            throw new IllegalStateException(
                "CascadeShuffleDAGRewriter: stage "
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
                "CascadeShuffleDAGRewriter: stage "
                    + stage.getStageId()
                    + " left/right shuffle partition counts disagree ("
                    + leftShuffle.getPartitionCount()
                    + " vs "
                    + rightShuffle.getPartitionCount()
                    + ")"
            );
        }
        return new JoinShuffle(
            join,
            leftId,
            rightId,
            leftShuffle.getHashKeys(),
            rightShuffle.getHashKeys(),
            leftShuffle.getPartitionCount()
        );
    }

    private static OpenSearchShuffleExchange asShuffle(RelNode input, int stageId, String side) {
        RelNode n = RelNodeUtils.unwrapHep(input);
        if (n instanceof OpenSearchShuffleExchange shuffle) {
            return shuffle;
        }
        throw new IllegalStateException(
            "CascadeShuffleDAGRewriter: stage " + stageId + " join " + side + " input is not a shuffle: " + n.getRelTypeName()
        );
    }

    private static int childStageId(OpenSearchShuffleExchange shuffle) {
        RelNode inner = RelNodeUtils.unwrapHep(shuffle.getInput());
        if (inner instanceof OpenSearchStageInputScan sis) {
            return sis.getChildStageId();
        }
        throw new IllegalStateException("CascadeShuffleDAGRewriter: shuffle input is not a StageInputScan: " + inner.getRelTypeName());
    }

    /**
     * Collects join-shuffle stages top-down: a stage whose fragment contains an {@link OpenSearchJoin}
     * sitting over two {@link OpenSearchShuffleExchange} inputs. Recurses into child stages.
     */
    private static void collectJoinShuffleStages(Stage stage, List<Stage> out) {
        if (stage == null) {
            return;
        }
        if (isJoinShuffleStage(stage)) {
            out.add(stage);
        }
        for (Stage child : stage.getChildStages()) {
            collectJoinShuffleStages(child, out);
        }
    }

    private static boolean isJoinShuffleStage(Stage stage) {
        RelNode fragment = stage.getFragment();
        if (fragment == null) {
            return false;
        }
        OpenSearchJoin join = findJoin(fragment);
        if (join == null) {
            return false;
        }
        // INNER only — mirrors the CascadeShufflePlanRewriter guard. Defense-in-depth: a non-INNER
        // join that CBO itself shuffled (not via our plan rewriter) must not be pulled into the
        // cascade worker tier, which only implements INNER hash-join semantics across partitions.
        // A non-INNER join-over-shuffles keeps its single-level HashShuffleDispatch path.
        if (join.getJoinType() != JoinRelType.INNER) {
            return false;
        }
        // Both of the JOIN'S INPUTS must be shuffles. Only the join is lifted into a worker (the
        // rewriter replaces just the join node with a StageInputScan), so operators ABOVE the join
        // in this stage's fragment — e.g. the root stage's Aggregate / Sort / Project that run on the
        // COORDINATOR after the worker join — are irrelevant to cascade safety and must NOT be checked
        // here (checking the whole fragment root wrongly rejected the canonical Agg/Sort-over-join
        // root stage). The partition-preserving constraint that matters — no Aggregate/Sort BETWEEN
        // join levels — is enforced by CascadeShufflePlanRewriter when it decides to shuffle the
        // inputs (an unsafe intermediate op leaves the input a reducer, not a shuffle), so the
        // join-inputs-are-shuffles check below is exactly the post-condition to verify.
        if (!(RelNodeUtils.unwrapHep(join.getInput(0)) instanceof OpenSearchShuffleExchange)
            || !(RelNodeUtils.unwrapHep(join.getInput(1)) instanceof OpenSearchShuffleExchange)) {
            return false;
        }
        // The join must be the stage's ONLY source of child stages. rebuild() lifts the join and
        // rebuilds this stage with the worker as its single child — if the fragment has any other
        // OpenSearchStageInputScan leaf (a sibling of the join, e.g. Union(Join(...), StageInput→X)
        // or an appendcol), that sibling's child stage would be orphaned and the fragment left
        // dangling. The join's two inputs contribute exactly two stage-input leaves (one per shuffle);
        // require the whole fragment to have exactly those two. A multi-input root keeps its
        // CBO-chosen (single-level / coord-centric) path rather than risk a dropped sibling.
        // (codex review: multi-input-root orphan blocker.)
        if (RelNodeUtils.findNodes(fragment, OpenSearchStageInputScan.class).size() != 2) {
            return false;
        }
        return true;
    }

    /**
     * Rebuilds a stage subtree. When {@code stage.id == topStageId}, the topmost join in its fragment
     * is replaced by a {@code StageInputScan(topWorker.id)} and {@code topWorker} becomes its only
     * child (the stage turns into a reduce over the worker). When a stage id is in
     * {@code intermediateWorkers}, its worker version is used (with rebuilt children). Otherwise the
     * stage is copied with rebuilt children.
     */
    private static Stage rebuild(
        Stage stage,
        int topStageId,
        Stage topWorker,
        Map<Integer, Stage> intermediateWorkers,
        Map<Integer, Stage> byId
    ) {
        if (stage == null) {
            return null;
        }
        if (stage.getStageId() == topStageId && topWorker != null) {
            OpenSearchJoin join = findJoin(stage.getFragment());
            OpenSearchStageInputScan workerScan = new OpenSearchStageInputScan(
                join.getCluster(),
                join.getTraitSet(),
                topWorker.getStageId(),
                join.getRowType(),
                ((OpenSearchRelNode) join).getViableBackends(),
                ((OpenSearchRelNode) join).getOutputFieldStorage()
            );
            RelNode newFragment = replaceNode(stage.getFragment(), join, workerScan);
            Stage reduce = new Stage(
                stage.getStageId(),
                newFragment,
                List.of(topWorker),
                stage.getExchangeInfo(),
                stage.getExchangeSinkProvider(),
                stage.getTargetResolver()
            );
            reduce.setRole(stage.getRole());
            if (stage.getInstructionHandlerFactory() != null) {
                reduce.setInstructionHandlerFactory(stage.getInstructionHandlerFactory());
            }
            return reduce;
        }

        Stage base = intermediateWorkers.getOrDefault(stage.getStageId(), stage);
        List<Stage> rebuiltChildren = new ArrayList<>(base.getChildStages().size());
        boolean isWorker = intermediateWorkers.containsKey(stage.getStageId());
        for (Stage child : base.getChildStages()) {
            rebuiltChildren.add(rebuild(child, topStageId, topWorker, intermediateWorkers, byId));
        }
        Stage copy = new Stage(
            base.getStageId(),
            base.getFragment(),
            rebuiltChildren,
            base.getExchangeInfo(),
            base.getExchangeSinkProvider(),
            base.getTargetResolver()
        );
        copy.setRole(base.getRole());
        if (base.getInstructionHandlerFactory() != null) {
            copy.setInstructionHandlerFactory(base.getInstructionHandlerFactory());
        }
        if (!isWorker) {
            // Preserve already-resolved alternatives on pass-through stages; worker stages get
            // re-forked below so their (empty) alternatives are fine to leave for forkAll.
            copy.setPlanAlternatives(base.getPlanAlternatives());
        }
        return copy;
    }

    /** Finds the first {@link OpenSearchJoin} in {@code root}'s subtree; null if none. */
    private static OpenSearchJoin findJoin(RelNode root) {
        RelNode n = RelNodeUtils.unwrapHep(root);
        if (n instanceof OpenSearchJoin j) {
            return j;
        }
        for (RelNode input : n.getInputs()) {
            OpenSearchJoin found = findJoin(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** Rebuilds {@code root} replacing {@code target} with {@code replacement}; ancestors copy. */
    private static RelNode replaceNode(RelNode root, RelNode target, RelNode replacement) {
        if (root == target) {
            return replacement;
        }
        List<RelNode> oldInputs = root.getInputs();
        if (oldInputs.isEmpty()) {
            return root;
        }
        List<RelNode> newInputs = new ArrayList<>(oldInputs.size());
        boolean changed = false;
        for (RelNode input : oldInputs) {
            RelNode rewritten = replaceNode(input, target, replacement);
            newInputs.add(rewritten);
            if (rewritten != input) {
                changed = true;
            }
        }
        return changed ? root.copy(root.getTraitSet(), newInputs) : root;
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

    private static int nextStageId(QueryDAG dag) {
        int[] max = { -1 };
        walkMax(dag.rootStage(), max);
        return max[0] + 1;
    }

    private static void walkMax(Stage stage, int[] max) {
        if (stage == null) {
            return;
        }
        if (stage.getStageId() > max[0]) {
            max[0] = stage.getStageId();
        }
        for (Stage child : stage.getChildStages()) {
            walkMax(child, max);
        }
    }
}
