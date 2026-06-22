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
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
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
        return rewriteStructure(dag, registry, nodeResolver, null);
    }

    /**
     * Overload taking an optional {@link AggLift}. When non-null AND the topmost join-shuffle stage IS
     * the root stage (the q5/q10 agg-over-cascade shape), the top worker fragment is decorated with the
     * lifted pre-aggregate pipeline + {@code Aggregate(PARTIAL)} ({@link AggLift#workerFragment}), the
     * coordinator root becomes {@code Sort? → Aggregate(FINAL) → StageInputScan(topWorker)}
     * ({@link AggLift#coordinatorFragment}), and the dimension {@link AggLift#broadcastBuildStages} ride
     * as extra top-worker children so the dispatcher can build + inject them. Null restores the plain
     * cascade behaviour (join stays in the worker, agg/sort stay on the coordinator). Threaded by
     * {@link DistributedAggOverJoinRewriter}.
     */
    public static Structure rewriteStructure(QueryDAG dag, CapabilityRegistry registry, NodeListResolver nodeResolver, AggLift aggLift) {
        return rewriteStructure(dag, registry, nodeResolver, aggLift, null);
    }

    /**
     * Overload taking a caller-supplied stage-id counter. When {@code sharedIdCounter} is non-null it is
     * used (and advanced) instead of {@code nextStageId(dag)} for minting the new top-worker id. The
     * multi-subtree scalar-subquery path (TPC-H q2/q11) threads ONE counter across BOTH child-subtree
     * rewrites so the new top-worker ids never collide — each child computes its own {@code nextStageId}
     * from its local subtree max, which would otherwise overlap once both are grafted under the shared
     * coordinator root, and {@code OpenSearchStageInputScan} references stage ids BY VALUE in the
     * fragment (no safe post-graft renumber). Null preserves the single-DAG behaviour (q5/q10/q11-main).
     */
    public static Structure rewriteStructure(
        QueryDAG dag,
        CapabilityRegistry registry,
        NodeListResolver nodeResolver,
        AggLift aggLift,
        int[] sharedIdCounter
    ) {
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
        // Use the caller's shared counter when supplied (multi-subtree graft, q2/q11) so new worker ids
        // are globally unique across sibling rewrites; otherwise derive from this DAG's max (q5/q10).
        int[] nextId = sharedIdCounter != null ? sharedIdCounter : new int[] { nextStageId(dag) };

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

        // Distributed-agg-over-cascade (q5/q10): the top worker carries the lifted pre-agg pipeline +
        // PARTIAL aggregate, and the dimension build stages ride as extra worker children. Only valid
        // when the topmost join-shuffle stage IS the root (the aggregate/dim-joins were in the root
        // fragment above the liftable join). For any deeper-topStage cascade the lift does not apply.
        boolean applyAggLift = aggLift != null && topStage.getStageId() == root.getStageId();
        RelNode topWorkerFragment = applyAggLift ? aggLift.workerFragment(topDesc.join(), topWorkerId) : topDesc.join();
        List<Stage> topWorkerChildren = new ArrayList<>();
        topWorkerChildren.add(topLeftChild);
        topWorkerChildren.add(topRightChild);
        if (applyAggLift) {
            // The dimension stages become broadcast builds on the worker; keep them as children so the
            // scheduler/dispatcher can run + capture them (they reference broadcast-<id> NamedScans in
            // the worker fragment, not partition streams).
            topWorkerChildren.addAll(aggLift.broadcastBuildStages());
        }
        Stage topWorker = new Stage(
            topWorkerId,
            topWorkerFragment,
            topWorkerChildren,
            ExchangeInfo.singleton(),
            backend.getExchangeSinkProvider(),
            new WorkerTargetResolver(topNodes)
        );
        topWorker.setRole(Stage.StageRole.SHUFFLE_WORKER);
        topWorker.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());

        // Rebuild the DAG from the root. Default: at the top join-shuffle stage, replace the join with
        // a StageInputScan(topWorkerId) and make topWorker its only child; elsewhere copy through. For
        // the agg-lift the coordinator root is rebuilt as Sort? → Aggregate(FINAL) → StageInput(worker)
        // — the dimension joins moved ONTO the worker, so they no longer appear on the coordinator.
        Stage newRoot;
        if (applyAggLift) {
            newRoot = new Stage(
                root.getStageId(),
                aggLift.coordinatorFragment(topWorker),
                List.of(topWorker),
                root.getExchangeInfo(),
                root.getExchangeSinkProvider(),
                root.getTargetResolver()
            );
            newRoot.setRole(root.getRole());
            if (root.getInstructionHandlerFactory() != null) {
                newRoot.setInstructionHandlerFactory(root.getInstructionHandlerFactory());
            }
        } else {
            newRoot = rebuild(root, topStage.getStageId(), topWorker, intermediateWorkers, byId);
        }
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

    /**
     * Optional hook that turns the cascade top worker into a distributed-agg worker (q5/q10 Variant A).
     * Supplied by {@link DistributedAggOverJoinRewriter}; the cascade rewriter calls it only when the
     * topmost join-shuffle stage IS the root stage.
     *
     * <ul>
     *   <li>{@link #workerFragment} wraps the lifted top join in the pre-aggregate pipeline (dimension
     *       joins rewritten to broadcast scans) + {@code Aggregate(PARTIAL)}; {@code topWorkerId} is the
     *       new worker's stage id (only needed if the fragment must reference it — it does not today).</li>
     *   <li>{@link #coordinatorFragment} builds the coordinator reduce fragment ({@code Sort? →
     *       Aggregate(FINAL) → StageInputScan(topWorker)}) given the freshly-built top worker stage.</li>
     *   <li>{@link #broadcastBuildStages} are the dimension stages (already tagged
     *       {@link Stage.StageRole#BROADCAST_BUILD}) to attach as extra top-worker children.</li>
     * </ul>
     */
    public interface AggLift {
        RelNode workerFragment(OpenSearchJoin liftedTopJoin, int topWorkerId);

        RelNode coordinatorFragment(Stage topWorker);

        List<Stage> broadcastBuildStages();
    }

    /** Per-join-shuffle-stage analysis: the join, its left/right child stage ids, and per-side keys. */
    private record JoinShuffle(OpenSearchJoin join, int leftChildId, int rightChildId, List<Integer> leftKeys, List<Integer> rightKeys,
        int partitionCount) {
    }

    /** Extracts the join + its two shuffle inputs' child stage ids and hash keys. */
    private static JoinShuffle analyzeJoinShuffle(Stage stage) {
        OpenSearchJoin join = findLiftableJoin(stage.getFragment());
        if (join == null) {
            throw new IllegalStateException("CascadeShuffleDAGRewriter: stage " + stage.getStageId() + " has no liftable join");
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
        return findLiftableJoin(stage.getFragment()) != null;
    }

    /**
     * Returns the {@link OpenSearchJoin} this stage's fragment should lift into a worker tier, or
     * {@code null} if the fragment has none / is not cascade-safe.
     *
     * <p>The liftable join is the HIGHEST {@link OpenSearchJoin} whose two inputs are BOTH
     * {@link OpenSearchShuffleExchange} — i.e. a join over two already-shuffled cascade inputs. For a
     * 2-way / leaf cascade level the fragment root IS that join. For a NESTED case (TPC-H q5/q10:
     * {@code Sort(Agg(Project(Join_dim(... Join_dim(Join_bottom, ER→dim) ...))))}) the liftable join
     * {@code Join_bottom} sits UNDER one or more coordinator joins that key on different columns and
     * therefore did not shuffle. Only {@code Join_bottom} is lifted; the coordinator joins above it
     * stay in this stage consuming the worker's SINGLETON output (their reducer-fed dimension inputs
     * remain child stages — see {@link #rebuild}).
     *
     * <p>Cascade-safety / orphan-protection (codex blockers — do NOT loosen):
     * <ul>
     *   <li>The liftable join must be INNER (the worker only implements INNER hash-join across
     *       partitions). The coordinator joins above it may be any type — they are NOT lifted, they
     *       run on the coordinator exactly as in the coord-centric path.</li>
     *   <li>Every operator on the path from the fragment root DOWN TO the liftable join must be a
     *       coordinator op that runs AFTER the worker gathers — {@link OpenSearchJoin},
     *       {@link OpenSearchProject}, {@link OpenSearchFilter}, {@link OpenSearchSort},
     *       {@link OpenSearchAggregate}. A multi-input combinator that is NOT a join (e.g.
     *       {@link org.opensearch.analytics.planner.rel.OpenSearchUnion} / appendcol) on the path is
     *       rejected: lifting under it would leave the join's SIBLING stage-input orphaned, and its
     *       per-partition semantics are not validated. The "nested under coordinator joins (safe)"
     *       vs "sibling under Union/appendcol (unsafe)" distinction is encoded HERE.</li>
     *   <li>The liftable join must be UNIQUE in the fragment — no second disjoint join-over-two-
     *       shuffles (e.g. {@code Union(Join(sh,sh), Join(sh,sh))}). One fragment rebuild lifts exactly
     *       one join; two would need two workers in one stage, which {@link #rebuild} does not express.</li>
     * </ul>
     * The per-side hash-key match and the no-Aggregate/Sort-BETWEEN-levels (partition-preserving)
     * constraints are enforced upstream by {@link CascadeShufflePlanRewriter} (an unsafe intermediate
     * op leaves the input a reducer, not a shuffle) and re-checked in {@link #analyzeJoinShuffle}.
     */
    static OpenSearchJoin findLiftableJoin(RelNode fragment) {
        if (fragment == null) {
            return null;
        }
        // Identity set: a fragment is a DAG, not a tree — a common sub-expression (e.g. a shared
        // join node reachable via two parents) would otherwise be counted twice and trip the
        // uniqueness check, hiding a legitimately liftable join. Dedup by reference identity so a
        // SHARED liftable join still counts as one. (real-DAG diagnosis: double-referenced subtree)
        Set<OpenSearchJoin> liftable = Collections.newSetFromMap(new IdentityHashMap<>());
        collectLiftableJoins(RelNodeUtils.unwrapHep(fragment), liftable);
        // Exactly one join-over-two-shuffles may be lifted per fragment (see uniqueness note above).
        if (liftable.size() != 1) {
            return null;
        }
        OpenSearchJoin join = liftable.iterator().next();
        if (join.getJoinType() != JoinRelType.INNER) {
            return null;
        }
        // The path from the fragment root down to the liftable join must contain only coordinator
        // operators (no Union/appendcol sibling) — otherwise lifting would orphan a sibling stage.
        if (!isCoordinatorPathTo(RelNodeUtils.unwrapHep(fragment), join)) {
            return null;
        }
        return join;
    }

    /** Collects every {@link OpenSearchJoin} whose two inputs are both {@link OpenSearchShuffleExchange}. */
    private static void collectLiftableJoins(RelNode node, Set<OpenSearchJoin> out) {
        if (node instanceof OpenSearchJoin join
            && RelNodeUtils.unwrapHep(join.getInput(0)) instanceof OpenSearchShuffleExchange
            && RelNodeUtils.unwrapHep(join.getInput(1)) instanceof OpenSearchShuffleExchange) {
            out.add(join);
            // A join-over-two-shuffles' inputs are shuffles (StageInputScan leaves) — no deeper
            // liftable join inside THIS stage's fragment (lower levels are separate child stages).
            return;
        }
        for (RelNode input : node.getInputs()) {
            collectLiftableJoins(RelNodeUtils.unwrapHep(input), out);
        }
    }

    /**
     * True iff every node on the path from {@code root} down to {@code target} (inclusive of the
     * intermediate operators, exclusive of {@code target} itself) is a coordinator op safe to keep in
     * this stage after the lift: {@link OpenSearchJoin} / {@link OpenSearchProject} /
     * {@link OpenSearchFilter} / {@link OpenSearchSort} / {@link OpenSearchAggregate}. Any other node
     * type on the path (notably a non-join multi-input combinator like
     * {@link org.opensearch.analytics.planner.rel.OpenSearchUnion}) returns false.
     */
    private static boolean isCoordinatorPathTo(RelNode root, OpenSearchJoin target) {
        if (root == target) {
            return true;
        }
        boolean safeNode = root instanceof OpenSearchJoin
            || root instanceof OpenSearchProject
            || root instanceof OpenSearchFilter
            || root instanceof OpenSearchSort
            || root instanceof OpenSearchAggregate;
        if (!safeNode) {
            return false;
        }
        for (RelNode input : root.getInputs()) {
            if (containsNode(RelNodeUtils.unwrapHep(input), target)) {
                return isCoordinatorPathTo(RelNodeUtils.unwrapHep(input), target);
            }
        }
        return false;
    }

    /** True iff {@code target} appears anywhere in {@code node}'s subtree (reference identity). */
    private static boolean containsNode(RelNode node, RelNode target) {
        if (node == target) {
            return true;
        }
        for (RelNode input : node.getInputs()) {
            if (containsNode(RelNodeUtils.unwrapHep(input), target)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Rebuilds a stage subtree. When {@code stage.id == topStageId}, the LIFTABLE join in its fragment
     * (see {@link #findLiftableJoin}) is replaced by a {@code StageInputScan(topWorker.id)} and
     * {@code topWorker} is added as a child. Any other child stages whose {@code StageInputScan} leaves
     * SURVIVE the replacement — the reducer-fed dimension inputs to coordinator joins ABOVE the lifted
     * join (TPC-H q5/q10: supplier/nation/region) — are KEPT as children so they are NOT orphaned; only
     * the two child stages feeding the lifted join's shuffles are dropped (replaced by the worker). When
     * a stage id is in {@code intermediateWorkers}, its worker version is used (with rebuilt children).
     * Otherwise the stage is copied with rebuilt children.
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
            OpenSearchJoin join = findLiftableJoin(stage.getFragment());
            OpenSearchStageInputScan workerScan = new OpenSearchStageInputScan(
                join.getCluster(),
                join.getTraitSet(),
                topWorker.getStageId(),
                join.getRowType(),
                ((OpenSearchRelNode) join).getViableBackends(),
                ((OpenSearchRelNode) join).getOutputFieldStorage()
            );
            RelNode newFragment = replaceNode(stage.getFragment(), join, workerScan);
            // The worker is the lifted join's replacement child. Every OTHER child stage still
            // referenced by a StageInputScan in the rewritten fragment — the reducer-fed dimension
            // inputs to coordinator joins ABOVE the lifted join — must stay as children (else they are
            // orphaned: a StageInputScan with no backing stage hangs the scheduler). Recurse so a deeper
            // cascade under a kept dimension input is also rewritten (defensive; q5/q10 dims are leaf
            // reducers). The two child stages feeding the lifted join's shuffles are gone — their
            // StageInputScans were replaced by the single workerScan above. (cascade-extend orphan-safety)
            List<Integer> survivingChildIds = RelNodeUtils.findNodes(newFragment, OpenSearchStageInputScan.class)
                .stream()
                .map(OpenSearchStageInputScan::getChildStageId)
                .filter(id -> id != topWorker.getStageId())
                .toList();
            List<Stage> reduceChildren = new ArrayList<>(survivingChildIds.size() + 1);
            reduceChildren.add(topWorker);
            for (Stage child : stage.getChildStages()) {
                if (survivingChildIds.contains(child.getStageId())) {
                    reduceChildren.add(rebuild(child, topStageId, topWorker, intermediateWorkers, byId));
                }
            }
            Stage reduce = new Stage(
                stage.getStageId(),
                newFragment,
                reduceChildren,
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
