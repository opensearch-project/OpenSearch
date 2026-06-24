/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.exec.join.CascadeShuffleDAGRewriter;
import org.opensearch.analytics.exec.join.CascadeShufflePlanRewriter;
import org.opensearch.analytics.exec.join.DistributionEnforcementPass;
import org.opensearch.analytics.exec.join.GeneralShuffleDAGRewriter;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.PlanAlternativeSelector;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.dag.WorkerTargetResolver;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Locks in the cascaded hash-shuffle plan shape for a 3-way INNER equi-join
 * {@code Join(Join(A,B), C)}:
 * <ol>
 *   <li>{@link CascadeShufflePlanRewriter} turns the outer join's coordinator-gathered inputs into
 *       hash-shuffle inputs (post-CBO), so {@code DAGBuilder} cuts a nested-shuffle DAG.</li>
 *   <li>{@link CascadeShuffleDAGRewriter#rewriteStructure} lifts each join level into its own worker
 *       tier — the top join into a NEW worker, the inner join into a consume-and-produce worker.</li>
 * </ol>
 * Validates the structure (worker count, execution types, producer wiring, coordinator gather)
 * without the convert pipeline (the mock backend has no fragment convertor; conversion + actual
 * row-count parity are exercised on the live cluster).
 */
public class CascadeShuffleProbeTests extends BasePlannerRulesTests {

    private static final int CLUSTER_DATA_NODES = 3;
    private static final long LARGE = 10_000_000L;

    public void testCascadeShuffleShape_threeWayJoin() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeThreeWayJoin(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);

        // Mirror DefaultPlanExecutor's plan pipeline EXCEPT convertAll — the mock backend has no
        // fragment convertor. forkAll + selectAll populate plan alternatives' backendId, which is all
        // the structural rewrite reads; convertAll (real backend) is exercised by the live cluster.
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        // Now drive the recursive DAG rewriter (worker-tier surgery) and assert the cascade shape.
        assertTrue("expected a cascade DAG (>1 join-shuffle stage)", CascadeShuffleDAGRewriter.isCascade(dag));

        CascadeShuffleDAGRewriter.Structure structure = CascadeShuffleDAGRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );

        // Structural assertions: 2 join levels → 2 worker tiers; both run as WORKER_FRAGMENT.
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.buildLevels();
        assertEquals("two join levels → two worker tiers", 2, levels.size());
        for (CascadeShuffleDAGRewriter.WorkerLevel level : levels) {
            Stage worker = level.worker();
            assertEquals(
                "worker stage must be WORKER_FRAGMENT (has WorkerTargetResolver)",
                StageExecutionType.WORKER_FRAGMENT,
                worker.getExecutionType()
            );
            assertTrue("worker resolver is WorkerTargetResolver", worker.getTargetResolver() instanceof WorkerTargetResolver);
            assertEquals("worker role SHUFFLE_WORKER", Stage.StageRole.SHUFFLE_WORKER, worker.getRole());
        }
        // The deepest worker (level 0) consumes two leaf shard scans; the top worker (level 1)
        // consumes the deepest worker + a leaf scan.
        CascadeShuffleDAGRewriter.WorkerLevel deepest = levels.get(0);
        CascadeShuffleDAGRewriter.WorkerLevel top = levels.get(1);
        assertEquals(
            "deepest worker left producer is a shard fragment",
            StageExecutionType.SHARD_FRAGMENT,
            deepest.leftProducer().getExecutionType()
        );
        assertEquals(
            "top worker's one producer is the deepest worker (consume-and-produce)",
            deepest.worker().getStageId(),
            // exactly one of top's producers is the intermediate worker
            (top.leftProducer().getStageId() == deepest.worker().getStageId() ? top.leftProducer() : top.rightProducer()).getStageId()
        );
        // Root reduce stage gathers the top worker via its single child; the top worker's exchange
        // is SINGLETON (gathers to coordinator). The root stage itself is the coordinator terminal
        // (null exchange info — nothing consumes it).
        Stage rootStage = structure.dag().rootStage();
        assertEquals("root reduce has exactly one child = top worker", 1, rootStage.getChildStages().size());
        Stage topWorkerInDag = rootStage.getChildStages().get(0);
        assertEquals("top worker id matches", top.worker().getStageId(), topWorkerInDag.getStageId());
        assertEquals(
            "top worker gathers SINGLETON to coordinator",
            RelDistribution.Type.SINGLETON,
            topWorkerInDag.getExchangeInfo().distributionType()
        );
    }

    /**
     * Option B (general enforcement pass): the SAME 3-way shared-key join, scheduled by
     * {@link DistributionEnforcementPass} instead of {@code CascadeShufflePlanRewriter}. The pass must
     * produce a cascade of BINARY worker tiers — BOTH joins over two ShuffleExchange inputs.
     *
     * <p><b>Binary-tier lowering.</b> A join is a worker-tier boundary: the hash-shuffle transport
     * delivers exactly two named inputs per worker (left/right buffer slices). So even though the bottom
     * join's output already satisfies the top join's hash requirement (shared key), the pass inserts a
     * SAME-KEY inter-tier shuffle on the top join's left input — the bottom join becomes its own binary
     * producer stage, exactly as the legacy cascade does (intermediate worker → shuffle → parent worker).
     * The top join thus sits over TWO shuffles (re-shuffled bottom-join output + shuffled c), and under its
     * left shuffle sits the bottom join (itself over two scan shuffles). Eliminating the same-key inter-tier
     * shuffle for genuinely co-partitioned tiers is a future N-ary-transport optimization.
     */
    public void testEnforcementPass_threeWayJoinFormsCascade() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode cbo = runPlanner(makeThreeWayJoin(context), context);
        RelNode enforced = DistributionEnforcementPass.enforce(
            cbo,
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1L
        );

        // Binary-tier lowering: BOTH joins sit over two ShuffleExchange inputs (no N-ary collapse).
        List<OpenSearchJoin> joins = findAll(enforced, OpenSearchJoin.class);
        assertEquals("two joins in the 3-way plan", 2, joins.size());
        for (OpenSearchJoin join : joins) {
            assertTrue(
                "each join level must sit over two shuffle inputs (binary tier)",
                unwrap(join.getInput(0)) instanceof OpenSearchShuffleExchange
                    && unwrap(join.getInput(1)) instanceof OpenSearchShuffleExchange
            );
        }
        // The top join's left shuffle re-partitions the bottom join's output (same-key inter-tier shuffle);
        // under it sits the bottom join over its two scan shuffles.
        OpenSearchJoin top = joins.stream().filter(j -> findAll(j, OpenSearchJoin.class).size() == 2).findFirst().orElseThrow();
        RelNode underTopLeftShuffle = unwrap(((OpenSearchShuffleExchange) unwrap(top.getInput(0))).getInput());
        assertTrue("the bottom join sits under the top join's left inter-tier shuffle", underTopLeftShuffle instanceof OpenSearchJoin);
    }

    /**
     * Option B: agg over a 3-way cascade (q5/q10 class). The enforcement pass must split the SINGLE
     * aggregate into PARTIAL (over the distributed cascade) + FINAL (over a coordinator gather), and
     * the cascade below must still form. Asserts: exactly one PARTIAL + one FINAL aggregate, a cascade
     * of 2 joins, and the FINAL gathers (ER above PARTIAL).
     */
    public void testEnforcementPass_aggOverThreeWayJoinSplitsAndCascades() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode cbo = runPlanner(makeAggregateOverThreeWayJoin(context), context);
        RelNode enforced = DistributionEnforcementPass.enforce(
            cbo,
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1L
        );

        List<org.opensearch.analytics.planner.rel.OpenSearchAggregate> aggs = findAll(
            enforced,
            org.opensearch.analytics.planner.rel.OpenSearchAggregate.class
        );
        long partials = aggs.stream().filter(a -> a.getMode() == org.opensearch.analytics.planner.rel.AggregateMode.PARTIAL).count();
        long finals = aggs.stream().filter(a -> a.getMode() == org.opensearch.analytics.planner.rel.AggregateMode.FINAL).count();
        assertEquals("exactly one PARTIAL aggregate (pushed below the gather)", 1, partials);
        assertEquals("exactly one FINAL aggregate (on the coordinator)", 1, finals);

        // The cascade still forms below the PARTIAL: 2 joins, each a binary tier (over two shuffles).
        List<OpenSearchJoin> joins = findAll(enforced, OpenSearchJoin.class);
        assertEquals("two joins in the 3-way cascade", 2, joins.size());
        long binaryTierJoins = joins.stream()
            .filter(
                j -> unwrap(j.getInput(0)) instanceof OpenSearchShuffleExchange
                    && unwrap(j.getInput(1)) instanceof OpenSearchShuffleExchange
            )
            .count();
        assertEquals("both join levels are binary tiers (each over two shuffles)", 2, binaryTierJoins);
    }

    /**
     * Option B coverage the rewriters CANNOT do (1): a LEFT-outer 3-way join. The current cascade is
     * INNER-only; the enforcement pass co-partitions outer joins too (null-fill is partition-local for a
     * hash-partitioned outer join). Asserts the cascade still forms over a LEFT top join.
     */
    public void testEnforcementPass_leftOuterThreeWayJoinCascades() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode cbo = runPlanner(makeThreeWayJoinTopType(context, JoinRelType.LEFT), context);
        RelNode enforced = DistributionEnforcementPass.enforce(
            cbo,
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1L
        );

        List<OpenSearchJoin> joins = findAll(enforced, OpenSearchJoin.class);
        assertEquals("two joins", 2, joins.size());
        long shuffleJoins = joins.stream()
            .filter(
                j -> unwrap(j.getInput(0)) instanceof OpenSearchShuffleExchange
                    || unwrap(j.getInput(1)) instanceof OpenSearchShuffleExchange
            )
            .count();
        assertEquals("both join levels are distributed (outer join co-partitions too)", 2, shuffleJoins);
        boolean hasLeftJoin = joins.stream().anyMatch(j -> j.getJoinType() == JoinRelType.LEFT);
        assertTrue("the top join stays LEFT (distribution doesn't change join semantics)", hasLeftJoin);
    }

    /**
     * Option B coverage the rewriters struggle with (2): a MIXED-KEY 3-way join — a⋈b on col0, then ⋈c
     * on col1. The bottom join's output is HASH(col0); the top join needs HASH(col1) → the pass inserts a
     * RE-SHUFFLE exactly at the key change (the bottom join's output does NOT satisfy the top's
     * requirement). Asserts: the top join's left input is a ShuffleExchange (re-shuffled), not the bare
     * bottom join.
     */
    public void testEnforcementPass_mixedKeyCascadeReshufflesAtKeyChange() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode cbo = runPlanner(makeMixedKeyThreeWayJoin(context), context);
        RelNode enforced = DistributionEnforcementPass.enforce(
            cbo,
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1L
        );

        List<OpenSearchJoin> joins = findAll(enforced, OpenSearchJoin.class);
        assertEquals("two joins", 2, joins.size());
        // The top join (keyed on col1) must have BOTH inputs shuffled — its left input is a re-shuffle of
        // the bottom join (which was keyed on col0), because HASH(col0) does NOT satisfy HASH(col1).
        OpenSearchJoin top = joins.stream()
            .filter(j -> findAll(j, OpenSearchJoin.class).size() == 2) // the one containing the other join
            .findFirst()
            .orElseThrow();
        assertTrue("top join left input re-shuffled at the key change", unwrap(top.getInput(0)) instanceof OpenSearchShuffleExchange);
        assertTrue("top join right input shuffled", unwrap(top.getInput(1)) instanceof OpenSearchShuffleExchange);
        // And under the left re-shuffle sits the bottom join (which itself shuffled its two scans).
        RelNode underLeftShuffle = unwrap(((OpenSearchShuffleExchange) unwrap(top.getInput(0))).getInput());
        assertTrue("bottom join sits under the top's left re-shuffle", underLeftShuffle instanceof OpenSearchJoin);
    }

    /**
     * Option B size floor: a 3-way join whose scans are all BELOW the row floor must stay
     * coordinator-centric — the pass distributes nothing (no ShuffleExchange), matching CBO's cheap
     * coord-centric choice for small joins. Guards against the pass force-distributing every join.
     */
    public void testEnforcementPass_smallJoinBelowFloorStaysCoordCentric() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", 1_000L, "b_idx", 1_000L, "c_idx", 1_000L);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode cbo = runPlanner(makeThreeWayJoin(context), context);
        RelNode enforced = DistributionEnforcementPass.enforce(
            cbo,
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1_000_000L
        );

        assertTrue("no shuffle for a join below the row floor", findAll(enforced, OpenSearchShuffleExchange.class).isEmpty());
    }

    // ── GeneralShuffleDAGRewriter (B2): in-place worker promotion of the enforced DAG ──────────────

    /**
     * B2: the enforced 3-way join DAG, promoted by {@link GeneralShuffleDAGRewriter}. Binary-tier lowering
     * made BOTH joins their own join-over-two-shuffles stage, so the rewriter promotes TWO worker tiers in
     * place (each {@code SHUFFLE_WORKER} + {@code WORKER_FRAGMENT}). The deepest worker consumes two leaf
     * scan shuffles; the top worker consumes the deepest worker (as a producer) + a leaf scan shuffle.
     */
    public void testGeneralShuffle_threeWayJoinPromotesTwoWorkerTiers() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        GeneralShuffleDAGRewriter.Structure structure = enforceAndPromote(makeThreeWayJoin(context), context);
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.buildLevels();
        assertEquals("two binary join tiers → two worker tiers", 2, levels.size());
        for (CascadeShuffleDAGRewriter.WorkerLevel level : levels) {
            assertEquals(
                "promoted join tier must be a WORKER_FRAGMENT",
                StageExecutionType.WORKER_FRAGMENT,
                level.worker().getExecutionType()
            );
            assertTrue("worker resolver is WorkerTargetResolver", level.worker().getTargetResolver() instanceof WorkerTargetResolver);
            assertEquals("worker role SHUFFLE_WORKER", Stage.StageRole.SHUFFLE_WORKER, level.worker().getRole());
        }
        // Deepest worker (level 0) consumes two leaf shard scans; top worker (level 1) consumes the deepest
        // worker (its producer) + a leaf scan.
        CascadeShuffleDAGRewriter.WorkerLevel deepest = levels.get(0);
        CascadeShuffleDAGRewriter.WorkerLevel top = levels.get(1);
        assertEquals(
            "deepest worker left producer is a shard fragment",
            StageExecutionType.SHARD_FRAGMENT,
            deepest.leftProducer().getExecutionType()
        );
        boolean topConsumesDeepest = top.leftProducer().getStageId() == deepest.worker().getStageId()
            || top.rightProducer().getStageId() == deepest.worker().getStageId();
        assertTrue("top worker consumes the deepest worker as one of its producers", topConsumesDeepest);
        // The root stays a coordinator reduce gathering the top worker's SINGLETON output.
        Stage newRoot = structure.dag().rootStage();
        assertEquals(StageExecutionType.COORDINATOR_REDUCE, newRoot.getExecutionType());
    }

    /**
     * B2: the enforced agg-over-3-way DAG. The pass pre-split the aggregate into
     * {@code FINAL(ER(PARTIAL(...)))}; DAGBuilder cut the ER, so the PARTIAL sits in the SAME stage as the
     * top join. {@link GeneralShuffleDAGRewriter} promotes that stage to a worker IN PLACE — so the PARTIAL
     * aggregate rides on the top worker (runs per-partition), and the FINAL stays on the coordinator. Asserts
     * two worker tiers, the PARTIAL on the top worker's fragment, the FINAL on the coordinator root.
     */
    public void testGeneralShuffle_aggOverJoinKeepsPartialOnWorker() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        GeneralShuffleDAGRewriter.Structure structure = enforceAndPromote(makeAggregateOverThreeWayJoin(context), context);
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.buildLevels();
        assertEquals("two binary join tiers → two worker tiers", 2, levels.size());

        // The top worker fragment carries the PARTIAL aggregate above its join.
        CascadeShuffleDAGRewriter.WorkerLevel top = levels.get(levels.size() - 1);
        List<org.opensearch.analytics.planner.rel.OpenSearchAggregate> workerAggs = findAll(
            top.worker().getFragment(),
            org.opensearch.analytics.planner.rel.OpenSearchAggregate.class
        );
        assertEquals("top worker fragment carries exactly the PARTIAL aggregate", 1, workerAggs.size());
        assertEquals(
            "the worker aggregate is the PARTIAL",
            org.opensearch.analytics.planner.rel.AggregateMode.PARTIAL,
            workerAggs.get(0).getMode()
        );

        // The coordinator root carries the FINAL aggregate (and gathers the top worker).
        Stage newRoot = structure.dag().rootStage();
        assertEquals(StageExecutionType.COORDINATOR_REDUCE, newRoot.getExecutionType());
        List<org.opensearch.analytics.planner.rel.OpenSearchAggregate> rootAggs = findAll(
            newRoot.getFragment(),
            org.opensearch.analytics.planner.rel.OpenSearchAggregate.class
        );
        assertEquals("coordinator root carries exactly the FINAL aggregate", 1, rootAggs.size());
        assertEquals(
            "the root aggregate is the FINAL",
            org.opensearch.analytics.planner.rel.AggregateMode.FINAL,
            rootAggs.get(0).getMode()
        );
    }

    /**
     * B2 coverage the legacy cascade CANNOT do: a BUSHY (non-left-deep) tree {@code (A⋈B) ⋈ (C⋈D)} on a
     * shared key. The legacy {@code deepestInnerEquiJoin} walk assumes a left-deep chain (one bottom join,
     * one probe per level) and bails on a bushy tree; the general pass is a plain bottom-up visitor, so it
     * distributes BOTH sub-joins and the top join with no special code → THREE binary worker tiers, the top
     * worker consuming the two sub-join workers as its producers.
     */
    public void testGeneralShuffle_bushyTreePromotesThreeWorkerTiers() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3, "d_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE, "d_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        GeneralShuffleDAGRewriter.Structure structure = enforceAndPromote(makeBushyFourWayJoin(context), context);
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.buildLevels();
        assertEquals("bushy (A⋈B)⋈(C⋈D) → three binary worker tiers", 3, levels.size());
        for (CascadeShuffleDAGRewriter.WorkerLevel level : levels) {
            assertEquals("each tier is a WORKER_FRAGMENT", StageExecutionType.WORKER_FRAGMENT, level.worker().getExecutionType());
            assertEquals("each tier role SHUFFLE_WORKER", Stage.StageRole.SHUFFLE_WORKER, level.worker().getRole());
        }
        // The top tier (level 2, last bottom-up) consumes the two sub-join workers (levels 0 and 1) as BOTH
        // its producers — the bushy signature (vs the left-deep cascade, where only the left producer is an
        // intermediate worker).
        CascadeShuffleDAGRewriter.WorkerLevel top = levels.get(2);
        int l0 = levels.get(0).worker().getStageId();
        int l1 = levels.get(1).worker().getStageId();
        java.util.Set<Integer> topProducers = java.util.Set.of(top.leftProducer().getStageId(), top.rightProducer().getStageId());
        assertEquals("top tier's two producers are the two sub-join workers", java.util.Set.of(l0, l1), topProducers);
        Stage newRoot = structure.dag().rootStage();
        assertEquals("root stays coordinator reduce", StageExecutionType.COORDINATOR_REDUCE, newRoot.getExecutionType());
    }

    /**
     * B2 regression (the q3 intermediate-Project worker-timeout, found at sf-scale on the cluster): a 3-way
     * join with an explicit row-wise Project BETWEEN the two join levels — the real PPL plan shape, where
     * each join's output is renamed by a Project. The intermediate Project must RIDE on the bottom join's
     * worker (propagate its HASH partitioning), NOT gather it to the coordinator — gathering would cut the
     * bottom join into a separate reduce-fed stage that never ships its shuffle, so the top worker times out
     * on its missing input. Asserts TWO worker tiers (not a gather between them) and the intermediate Project
     * sits inside the bottom worker's fragment.
     */
    public void testGeneralShuffle_intermediateProjectRidesOnWorker() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        GeneralShuffleDAGRewriter.Structure structure = enforceAndPromote(makeThreeWayJoinWithMidProject(context), context);
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.buildLevels();
        // TWO worker tiers — NOT three (a spurious gather stage from the mid-Project would break promotion).
        assertEquals("intermediate Project must not insert a gather tier → still two worker tiers", 2, levels.size());
        for (CascadeShuffleDAGRewriter.WorkerLevel level : levels) {
            assertEquals("each tier is a WORKER_FRAGMENT", StageExecutionType.WORKER_FRAGMENT, level.worker().getExecutionType());
        }
        // The bottom worker (level 0) must consume two leaf shard scans directly — proving the mid-Project
        // did NOT cut a gather stage between the bottom join and its scans.
        CascadeShuffleDAGRewriter.WorkerLevel deepest = levels.get(0);
        assertEquals(
            "bottom worker left producer is a shard fragment (no gather inserted by the mid-Project)",
            StageExecutionType.SHARD_FRAGMENT,
            deepest.leftProducer().getExecutionType()
        );
        // The top worker consumes the bottom worker as one of its two producers (the inter-tier shuffle), not
        // a coordinator-reduce stage.
        CascadeShuffleDAGRewriter.WorkerLevel top = levels.get(1);
        boolean topConsumesDeepest = top.leftProducer().getStageId() == deepest.worker().getStageId()
            || top.rightProducer().getStageId() == deepest.worker().getStageId();
        assertTrue("top worker consumes the bottom worker directly (no gather between tiers)", topConsumesDeepest);
        for (CascadeShuffleDAGRewriter.WorkerLevel level : levels) {
            assertTrue(
                "no worker producer may be a COORDINATOR_REDUCE (that would never ship a shuffle → timeout)",
                level.leftProducer().getExecutionType() != StageExecutionType.COORDINATOR_REDUCE
                    && level.rightProducer().getExecutionType() != StageExecutionType.COORDINATOR_REDUCE
            );
        }
    }

    /** Builds {@code Project(Join(Project(Join(a,b)), c))} — a 3-way join with an explicit row-wise Project
     *  between the two join levels (the real PPL plan shape; each join's output is renamed). Shared key col0. */
    private RelNode makeThreeWayJoinWithMidProject(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        int aCols = aScan.getRowType().getFieldCount();
        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);
        // Row-wise identity Project over the bottom join (preserves col0 at position 0 so the top join keys
        // on it). This is the transparent op that must ride on the bottom worker.
        int abCols = ab.getRowType().getFieldCount();
        List<RexNode> midProjects = new java.util.ArrayList<>();
        for (int i = 0; i < abCols; i++) {
            midProjects.add(rexBuilder.makeInputRef(ab, i));
        }
        RelNode midProject = org.apache.calcite.rel.logical.LogicalProject.create(
            ab,
            List.of(),
            midProjects,
            ab.getRowType().getFieldNames()
        );

        RexNode abcCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, abCols)
        );
        return LogicalJoin.create(midProject, cScan, List.of(), abcCond, Set.of(), JoinRelType.INNER);
    }

    /** Builds the BUSHY tree {@code (A⋈B on col0) ⋈ (C⋈D on col0) on col0} — both sides contain a join, so
     *  it is NOT left-deep. All joins share key col0 (status). */
    private RelNode makeBushyFourWayJoin(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelNode dScan = stubScan(mockTable("d_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        int aCols = aScan.getRowType().getFieldCount();
        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);

        int cCols = cScan.getRowType().getFieldCount();
        RexNode cdCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, cCols)
        );
        RelNode cd = LogicalJoin.create(cScan, dScan, List.of(), cdCond, Set.of(), JoinRelType.INNER);

        int abCols = ab.getRowType().getFieldCount();
        // Top join: ab.col0 = cd.col0 (cd's col0 is at offset abCols in the joined row).
        RexNode topCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, abCols)
        );
        return LogicalJoin.create(ab, cd, List.of(), topCond, Set.of(), JoinRelType.INNER);
    }

    /**
     * Codex BLOCKER 2 regression: a decomposable aggregate grouped on a NON-join-key column over a
     * multi-way join ({@code A⋈B⋈C on col0 | stats count() by col1}). The aggregate's group key (col1)
     * differs from the join partitioning (col0). The pass must NOT insert a group-key re-shuffle (that
     * would be a non-join shuffle edge GeneralShuffleDAGRewriter can't wire → hang) — instead the PARTIAL
     * rides on the top-join worker partitioned by the JOIN key (Variant A: a decomposable PARTIAL is
     * correct under any partitioning; the FINAL gathers + re-merges). Asserts: exactly one PARTIAL + one
     * FINAL, the PARTIAL's input is the join directly (NO ShuffleExchange between the PARTIAL and the join),
     * and two worker tiers still promote.
     */
    public void testGeneralShuffle_aggGroupedOnNonJoinKey_noReshuffle() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeAggregateGroupedOnNonJoinKey(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode enforced = DistributionEnforcementPass.enforce(cbo, context.getDistributionTraitDef(), CLUSTER_DATA_NODES, 1L);

        List<org.opensearch.analytics.planner.rel.OpenSearchAggregate> aggs = findAll(
            enforced,
            org.opensearch.analytics.planner.rel.OpenSearchAggregate.class
        );
        long partials = aggs.stream().filter(a -> a.getMode() == org.opensearch.analytics.planner.rel.AggregateMode.PARTIAL).count();
        long finals = aggs.stream().filter(a -> a.getMode() == org.opensearch.analytics.planner.rel.AggregateMode.FINAL).count();
        assertEquals("one PARTIAL aggregate", 1, partials);
        assertEquals("one FINAL aggregate", 1, finals);
        // The PARTIAL must sit DIRECTLY over the join (no group-key re-shuffle between them) — Variant A.
        org.opensearch.analytics.planner.rel.OpenSearchAggregate partial = aggs.stream()
            .filter(a -> a.getMode() == org.opensearch.analytics.planner.rel.AggregateMode.PARTIAL)
            .findFirst()
            .orElseThrow();
        assertTrue(
            "PARTIAL rides directly on the join worker — NO group-key ShuffleExchange between PARTIAL and join",
            unwrap(partial.getInput(0)) instanceof OpenSearchJoin
        );
        // And the DAG still promotes two join worker tiers (the un-wireable agg-shuffle bug would have left
        // the PARTIAL stage consuming an un-promoted shuffle).
        QueryDAG dag = DAGBuilder.build(enforced, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);
        GeneralShuffleDAGRewriter.Structure structure = GeneralShuffleDAGRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );
        assertEquals("two join worker tiers promoted", 2, structure.buildLevels().size());
    }

    /**
     * Codex BLOCKER 1 regression: a window function ({@code RexOver}) over a distributed multi-way join.
     * The window-bearing {@code OpenSearchProject} requires {@code COORDINATOR+SINGLETON} input (a global
     * window frame can't run per-partition). Over a distributed join the pass MUST insert a gather (ER)
     * before the window — the bug was that the SINGLETON requirement went through the satisfies()-gated
     * buildEnforcer, which trusted the join's stale CBO coordSingleton trait and skipped the ER, leaving the
     * window running per-partition (silently wrong). Asserts the window Project's input is an
     * {@link OpenSearchExchangeReducer} (gathered), and the cascade still formed below it.
     */
    public void testEnforcementPass_windowOverJoinGathersBeforeWindow() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeWindowOverThreeWayJoin(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode enforced = DistributionEnforcementPass.enforce(cbo, context.getDistributionTraitDef(), CLUSTER_DATA_NODES, 1L);

        // The window-bearing project must sit over a gather, not a partitioned join.
        OpenSearchProject windowProject = findAll(enforced, OpenSearchProject.class).stream()
            .filter(p -> !findAll(p, OpenSearchJoin.class).isEmpty()) // the project above the join
            .findFirst()
            .orElseThrow(() -> new AssertionError("no window project found"));
        assertTrue(
            "window project must gather (ER) its distributed-join input before running the global window frame",
            unwrap(windowProject.getInput(0)) instanceof OpenSearchExchangeReducer
        );
        // The cascade still distributes BELOW the gather (the join itself is two binary tiers).
        List<OpenSearchJoin> joins = findAll(enforced, OpenSearchJoin.class);
        assertEquals("two joins distributed below the window gather", 2, joins.size());
        long binaryTiers = joins.stream()
            .filter(
                j -> unwrap(j.getInput(0)) instanceof OpenSearchShuffleExchange
                    && unwrap(j.getInput(1)) instanceof OpenSearchShuffleExchange
            )
            .count();
        assertEquals("both join levels are binary tiers below the gather", 2, binaryTiers);
    }

    /** Builds {@code Project(status, COUNT() OVER ())} over a 3-way INNER join — a global-window-frame
     *  RexOver above the join, which requires fully-gathered (SINGLETON) input. */
    private RelNode makeWindowOverThreeWayJoin(PlannerContext context) {
        RelNode join = makeThreeWayJoin(context);
        org.apache.calcite.rex.RexBuilder rb = join.getCluster().getRexBuilder();
        RexNode countOver = rb.makeOver(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            (org.apache.calcite.sql.SqlAggFunction) SqlStdOperatorTable.COUNT,
            List.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
            true,
            true,
            false,
            false,
            false
        );
        return LogicalProject.create(join, List.of(), List.of(rb.makeInputRef(join, 0), countOver), List.of("status", "cnt"));
    }

    /**
     * Codex re-review BLOCKER regression: a NON-decomposable SINGLE aggregate ({@code COUNT(DISTINCT col1)})
     * over a distributed multi-way join. A non-decomposable aggregate is split-ineligible
     * ({@code shouldSkipPartialFinalSplit} true for DISTINCT) so {@code requiredInputDistribution} is null —
     * but it must NOT ride on the join worker per-partition (that would compute per-partition distinct
     * counts with no FINAL merge → silently wrong). The pass must GATHER the join output and run the SINGLE
     * aggregate coordinator-centric. Asserts: exactly one SINGLE aggregate (no PARTIAL/FINAL split), and its
     * input is an {@link OpenSearchExchangeReducer} (gathered), not a partitioned join.
     */
    public void testEnforcementPass_nonDecomposableAggOverJoinGathers() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeDistinctCountOverThreeWayJoin(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode enforced = DistributionEnforcementPass.enforce(cbo, context.getDistributionTraitDef(), CLUSTER_DATA_NODES, 1L);

        List<org.opensearch.analytics.planner.rel.OpenSearchAggregate> aggs = findAll(
            enforced,
            org.opensearch.analytics.planner.rel.OpenSearchAggregate.class
        );
        assertEquals("non-decomposable agg stays a single SINGLE aggregate (no PARTIAL/FINAL split)", 1, aggs.size());
        org.opensearch.analytics.planner.rel.OpenSearchAggregate agg = aggs.get(0);
        assertEquals("the aggregate is SINGLE (not split)", org.opensearch.analytics.planner.rel.AggregateMode.SINGLE, agg.getMode());
        assertTrue(
            "non-decomposable agg must GATHER its distributed-join input (ER), NOT ride per-partition on the worker",
            unwrap(agg.getInput(0)) instanceof OpenSearchExchangeReducer
        );
    }

    /** Builds {@code Aggregate(COLLECT(col1) by col0)} over a 3-way join — COLLECT is STATE_EXPANDING, so
     *  {@code shouldSkipPartialFinalSplit} is true (genuinely non-decomposable: it can't merge per-partition
     *  partials additively), so the pass must gather, not split/ride-per-partition. (Single-arg
     *  COUNT(DISTINCT) is NOT a valid example — it's rewritten to the decomposable APPROX_COUNT_DISTINCT.) */
    private RelNode makeDistinctCountOverThreeWayJoin(PlannerContext context) {
        RelNode join = makeThreeWayJoin(context);
        // Multi-arg COUNT(DISTINCT col1, col2): residual DISTINCT (the single-arg OpenSearchDistinctCountRule
        // rewrite to decomposable APPROX_COUNT_DISTINCT does NOT apply to multi-arg), so
        // shouldSkipPartialFinalSplit returns true (aggCall.isDistinct()) → non-decomposable → must gather.
        // COUNT is a mock-backend-supported function (unlike COLLECT/LISTAGG), so the planner admits it.
        AggregateCall distinctMultiArg = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            /* distinct */ true,
            /* argList */ List.of(1, 2),
            /* filterArg */ -1,
            join,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "dc"
        );
        return LogicalAggregate.create(join, List.of(), ImmutableBitSet.of(0), null, List.of(distinctMultiArg));
    }

    private RelNode makeAggregateGroupedOnNonJoinKey(PlannerContext context) {
        RelNode join = makeThreeWayJoin(context); // joins on col0 (status)
        // GROUP BY col1 (size) — a DIFFERENT column than the join key col0.
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            join,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        return LogicalAggregate.create(join, List.of(), ImmutableBitSet.of(1), null, List.of(countCall));
    }

    /** Enforce + cut + fork/select + promote, returning the GeneralShuffleDAGRewriter structure. Mirrors the
     *  cascade-probe pipeline (no convertAll — the mock backend has no fragment convertor). */
    private GeneralShuffleDAGRewriter.Structure enforceAndPromote(RelNode logical, PlannerContext context) {
        RelNode cbo = runPlanner(logical, context);
        RelNode enforced = DistributionEnforcementPass.enforce(
            cbo,
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1L
        );
        QueryDAG dag = DAGBuilder.build(enforced, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);
        assertTrue("enforced DAG has a distributed join to promote", GeneralShuffleDAGRewriter.hasDistributedJoin(dag));
        return GeneralShuffleDAGRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );
    }

    /** Like makeThreeWayJoin but the TOP join uses the given type (INNER/LEFT/…). */
    private RelNode makeThreeWayJoinTopType(PlannerContext context, JoinRelType topType) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        int aCols = aScan.getRowType().getFieldCount();
        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);
        int abCols = ab.getRowType().getFieldCount();
        RexNode abcCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, abCols)
        );
        return LogicalJoin.create(ab, cScan, List.of(), abcCond, Set.of(), topType);
    }

    /** a⋈b on col0, then (ab)⋈c on col1 — the cascade re-keys at the top level. */
    private RelNode makeMixedKeyThreeWayJoin(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        int aCols = aScan.getRowType().getFieldCount();
        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);
        int abCols = ab.getRowType().getFieldCount();
        // top join keyed on col1 (size), a DIFFERENT column than the bottom (col0/status)
        RexNode abcCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, abCols + 1)
        );
        return LogicalJoin.create(ab, cScan, List.of(), abcCond, Set.of(), JoinRelType.INNER);
    }

    /**
     * Codex round-3 BLOCKER (a SELF-JOIN must produce TWO distinct producer stages, not one shared id that
     * enrichLevels would enrich as both left and right → only one sink → hang). Verifies the binary shuffle
     * transport is safe for {@code a ⋈ a on k}: DAGBuilder cuts each shuffle input into its OWN stage (one
     * id per shuffle-input cut), so the two producers have distinct ids even when both scan the same table.
     */
    public void testGeneralShuffle_selfJoinHasDistinctProducerStages() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);
        RelNode l = stubScan(mockTable("a_idx", "status", "size"));
        RelNode r = stubScan(mockTable("a_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, l.getRowType().getFieldCount())
        );
        RelNode logical = LogicalJoin.create(l, r, List.of(), cond, Set.of(), JoinRelType.INNER);
        RelNode cbo = runPlanner(logical, context);
        RelNode enforced = DistributionEnforcementPass.enforce(cbo, context.getDistributionTraitDef(), CLUSTER_DATA_NODES, 1L);
        QueryDAG dag = DAGBuilder.build(enforced, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        assertTrue("self-join over two large scans distributes", GeneralShuffleDAGRewriter.hasDistributedJoin(dag));
        GeneralShuffleDAGRewriter.Structure s = GeneralShuffleDAGRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (lvl, pc) -> nodeIds(pc)
        );
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = s.buildLevels();
        assertEquals("one worker tier for the self-join", 1, levels.size());
        CascadeShuffleDAGRewriter.WorkerLevel wl = levels.get(0);
        assertTrue(
            "self-join's two shuffle producers MUST be distinct stages (else one sink serves both sides → hang)",
            wl.leftProducer().getStageId() != wl.rightProducer().getStageId()
        );
    }

    private <T> List<T> findAll(RelNode root, Class<T> type) {
        List<T> out = new java.util.ArrayList<>();
        collect(unwrap(root), type, out);
        return out;
    }

    @SuppressWarnings("unchecked")
    private <T> void collect(RelNode node, Class<T> type, List<T> out) {
        if (type.isInstance(node)) {
            out.add((T) node);
        }
        for (RelNode input : node.getInputs()) {
            collect(unwrap(input), type, out);
        }
    }

    private static RelNode unwrap(RelNode node) {
        return org.opensearch.analytics.planner.RelNodeUtils.unwrapHep(node);
    }

    /**
     * Positive: {@code Aggregate(Join(Join(A,B), C))} — an Aggregate sits ABOVE the topmost join
     * (the TPC-H q3 shape: `… join … join … | stats … by …`). The aggregate runs on the COORDINATOR
     * after the worker join (only the join is lifted into a worker), so it must NOT block cascade
     * detection. Regression guard: an earlier version checked the whole stage-fragment ROOT for
     * partition-preservation and wrongly rejected this — q3 then mis-routed to the single-level
     * HashShuffleDispatch and failed "could not locate consumer stage". The agg-ABOVE-join case is
     * safe; only agg/sort BETWEEN join levels is not (covered by the negative tests).
     */
    public void testCascadeDetected_aggregateAboveTopJoin() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeAggregateOverThreeWayJoin(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);

        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        assertTrue(
            "Aggregate ABOVE the top join (q3 shape) must still be detected as a cascade — the agg runs"
                + " on the coordinator after the worker join, not per-partition",
            CascadeShuffleDAGRewriter.isCascade(dag)
        );
        // And it has the two worker tiers (inner + outer join).
        CascadeShuffleDAGRewriter.Structure structure = CascadeShuffleDAGRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );
        assertEquals("two join levels → two worker tiers", 2, structure.buildLevels().size());
    }

    /**
     * Negative: {@code Join(Aggregate(Join(A,B)), C)} — an Aggregate sits BETWEEN the two join
     * levels. Cascading it would run the GROUP BY once per hash partition (partitioned by the inner
     * join key, not the group key) → per-partition partial groups, silently wrong. The cascade-safe
     * gate must keep this shape OUT of the cascade (it stays on its CBO-chosen coord-centric path).
     */
    public void testCascadeRejected_aggregateBetweenJoins() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeJoinOverAggregateOverJoin(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);

        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        // The outer join must NOT be cascaded — the Aggregate between the levels makes per-partition
        // execution unsafe. isCascade requires >1 join-over-shuffles stage with a partition-preserving
        // root-to-join chain; the aggregate breaks that chain, so at most the inner join shuffles.
        assertFalse(
            "Join(Aggregate(Join)) must NOT be detected as a cascade — aggregate between joins is not partition-safe",
            CascadeShuffleDAGRewriter.isCascade(dag)
        );
    }

    /**
     * Negative (depth 4): {@code Join(Join(Aggregate(Join(A,B)), D), E)} — the Aggregate sits two
     * join levels down. An earlier guard returned true for ANY nested join, so the OUTERMOST join's
     * reducer input (the MIDDLE join) was accepted as "an OpenSearchJoin" even though that middle
     * join was never converted (its Aggregate side blocked it). That left an unlifted reduce stage
     * the dispatcher would enrich as a producer that never ships partitions (hang). The fixed guard
     * accepts a nested join only when it is itself an INNER join over two shuffles, so this whole
     * shape must stay OUT of the cascade. (codex R4 blocker #1)
     */
    public void testCascadeRejected_aggregateTwoLevelsDown() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "d_idx", 3, "e_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "d_idx", LARGE, "e_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeJoinOverJoinOverAggregateOverJoin(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);

        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        // No cascade: the aggregate between the lowest two join levels breaks the partition-preserving
        // chain, so the middle join is never a validated cascade level and the outer join must not lift.
        assertFalse(
            "Join(Join(Aggregate(Join))) must NOT cascade — aggregate breaks the chain at the middle join",
            CascadeShuffleDAGRewriter.isCascade(dag)
        );
    }

    /**
     * The TPC-H q5/q10 shape: a join-over-two-shuffles ({@code Join_bottom}) NESTED in the root
     * fragment UNDER a coordinator dimension join that keys on a DIFFERENT column (so the dimension
     * stayed a reducer, not a shuffle). Models q5's left-deep
     * {@code ((((cust⋈ord)⋈lineitem)⋈supplier)⋈nation)⋈region} reduced to one nesting level:
     * {@code Join_dim( Join_bottom(A⋈B⋈… via shuffle), reducer→D )}.
     *
     * <p>This is the case the {@code findNodes(...).size() != 2} guard wrongly rejected (the root
     * fragment has THREE stage-inputs: stage2 + stage3 feeding {@code Join_bottom}'s shuffles, plus
     * stage4 feeding the dimension reducer). The fix must:
     * <ul>
     *   <li>detect the cascade ({@code isCascade == true}) — recognize the nested join-over-two-
     *       shuffles even with extra reducer-fed stage-inputs;</li>
     *   <li>lift the NESTED {@code Join_bottom} (not the topmost {@code Join_dim}, whose right input
     *       is a reducer) into a worker tier;</li>
     *   <li>KEEP the dimension stage (stage4) as a coordinator-reduce child of the rebuilt root — NOT
     *       orphan it — so the dimension join runs on the coordinator over the worker's SINGLETON
     *       output.</li>
     * </ul>
     */
    public void testCascadeExtend_dimensionJoinAboveNestedShuffleJoin() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3, "d_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE, "d_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeFourWayDimTop(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        // The root fragment has 3 stage-inputs (stage2, stage3 under Join_bottom; stage4 under the
        // dimension reducer) — the old size()!=2 guard rejected this. The fix must still detect it.
        assertTrue(
            "a join-over-two-shuffles nested under a coordinator dimension join must be detected as a cascade",
            CascadeShuffleDAGRewriter.isCascade(dag)
        );

        // Capture the root stage's pre-rewrite dimension child stage ids (SINGLETON-reduce children of
        // the root that feed the dimension join, NOT the two shuffle children of Join_bottom).
        Stage preRoot = dag.rootStage();
        List<Integer> dimChildIdsBefore = preRoot.getChildStages()
            .stream()
            .filter(s -> s.getExchangeInfo() != null && s.getExchangeInfo().distributionType() == RelDistribution.Type.SINGLETON)
            .map(Stage::getStageId)
            .toList();
        assertEquals("expected exactly one reducer-fed dimension child stage on the root", 1, dimChildIdsBefore.size());
        int dimStageId = dimChildIdsBefore.get(0);

        CascadeShuffleDAGRewriter.Structure structure = CascadeShuffleDAGRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );

        // Two join levels lifted into workers: the inner A⋈B join (stage2) and Join_bottom.
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.buildLevels();
        assertEquals("two lifted join levels → two worker tiers", 2, levels.size());
        for (CascadeShuffleDAGRewriter.WorkerLevel level : levels) {
            assertEquals(
                "lifted join level must be a WORKER_FRAGMENT",
                StageExecutionType.WORKER_FRAGMENT,
                level.worker().getExecutionType()
            );
            assertEquals("lifted join level role SHUFFLE_WORKER", Stage.StageRole.SHUFFLE_WORKER, level.worker().getRole());
        }

        // The rebuilt root must keep the dimension stage as a coordinator-reduce child AND gain the
        // top worker. It must NOT be just [topWorker] (that would orphan the dimension stage).
        Stage newRoot = structure.dag().rootStage();
        CascadeShuffleDAGRewriter.WorkerLevel top = levels.get(1);
        assertEquals(
            "rebuilt root has two children: the top worker + the (kept) dimension reduce stage",
            2,
            newRoot.getChildStages().size()
        );
        assertTrue(
            "the lifted top worker must be a child of the rebuilt root",
            newRoot.getChildStages().stream().anyMatch(s -> s.getStageId() == top.worker().getStageId())
        );
        Stage keptDim = newRoot.getChildStages().stream().filter(s -> s.getStageId() == dimStageId).findFirst().orElse(null);
        assertTrue("the reducer-fed dimension stage must be KEPT as a root child, not orphaned", keptDim != null);
        assertEquals(
            "the kept dimension stage stays a coordinator-reduce (SINGLETON gather), not lifted",
            RelDistribution.Type.SINGLETON,
            keptDim.getExchangeInfo().distributionType()
        );
        // And the dimension stage is NOT one of the lifted workers (it stays coordinator-side).
        assertTrue(
            "the dimension stage must NOT be a lifted worker",
            levels.stream().noneMatch(l -> l.worker().getStageId() == dimStageId)
        );
    }

    /**
     * Like {@link #testCascadeExtend_dimensionJoinAboveNestedShuffleJoin} but with TWO stacked
     * coordinator dimension joins above {@code Join_bottom} (the real q5 stacks supplier/nation/
     * region — three). Exercises {@code isCoordinatorPathTo} walking through MULTIPLE coordinator
     * joins on the root→liftable-join path, and {@code rebuild} keeping BOTH dimension reduce stages
     * as coordinator-side children of the rebuilt root. Only {@code Join_bottom} (+ its lower cascade
     * level) is lifted into workers.
     */
    public void testCascadeExtend_twoStackedDimensionJoins() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3, "d_idx", 3, "e_idx", 3);
        Map<String, Long> rowCounts = new HashMap<>();
        for (String t : List.of("a_idx", "b_idx", "c_idx", "d_idx", "e_idx")) {
            rowCounts.put(t, LARGE);
        }
        PlannerContext context = buildMppContext(shardCounts, rowCounts);
        RelNode logical = makeFiveWayTwoDimsTop(context);
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        assertTrue(
            "nested join-over-two-shuffles under TWO stacked dimension joins must cascade",
            CascadeShuffleDAGRewriter.isCascade(dag)
        );

        List<Integer> dimChildIds = dag.rootStage()
            .getChildStages()
            .stream()
            .filter(s -> s.getExchangeInfo() != null && s.getExchangeInfo().distributionType() == RelDistribution.Type.SINGLETON)
            .map(Stage::getStageId)
            .toList();
        assertEquals("two reducer-fed dimension children on the root", 2, dimChildIds.size());

        CascadeShuffleDAGRewriter.Structure structure = CascadeShuffleDAGRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.buildLevels();
        assertEquals("only the bottom cascade lifts → two worker tiers", 2, levels.size());

        Stage newRoot = structure.dag().rootStage();
        // 1 top worker + 2 kept dimension stages = 3 children, none orphaned.
        assertEquals("rebuilt root keeps both dimension stages + the top worker", 3, newRoot.getChildStages().size());
        for (int dimId : dimChildIds) {
            Stage kept = newRoot.getChildStages().stream().filter(s -> s.getStageId() == dimId).findFirst().orElse(null);
            assertTrue("dimension stage " + dimId + " must be kept, not orphaned", kept != null);
            assertEquals(
                "kept dimension stage " + dimId + " stays SINGLETON coordinator-reduce",
                RelDistribution.Type.SINGLETON,
                kept.getExchangeInfo().distributionType()
            );
            assertTrue(
                "dimension stage " + dimId + " must NOT be a lifted worker",
                levels.stream().noneMatch(l -> l.worker().getStageId() == dimId)
            );
        }
    }

    private RelNode makeFiveWayTwoDimsTop(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelNode dScan = stubScan(mockTable("d_idx", "status", "size"));
        RelNode eScan = stubScan(mockTable("e_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        int aCols = aScan.getRowType().getFieldCount();
        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);
        int abCols = ab.getRowType().getFieldCount();
        RexNode abcCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, abCols)
        );
        RelNode abc = LogicalJoin.create(ab, cScan, List.of(), abcCond, Set.of(), JoinRelType.INNER);
        int abcCols = abc.getRowType().getFieldCount();
        RexNode abcdCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, abcCols + 1)
        );
        RelNode abcd = LogicalJoin.create(abc, dScan, List.of(), abcdCond, Set.of(), JoinRelType.INNER);
        int abcdCols = abcd.getRowType().getFieldCount();
        RexNode abcdeCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, abcdCols + 1)
        );
        return LogicalJoin.create(abcd, eScan, List.of(), abcdeCond, Set.of(), JoinRelType.INNER);
    }

    /**
     * The faithful TPC-H q10 root shape: {@code Aggregate(Join_dim(Join_bottom(shuffle,shuffle),
     * reducer→dim))} — an Aggregate sits ABOVE the dimension join, which sits above the nested
     * {@code Join_bottom}. Matches the REAL captured q10 DAG (verified from the sf=10 cluster log:
     * {@code Project(Sort(Sort(Sort(Aggregate(Project(Join_nation(Join_bottom(Shuffle→2, Shuffle→3),
     * Reducer→4)))))))}). The Agg/Sort/Project all run on the coordinator AFTER the worker gathers
     * {@code Join_bottom}'s output; only {@code Join_bottom} (+ its lower cascade level) lifts. Guards
     * against a regression where the coordinator-side Agg/Sort/Project above the dim join blocks
     * cascade detection.
     */
    public void testCascadeExtend_aggregateAboveNestedDimensionJoin() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3, "d_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE, "d_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);
        RelNode fourWay = makeFourWayDimTop(context);
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            fourWay,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        RelNode logical = LogicalAggregate.create(fourWay, List.of(), ImmutableBitSet.of(0), null, List.of(countCall));
        RelNode cbo = runPlanner(logical, context);
        RelNode rewritten = CascadeShufflePlanRewriter.rewrite(cbo, CLUSTER_DATA_NODES);
        QueryDAG dag = DAGBuilder.build(rewritten, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), false);

        assertTrue(
            "Aggregate above a nested dimension join over a cascade (real q10 shape) must still cascade",
            CascadeShuffleDAGRewriter.isCascade(dag)
        );
        // Capture the pre-rewrite dimension child stage id (the reducer-fed SINGLETON child that is
        // NOT one of Join_bottom's two HASH shuffle producers).
        int dimStageId = dag.rootStage()
            .getChildStages()
            .stream()
            .filter(s -> s.getExchangeInfo() != null && s.getExchangeInfo().distributionType() == RelDistribution.Type.SINGLETON)
            .map(Stage::getStageId)
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected a reducer-fed dimension child stage on the root"));

        CascadeShuffleDAGRewriter.Structure structure = CascadeShuffleDAGRewriter.rewriteStructure(
            dag,
            context.getCapabilityRegistry(),
            (levelIndex, partitionCount) -> nodeIds(partitionCount)
        );
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.buildLevels();
        assertEquals("only the bottom cascade lifts → two worker tiers", 2, levels.size());
        // The dimension reduce stage stays a coordinator child of the rebuilt root (kept, not
        // orphaned) and is NOT lifted into a worker.
        Stage newRoot = structure.dag().rootStage();
        Stage keptDim = newRoot.getChildStages().stream().filter(s -> s.getStageId() == dimStageId).findFirst().orElse(null);
        assertTrue("the dimension reduce stage must be kept on the coordinator, not orphaned", keptDim != null);
        assertTrue(
            "the dimension reduce stage must NOT be a lifted worker",
            levels.stream().noneMatch(l -> l.worker().getStageId() == dimStageId)
        );
    }

    /** Builds Join(Join(Join(a,b) on col0, c) on col0, d) on col1 (=size) — the top dimension join
     *  keys on a DIFFERENT column than the bottom cascade. Models q5's left-deep shape. */
    private RelNode makeFourWayDimTop(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelNode dScan = stubScan(mockTable("d_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        int aCols = aScan.getRowType().getFieldCount();
        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);

        int abCols = ab.getRowType().getFieldCount();
        RexNode abcCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, abCols)
        );
        RelNode abc = LogicalJoin.create(ab, cScan, List.of(), abcCond, Set.of(), JoinRelType.INNER);

        int abcCols = abc.getRowType().getFieldCount();
        // Outer join on col1 (size) = d.col1 — DIFFERENT key from the cascade (col0).
        RexNode abcdCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, abcCols + 1)
        );
        return LogicalJoin.create(abc, dScan, List.of(), abcdCond, Set.of(), JoinRelType.INNER);
    }

    private static List<String> nodeIds(int partitionCount) {
        List<String> ids = new java.util.ArrayList<>(partitionCount);
        for (int p = 0; p < partitionCount; p++) {
            ids.add("node-" + (p % CLUSTER_DATA_NODES));
        }
        return ids;
    }

    /** Builds Join(Join(a,b) on a.0=b.0, c) on (join.0 = c.0), all INNER equi. */
    private RelNode makeThreeWayJoin(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        int aCols = aScan.getRowType().getFieldCount();
        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);

        int abCols = ab.getRowType().getFieldCount();
        RexNode abcCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, abCols)
        );
        return LogicalJoin.create(ab, cScan, List.of(), abcCond, Set.of(), JoinRelType.INNER);
    }

    /** Builds Aggregate(Join(Join(a,b), c) by col0, count) — an Aggregate ABOVE the top join (the
     *  TPC-H q3 shape). The agg runs on the coordinator after the worker join, so cascade detection
     *  must NOT be blocked by it. */
    private RelNode makeAggregateOverThreeWayJoin(PlannerContext context) {
        RelNode join = makeThreeWayJoin(context);
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            join,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        return LogicalAggregate.create(join, List.of(), ImmutableBitSet.of(0), null, List.of(countCall));
    }

    /** Builds Join(Aggregate(Join(a,b) by col0, count), c) — an Aggregate between the join levels.
     *  Models PPL {@code a join b | stats count() by g | join c}. */
    private RelNode makeJoinOverAggregateOverJoin(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        int aCols = aScan.getRowType().getFieldCount();
        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);

        // GROUP BY col0, COUNT() — collapses A⋈B to one row per group key.
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            ab,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        RelNode agg = LogicalAggregate.create(ab, List.of(), ImmutableBitSet.of(0), null, List.of(countCall));

        // Outer join: agg's group-key column (output index 0) = c.col0.
        int aggCols = agg.getRowType().getFieldCount();
        RexNode aggcCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aggCols)
        );
        return LogicalJoin.create(agg, cScan, List.of(), aggcCond, Set.of(), JoinRelType.INNER);
    }

    /** Builds Join(Join(Aggregate(Join(a,b)), d), e) — the Aggregate is two join levels down, so the
     *  MIDDLE join sits over a non-partition-preserving (aggregate) side. Models
     *  {@code a join b | stats count() by g | join d | join e}. */
    private RelNode makeJoinOverJoinOverAggregateOverJoin(PlannerContext context) {
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode dScan = stubScan(mockTable("d_idx", "status", "size"));
        RelNode eScan = stubScan(mockTable("e_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        int aCols = aScan.getRowType().getFieldCount();
        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);

        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            ab,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        RelNode agg = LogicalAggregate.create(ab, List.of(), ImmutableBitSet.of(0), null, List.of(countCall));

        // Middle join: agg ⋈ d on group-key = d.col0.
        int aggCols = agg.getRowType().getFieldCount();
        RexNode aggdCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aggCols)
        );
        RelNode mid = LogicalJoin.create(agg, dScan, List.of(), aggdCond, Set.of(), JoinRelType.INNER);

        // Outer join: mid ⋈ e on col0 = e.col0.
        int midCols = mid.getRowType().getFieldCount();
        RexNode mideCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, midCols)
        );
        return LogicalJoin.create(mid, eScan, List.of(), mideCond, Set.of(), JoinRelType.INNER);
    }

    private PlannerContext buildMppContext(Map<String, Integer> shardCounts, Map<String, Long> rowCounts) {
        ClusterState state = mockClusterStateWithDataNodes(shardCounts);
        Settings settings = Settings.builder().put("analytics.mpp.enabled", true).build();
        ToLongFunction<String> rowCountLookup = name -> rowCounts.getOrDefault(name, PlannerContext.UNKNOWN_ROW_COUNT);
        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = FieldStorageResolver::new;
        AnalyticsSearchBackendPlugin shuffleAware = new ShuffleAwareDataFusionBackend(CLUSTER_DATA_NODES);
        CapabilityRegistry registry = new CapabilityRegistry(List.of(shuffleAware, LUCENE), fieldStorageFactory);
        return new PlannerContext(registry, state, settings, rowCountLookup, /* profiling */ false);
    }

    private static ClusterState mockClusterStateWithDataNodes(Map<String, Integer> shardCounts) {
        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(state.metadata()).thenReturn(metadata);

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(state.nodes()).thenReturn(nodes);
        Map<String, DiscoveryNode> dataNodes = new HashMap<>();
        for (int i = 0; i < CLUSTER_DATA_NODES; i++) {
            dataNodes.put("node-" + i, mock(DiscoveryNode.class));
        }
        when(nodes.getDataNodes()).thenReturn(dataNodes);

        for (Map.Entry<String, Integer> entry : shardCounts.entrySet()) {
            String indexName = entry.getKey();
            int shardCount = entry.getValue();
            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(indexMetadata.getIndex()).thenReturn(new Index(indexName, indexName + "-uuid"));
            when(indexMetadata.getNumberOfShards()).thenReturn(shardCount);
            MappingMetadata mappingMetadata = mock(MappingMetadata.class);
            when(mappingMetadata.sourceAsMap()).thenReturn(Map.of("properties", intFields()));
            when(indexMetadata.mapping()).thenReturn(mappingMetadata);
            when(indexMetadata.getSettings()).thenReturn(
                Settings.builder()
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene")
                    .build()
            );
            when(metadata.index(indexName)).thenReturn(indexMetadata);
        }
        return state;
    }

    private static class ShuffleAwareDataFusionBackend extends MockDataFusionBackend {
        private final int parallelism;

        ShuffleAwareDataFusionBackend(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public int defaultShuffleParallelism(ClusterState state) {
            return parallelism;
        }
    }
}
