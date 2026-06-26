/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import com.google.common.collect.ImmutableList;
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
import org.opensearch.analytics.exec.join.DistributionEnforcementPass;
import org.opensearch.analytics.exec.join.GeneralShuffleDAGRewriter;
import org.opensearch.analytics.exec.join.ShuffleEnrichment;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.PlanAlternativeSelector;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.ShardTargetResolver;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.dag.WorkerTargetResolver;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
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
 * Locks in the general hash-shuffle plan shape for multi-way INNER equi-joins (e.g.
 * {@code Join(Join(A,B), C)}), scheduled by the general distributed scheduler:
 * <ol>
 *   <li>{@link DistributionEnforcementPass} co-partitions each join level (post-CBO), inserting the
 *       hash-shuffle inputs so {@code DAGBuilder} cuts a nested-shuffle DAG.</li>
 *   <li>{@link GeneralShuffleDAGRewriter#rewriteStructure} promotes each join level into its own worker
 *       tier in place; {@link ShuffleEnrichment.WorkerLevel} describes each tier (worker stage + its two
 *       shuffle producers).</li>
 * </ol>
 * Validates the structure (worker count, execution types, producer wiring, coordinator gather)
 * without the convert pipeline (the mock backend has no fragment convertor; conversion + actual
 * row-count parity are exercised on the live cluster).
 */
public class CascadeShuffleProbeTests extends BasePlannerRulesTests {

    private static final int CLUSTER_DATA_NODES = 3;
    private static final long LARGE = 10_000_000L;
    private static final long SMALL = 1_000L;

    /**
     * Option B (general enforcement pass): a 3-way shared-key join scheduled by the general
     * {@link DistributionEnforcementPass}. The pass must produce a cascade of BINARY worker tiers —
     * BOTH joins over two ShuffleExchange inputs.
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
        List<ShuffleEnrichment.WorkerLevel> levels = structure.buildLevels();
        assertEquals("two binary join tiers → two worker tiers", 2, levels.size());
        for (ShuffleEnrichment.WorkerLevel level : levels) {
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
        ShuffleEnrichment.WorkerLevel deepest = levels.get(0);
        ShuffleEnrichment.WorkerLevel top = levels.get(1);
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
        List<ShuffleEnrichment.WorkerLevel> levels = structure.buildLevels();
        assertEquals("two binary join tiers → two worker tiers", 2, levels.size());

        // The top worker fragment carries the PARTIAL aggregate above its join.
        ShuffleEnrichment.WorkerLevel top = levels.get(levels.size() - 1);
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
        List<ShuffleEnrichment.WorkerLevel> levels = structure.buildLevels();
        assertEquals("bushy (A⋈B)⋈(C⋈D) → three binary worker tiers", 3, levels.size());
        for (ShuffleEnrichment.WorkerLevel level : levels) {
            assertEquals("each tier is a WORKER_FRAGMENT", StageExecutionType.WORKER_FRAGMENT, level.worker().getExecutionType());
            assertEquals("each tier role SHUFFLE_WORKER", Stage.StageRole.SHUFFLE_WORKER, level.worker().getRole());
        }
        // The top tier (level 2, last bottom-up) consumes the two sub-join workers (levels 0 and 1) as BOTH
        // its producers — the bushy signature (vs the left-deep cascade, where only the left producer is an
        // intermediate worker).
        ShuffleEnrichment.WorkerLevel top = levels.get(2);
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
        List<ShuffleEnrichment.WorkerLevel> levels = structure.buildLevels();
        // TWO worker tiers — NOT three (a spurious gather stage from the mid-Project would break promotion).
        assertEquals("intermediate Project must not insert a gather tier → still two worker tiers", 2, levels.size());
        for (ShuffleEnrichment.WorkerLevel level : levels) {
            assertEquals("each tier is a WORKER_FRAGMENT", StageExecutionType.WORKER_FRAGMENT, level.worker().getExecutionType());
        }
        // The bottom worker (level 0) must consume two leaf shard scans directly — proving the mid-Project
        // did NOT cut a gather stage between the bottom join and its scans.
        ShuffleEnrichment.WorkerLevel deepest = levels.get(0);
        assertEquals(
            "bottom worker left producer is a shard fragment (no gather inserted by the mid-Project)",
            StageExecutionType.SHARD_FRAGMENT,
            deepest.leftProducer().getExecutionType()
        );
        // The top worker consumes the bottom worker as one of its two producers (the inter-tier shuffle), not
        // a coordinator-reduce stage.
        ShuffleEnrichment.WorkerLevel top = levels.get(1);
        boolean topConsumesDeepest = top.leftProducer().getStageId() == deepest.worker().getStageId()
            || top.rightProducer().getStageId() == deepest.worker().getStageId();
        assertTrue("top worker consumes the bottom worker directly (no gather between tiers)", topConsumesDeepest);
        for (ShuffleEnrichment.WorkerLevel level : levels) {
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

    /**
     * sf=10 q3 BLOCKER regression: a filtered scan feeding a join must stay a SHARD producer, not become a
     * coordinator reduce. CBO gathers the scan (ER) under the filter; the pass peels that ER, and the filter
     * (transparent, shard-local child) must RIDE on the shards — so the join's shuffle wraps
     * {@code Shuffle(Filter(scan))} with NO ExchangeReducer between the filter and the scan. The bug: the
     * filter was gathered (step 3b), producing {@code Shuffle(Filter(ER(scan)))} → DAGBuilder cut a
     * coordinator ReduceStageExecution as the shuffle producer, which never ships → worker timeout.
     */
    public void testEnforcementPass_filteredScanJoinInputStaysShardProducer() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE);
        // Force the SHUFFLE path: the `status > 5` filter shrinks the build side under the 64 MiB broadcast
        // cap, so CBO would otherwise pick BROADCAST (and the pass now legitimately preserves it). A tiny cap
        // suppresses the broadcast alternative so CBO shuffles — the producer path this test guards.
        PlannerContext context = buildMppContext(shardCounts, rowCounts, /* maxBroadcastBytes */ 1L);

        // a_idx | where status > 5 | join on status = b.status b_idx
        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode filterCond = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeLiteral(5, intType, false)
        );
        RelNode filtered = org.apache.calcite.rel.logical.LogicalFilter.create(aScan, filterCond);
        int aCols = filtered.getRowType().getFieldCount();
        RexNode joinCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aCols)
        );
        RelNode logical = LogicalJoin.create(filtered, bScan, List.of(), joinCond, Set.of(), JoinRelType.INNER);
        RelNode cbo = runPlanner(logical, context);
        RelNode enforced = DistributionEnforcementPass.enforce(cbo, context.getDistributionTraitDef(), CLUSTER_DATA_NODES, 1L);

        // The filter feeding the join must sit DIRECTLY over the scan under its shuffle — no ER between them.
        org.opensearch.analytics.planner.rel.OpenSearchFilter filter = findAll(
            enforced,
            org.opensearch.analytics.planner.rel.OpenSearchFilter.class
        ).stream().findFirst().orElseThrow(() -> new AssertionError("no filter in enforced plan"));
        assertTrue(
            "filtered-scan join input must stay shard-local: Filter directly over the scan (no gather ER between)",
            unwrap(filter.getInput(0)) instanceof org.opensearch.analytics.planner.rel.OpenSearchTableScan
        );
        // And the filter is under a ShuffleExchange (the join shuffles it from the shards).
        boolean filterUnderShuffle = findAll(enforced, OpenSearchShuffleExchange.class).stream()
            .anyMatch(sh -> !findAll(sh, org.opensearch.analytics.planner.rel.OpenSearchFilter.class).isEmpty());
        assertTrue("the filtered scan is shuffled (join hash-ships it from the shards)", filterUnderShuffle);
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

    /**
     * sf=10 bucket-A BLOCKER regression: a JOIN whose input is a GATHERED sub-stage (a decorrelated
     * subquery's aggregate — TPC-H q4 `exists`→SEMI, q22 `not exists`→ANTI, q2/q15 scalar subqueries) must
     * NOT be distributed. {@code Join(Aggregate(Join(A,B)), C)} — the top join's left input is an aggregate
     * over a join, which the pass gathers to COORDINATOR+SINGLETON (a ReduceStageExecution at DAG time). A
     * reduce stage emits to its parent sink and CANNOT ship a hash shuffle, so distributing the top join
     * would leave its worker awaiting a producer that never fires (`ShuffleScanHandler timed out`). The
     * shippable-producer gate (step 3d) must keep the TOP join coord-centric: its inputs are NOT shuffled.
     * (The inner A⋈B join is shard-local on both sides, so it may still distribute — that's fine.)
     */
    public void testEnforcementPass_joinOverGatheredAggregateStaysCoordCentric() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode logical = makeJoinOverAggregateOverJoin(context); // Join(Aggregate(Join(A,B)), C)
        RelNode cbo = runPlanner(logical, context);
        RelNode enforced = DistributionEnforcementPass.enforce(cbo, context.getDistributionTraitDef(), CLUSTER_DATA_NODES, 1L);

        // The TOP join (the one whose left input is the aggregate) must have NEITHER input shuffled — it
        // gathers and runs coord-centric. Identify it as the join that contains the OpenSearchAggregate.
        OpenSearchJoin topJoin = findAll(enforced, OpenSearchJoin.class).stream()
            .filter(j -> !findAll(j, org.opensearch.analytics.planner.rel.OpenSearchAggregate.class).isEmpty())
            .findFirst()
            .orElseThrow(() -> new AssertionError("no join-over-aggregate found"));
        assertFalse(
            "top join's left input (the gathered aggregate sub-stage) must NOT be shuffled",
            unwrap(topJoin.getInput(0)) instanceof OpenSearchShuffleExchange
        );
        assertFalse(
            "top join's right input must NOT be shuffled either (join stays coord-centric when a producer can't ship)",
            unwrap(topJoin.getInput(1)) instanceof OpenSearchShuffleExchange
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
        List<ShuffleEnrichment.WorkerLevel> levels = s.buildLevels();
        assertEquals("one worker tier for the self-join", 1, levels.size());
        ShuffleEnrichment.WorkerLevel wl = levels.get(0);
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

    private static List<String> nodeIds(int partitionCount) {
        List<String> ids = new java.util.ArrayList<>(partitionCount);
        for (int p = 0; p < partitionCount; p++) {
            ids.add("node-" + (p % CLUSTER_DATA_NODES));
        }
        return ids;
    }

    /**
     * BROADCAST coverage (E1): when CBO picks BROADCAST for a small×large INNER equi-join, the merged
     * enforcement pass PRESERVES that decision (step 3b-bcast) — re-emitting
     * {@code Join(OpenSearchBroadcastExchange(build), probe)} rather than re-driving the join through the
     * shuffle algebra. Asserts exactly one broadcast exchange survives, NO shuffle is introduced, and one
     * join input is the broadcast (build) while the other stays bare (shard-local probe).
     */
    public void testEnforcementPass_broadcastWinnerIsPreserved() {
        Map<String, Integer> shardCounts = Map.of("small_idx", 3, "large_idx", 3);
        Map<String, Long> rowCounts = Map.of("small_idx", SMALL, "large_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode cbo = runPlanner(makeTwoWayJoin(context, "small_idx", "large_idx"), context);
        assertFalse(
            "precondition: CBO must pick BROADCAST for small×large (else the probe is vacuous)",
            findAll(cbo, OpenSearchBroadcastExchange.class).isEmpty()
        );

        RelNode enforced = DistributionEnforcementPass.enforce(cbo, context.getDistributionTraitDef(), CLUSTER_DATA_NODES, 1L);

        assertEquals(
            "general pass must preserve the CBO broadcast exchange (1):\n" + org.apache.calcite.plan.RelOptUtil.toString(enforced),
            1,
            findAll(enforced, OpenSearchBroadcastExchange.class).size()
        );
        assertTrue(
            "general pass must NOT introduce a shuffle for a broadcast-winner:\n" + org.apache.calcite.plan.RelOptUtil.toString(enforced),
            findAll(enforced, OpenSearchShuffleExchange.class).isEmpty()
        );
        OpenSearchJoin join = findAll(enforced, OpenSearchJoin.class).get(0);
        boolean leftBroadcast = unwrap(join.getInput(0)) instanceof OpenSearchBroadcastExchange;
        boolean rightBroadcast = unwrap(join.getInput(1)) instanceof OpenSearchBroadcastExchange;
        assertTrue("exactly one join input is a broadcast exchange (the build side)", leftBroadcast ^ rightBroadcast);
    }

    /**
     * BROADCAST under a shuffle cascade (E1): a 3-way where CBO broadcasts a small build at the BOTTOM join
     * while the top join shuffles. Unlike the earlier reverted attempt (which GATED broadcast off here to
     * avoid an un-runnable shape), the unified dispatcher now COMPOSES broadcast-under-shuffle, so the pass
     * preserves BOTH: the bottom broadcast AND the top shuffle survive in one enforced DAG. Asserts a
     * broadcast exchange is present AND at least one shuffle is present (the mixed shape).
     */
    public void testEnforcementPass_broadcastUnderCascadeIsPreserved() {
        // a_idx (small build) ⋈ b_idx (large) at the bottom → broadcast-eligible; ⋈ c_idx (large) on top → shuffle.
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", SMALL, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode cbo = runPlanner(makeThreeWayJoin(context), context);
        RelNode enforced = DistributionEnforcementPass.enforce(cbo, context.getDistributionTraitDef(), CLUSTER_DATA_NODES, 1L);

        // The mixed shape: a preserved broadcast AND a shuffle in the same enforced DAG. (Exact counts depend
        // on CBO's per-join choice; the contract is that broadcast is NOT stripped and shuffle still forms.)
        boolean hasBroadcast = !findAll(enforced, OpenSearchBroadcastExchange.class).isEmpty();
        boolean hasShuffle = !findAll(enforced, OpenSearchShuffleExchange.class).isEmpty();
        assertTrue(
            "a broadcast-under-cascade plan must preserve BOTH the broadcast and a shuffle (mixed shape):\n"
                + org.apache.calcite.plan.RelOptUtil.toString(enforced),
            hasBroadcast && hasShuffle
        );
    }

    /**
     * DAG-cut regression for broadcast-under-shuffle (the q3/q8/q9 fix): when a shuffle producer's fragment
     * is {@code Join(BroadcastScan(build), shardScan)} — a broadcast nested under a shuffle tier — the
     * producer stage must be cut as a {@link StageExecutionType#SHARD_FRAGMENT} with a
     * {@link ShardTargetResolver} (it scans a shard table and ships its hash partition), NOT a
     * {@code COORDINATOR_REDUCE}. {@code DAGBuilder.cutShuffle} decides this by {@code fragmentHasShardScan},
     * not by child-stage count: the {@code BROADCAST_BUILD} grandchild is a side-input, not a streamed
     * child, so its presence must not flip the producer to a reduce. Before the fix, such a producer became a
     * {@code COORDINATOR_REDUCE} that — once {@code UnifiedDispatch} strips the captured build child —
     * had zero children and threw "COORDINATOR_REDUCE stage N expected at least one child, got zero".
     */
    public void testDagCut_broadcastUnderShuffleProducerIsShardFragment() {
        // a_idx (small build) ⋈ b_idx (large) at the bottom → broadcast-eligible; ⋈ c_idx (large) on top → shuffle.
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", SMALL, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = buildMppContext(shardCounts, rowCounts);

        RelNode cbo = runPlanner(makeThreeWayJoin(context), context);
        RelNode enforced = DistributionEnforcementPass.enforce(cbo, context.getDistributionTraitDef(), CLUSTER_DATA_NODES, 1L);
        QueryDAG dag = DAGBuilder.build(enforced, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);

        // Find the stage whose fragment contains an OpenSearchBroadcastScan AND a shard TableScan — the
        // broadcast-probe-that-is-also-a-shuffle-producer. There must be at least one (the enforced plan
        // preserved a broadcast under a shuffle; if CBO declined broadcast this test is vacuous, guarded
        // by the precondition).
        List<Stage> broadcastProducers = new java.util.ArrayList<>();
        collectStages(dag.rootStage(), s -> {
            RelNode f = s.getFragment();
            return f != null
                && !RelNodeUtils.findNodes(f, OpenSearchBroadcastScan.class).isEmpty()
                && !RelNodeUtils.findNodes(f, OpenSearchTableScan.class).isEmpty();
        }, broadcastProducers);
        assertFalse(
            "precondition: a broadcast nested under a shuffle producer must exist in the enforced DAG\n" + dag,
            broadcastProducers.isEmpty()
        );

        for (Stage producer : broadcastProducers) {
            assertEquals(
                "broadcast-under-shuffle producer must be a SHARD_FRAGMENT (scans shards, ships its partition), "
                    + "not a COORDINATOR_REDUCE:\n"
                    + dag,
                StageExecutionType.SHARD_FRAGMENT,
                producer.getExecutionType()
            );
            assertTrue(
                "broadcast-under-shuffle producer must have a ShardTargetResolver",
                producer.getTargetResolver() instanceof ShardTargetResolver
            );
        }
    }

    /** Recursively collects stages whose predicate holds (test helper for DAG-shape assertions). */
    private static void collectStages(Stage stage, java.util.function.Predicate<Stage> pred, List<Stage> out) {
        if (pred.test(stage)) {
            out.add(stage);
        }
        for (Stage child : stage.getChildStages()) {
            collectStages(child, pred, out);
        }
    }

    /** Two-way INNER equi-join on col0 over two scans (mirrors JoinStrategyCBOSelectionTests.makeJoin). */
    private RelNode makeTwoWayJoin(PlannerContext context, String leftIdx, String rightIdx) {
        RelNode leftScan = stubScan(mockTable(leftIdx, "status", "size"));
        RelNode rightScan = stubScan(mockTable(rightIdx, "status", "size"));
        int leftCols = leftScan.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftCols)
        );
        return LogicalJoin.create(leftScan, rightScan, List.of(), condition, Set.of(), JoinRelType.INNER);
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

    private PlannerContext buildMppContext(Map<String, Integer> shardCounts, Map<String, Long> rowCounts) {
        return buildMppContext(shardCounts, rowCounts, -1L);
    }

    /**
     * @param maxBroadcastBytes when {@code >= 0}, sets {@code analytics.mpp.broadcast.max_bytes} — a tiny
     *     value suppresses the broadcast alternative (build can't fit the cap) so CBO must shuffle; a test
     *     that wants the SHUFFLE filtered-scan-producer path uses this so the broadcast-preserve branch
     *     (which legitimately fires for a small filtered build) doesn't take the plan. {@code < 0} leaves
     *     the production default (64 MiB).
     */
    private PlannerContext buildMppContext(Map<String, Integer> shardCounts, Map<String, Long> rowCounts, long maxBroadcastBytes) {
        ClusterState state = mockClusterStateWithDataNodes(shardCounts);
        Settings.Builder settingsBuilder = Settings.builder().put("analytics.mpp.enabled", true);
        if (maxBroadcastBytes >= 0) {
            settingsBuilder.put("analytics.mpp.broadcast.max_bytes", maxBroadcastBytes + "b");
        }
        Settings settings = settingsBuilder.build();
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
