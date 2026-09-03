/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchUnion;
import org.opensearch.analytics.spi.AggregateFunction;

import java.util.List;

/** Splits an {@link OpenSearchAggregate} into PARTIAL + FINAL when the input is partitioned. */
public class OpenSearchAggregateSplitRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchAggregateSplitRule(PlannerContext context) {
        super(operand(OpenSearchAggregate.class, operand(RelNode.class, any())), "OpenSearchAggregateSplitRule");
        this.context = context;
    }

    /**
     * Matches any SINGLE-mode aggregate.
     *
     * <p><b>Do NOT gate this on {@code analytics.mpp.shuffle.aggregate.enabled}.</b> {@link #onMatch}
     * registers the PARTIAL/FINAL split AND the {@code singleOnSingleton} gather-then-aggregate plan, so
     * suppressing the whole rule also removes the ONLY coordinator-centric alternative, leaving just the
     * SINGLE aggregate the marking phase placed over RANDOM input.
     * {@link OpenSearchAggregate#computeSelfCost} prices that at infinity (correctly — per-shard partials
     * would never merge), so no legal plan remains and the query dies with
     * {@code CannotPlanException: … the cost is still infinite} instead of routing coord-centric. It took out
     * unrelated shapes too: any multi-shard windowed or joined query planned while the toggle was off. The
     * sub-toggle is honoured in {@link #splitSuppressedBySubToggle} instead, which suppresses only the SPLIT.
     */
    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        return aggregate.getMode() == AggregateMode.SINGLE;
    }

    /**
     * Per-strategy sub-toggle {@code analytics.mpp.shuffle.aggregate.enabled}: when off, the aggregate runs
     * coordinator-centric (gather, then one SINGLE aggregate) while a join BELOW it still distributes.
     *
     * <p>Owned by the RULE rather than the post-CBO pass so that distributed-aggregate FORMATION is decided in
     * one place — a prerequisite for retiring the pass's {@code splitAggregate}. It suppresses only the
     * PARTIAL/FINAL alternative; {@link #onMatch} still registers {@code singleOnSingleton}, which is what
     * makes this safe where gating {@link #matches} was not.
     *
     * <p>Scoped to MPP being on, per this setting's own contract ("only has effect when MPP is on"): with MPP
     * off the PARTIAL/FINAL split is just the ordinary shard-partial / coordinator-final path and must keep
     * working, so the sub-toggle must not degrade it into gathering whole indices to the coordinator.
     */
    private boolean splitSuppressedBySubToggle() {
        return AnalyticsSettings.MPP_ENABLED.get(context.getSettings())
            && !AnalyticsSettings.MPP_SHUFFLE_AGGREGATE_ENABLED.get(context.getSettings());
    }

    /**
     * True when PARTIAL/FINAL split would yield a malformed row type or invalid aggregate
     * semantics. In those cases {@link #onMatch} still produces the SINGLE+SINGLETON
     * alternative (so the planner can route shard input through a coordinator gather), but
     * skips the PARTIAL+ER+FINAL alternative.
     *
     * <p>Two cases are unsafe today:
     * <ul>
     *   <li><b>percentile_approx</b> is a 2-arg aggregate (field, percent) whose FINAL phase
     *       needs (tdigest_state, percent_literal). {@code AggregateDecompositionResolver}'s
     *       single-field rewrite paths only produce a single-arg FINAL call, yielding
     *       {@code "Type mismatch: rel rowtype: RecordType(BIGINT p50, BIGINT p50_0) NOT NULL,
     *       equiv rowtype: RecordType(INTEGER bucket, BIGINT p50)"}. Other aggCalls in the
     *       same Aggregate (SUM, AVG, etc.) inherit the single-stage execution.</li>
     *   <li><b>Cross-family non-prefix groupSet</b>: PARTIAL's output places group keys at
     *       positions {@code [0..groupCount)}. FINAL reuses ORIGINAL's groupSet against
     *       PARTIAL's output. When an input column at index {@code k >= groupCount} is a group
     *       key (e.g. {@code groupSet={2}, groupCount=1}), PARTIAL's output at index {@code k}
     *       is an agg-result instead, and Calcite's row-type equivalence check fires only if
     *       that agg-result's {@link SqlTypeFamily} differs from the ORIGINAL input column's
     *       family. PPL {@code timechart}'s no-{@code by} form trips this: the Project below
     *       the Aggregate keeps the raw {@code @timestamp} (DATETIME family) at position 0
     *       and materializes {@code SPAN(@timestamp)} at a later position; the agg result at
     *       that later position is {@code DOUBLE} (NUMERIC family) → cross-family mismatch
     *       ({@code "Type mismatch ... DOUBLE -> TIMESTAMP(0)"}). Same-family non-prefix
     *       cases (e.g. {@code group={1}} with both columns INTEGER + a NUMERIC agg) pass
     *       Calcite's relaxed numeric type check and don't need the skip — see
     *       {@code PlanShapeTests.testJoinWithDifferentGroupKeys_multiShard}.</li>
     * </ul>
     *
     * <p>Until {@code AggregateDecompositionResolver} gains engine-native merge support
     * (percentile_approx) and ORIGINAL→FINAL groupSet remapping (cross-family non-prefix),
     * the split is conservative in those shapes — distributed parallelism is traded for
     * correctness.
     *
     * <p>Public so the post-CBO {@code OpenSearchPartialAggregatePushdownRewriter}
     * shares the same correctness gates as this coord-centric split — both use an identical PARTIAL/FINAL
     * safety check, so STATE_EXPANDING / DISTINCT / cross-family-non-prefix shapes stay coordinator-centric
     * in every path.
     */
    public static boolean shouldSkipPartialFinalSplit(OpenSearchAggregate aggregate) {
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            // STATE_EXPANDING aggregates (TAKE/FIRST/LAST/LIST/VALUES/PERCENTILE_APPROX/PATTERN)
            // can't decompose into per-shard partials reduced additively. APPROXIMATE goes through
            // the structural split — its engine-native merge (sketch state, reducer == self) is
            // wired at DistributedAggregateRewriter.overrideExchangeType.
            AggregateFunction.Type type = aggregateType(aggCall.getAggregation());
            if (type == AggregateFunction.Type.STATE_EXPANDING) {
                return true;
            }
            // Residual DISTINCT (e.g. multi-arg COUNT(DISTINCT a, b) that didn't match the
            // OpenSearchDistinctCountRule single-arg rewrite) gathers to the coordinator.
            if (aggCall.isDistinct()) {
                return true;
            }
        }
        int groupCount = aggregate.getGroupSet().cardinality();
        if (aggregate.getGroupSet().equals(ImmutableBitSet.range(groupCount))) {
            return false;
        }
        // Non-prefix groupSet: a group-key at k >= groupCount lands on PARTIAL's agg-output slot.
        List<RelDataType> inputFields = aggregate.getInput().getRowType().getFieldList().stream().map(f -> f.getType()).toList();
        List<AggregateCall> aggCalls = aggregate.getAggCallList();
        for (int k : aggregate.getGroupSet().toArray()) {
            if (k < groupCount) {
                continue;
            }
            int aggIdx = k - groupCount;
            if (aggIdx >= aggCalls.size() || k >= inputFields.size()) {
                return true;
            }
            SqlTypeFamily inputFamily = inputFields.get(k).getSqlTypeName().getFamily();
            SqlTypeFamily aggFamily = aggCalls.get(aggIdx).getType().getSqlTypeName().getFamily();
            if (inputFamily != aggFamily) {
                return true;
            }
        }
        return false;
    }

    private static AggregateFunction.Type aggregateType(SqlAggFunction op) {
        try {
            return AggregateFunction.fromSqlAggFunction(op).getType();
        } catch (IllegalStateException ignored) {
            return null;
        }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        RelNode child = call.rel(1);

        RelTraitSet singletonTraits = aggregate.getTraitSet().replace(context.getDistributionTraitDef().coordSingleton());
        RelNode singletonChild = convert(child, singletonTraits);
        OpenSearchAggregate singleOnSingleton = new OpenSearchAggregate(
            aggregate.getCluster(),
            singletonTraits,
            singletonChild,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList(),
            AggregateMode.SINGLE,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations()
        );

        // Exchange placement is deterministic on partitioning, not cost: a SINGLE aggregate over
        // partitioned (RANDOM) input is incorrect — each shard would aggregate in isolation and the
        // results would never merge. So when the input is partitioned and the aggregate is
        // splittable, emit ONLY the PARTIAL/FINAL split; don't also register the gather-everything
        // singleOnSingleton alternative for Volcano to cost-compare against (that comparison is what
        // streamed the whole table to the coordinator, and biasing it back via row-count estimates
        // perturbs the global cost model for unrelated query shapes). For unpartitioned input
        // (1 shard / already gathered) or a non-splittable aggregate, the single-stage plan is the
        // correct and only choice.
        // Split into PARTIAL/FINAL only when the input is genuinely partitioned AND no operator
        // below forces a gather (a gather-forced input is already singleton — a PARTIAL over it
        // would be invalid). Otherwise emit the single coordinator aggregate.
        boolean partitioned = isPartitioned(child);
        if (!partitioned || childForcesGather(child) || shouldSkipPartialFinalSplit(aggregate) || splitSuppressedBySubToggle()) {
            call.transformTo(singleOnSingleton);
            return;
        }

        // Assembling the two phases is shared with the post-CBO pushdown rewriter; only the gather differs.
        // Here it is convert(), so Volcano owns the exchange and can dedup it against a sibling subset.
        OpenSearchDistributionTraitDef traitDef = context.getDistributionTraitDef();
        RelNode finalAlternative = AggregatePartialFinalSplit.split(
            aggregate,
            child,
            traitDef,
            partial -> convert(partial, partial.getTraitSet().replace(traitDef.coordSingleton()))
        );

        // Partitioned + splittable: the split is the only correct plan, so don't register the
        // gather-everything alternative. Volcano has nothing to cost-compare — placement is fixed.
        call.transformTo(finalAlternative);
    }

    /**
     * True when the input is partitioned across shards (distribution type RANDOM), i.e. a SINGLE
     * aggregate over it would be incorrect. Reads the input's distribution trait directly — the
     * same deterministic, shard-count-driven signal the Join/Union split rules use, no cost model
     * involved. Returns false for SINGLETON (1 shard / already gathered) or when no distribution
     * trait is present yet (Volcano still exploring — the cost gate on SINGLE is the backstop).
     */
    private static boolean isPartitioned(RelNode input) {
        for (int i = 0; i < input.getTraitSet().size(); i++) {
            RelTrait trait = input.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) {
                // RANDOM+SHARD: a multi-shard scan, the original case. HASH+WORKER: the output of a
                // distributed join or a shuffle — also partitioned, and the shape the aggregate-over-join
                // split (q5/q10) needs. Accepting only RANDOM made this rule and the post-CBO pass agree
                // on NOTHING: the pass's own isPartitioned means HASH+WORKER exclusively, so the two
                // predicates covered disjoint sets and the rule could never produce the agg-over-join split.
                return dist.getType() == RelDistribution.Type.RANDOM_DISTRIBUTED || dist.getType() == RelDistribution.Type.HASH_DISTRIBUTED;
            }
        }
        return false;
    }

    /**
     * True when a gather-forcing operator sits between this aggregate and its scan, so the
     * aggregate's input is already (or will be) gathered to one node — splitting it into
     * PARTIAL/FINAL is invalid (the PARTIAL would sit over SINGLETON input, and the shard-side
     * PARTIAL is unsatisfiable). Walks the single-input chain, descending past pass-through
     * Projects (no {@code RexOver}) and Filters; stops at the first gather-forcing op or a terminal.
     *
     * <p>Gather-forcing operators (each returns infinite cost over non-SINGLETON input in its own
     * {@code computeSelfCost}, so Volcano gathers below them):
     * <ul>
     *   <li>collated or limited {@link OpenSearchSort} (global order/limit can't run per-shard);</li>
     *   <li>a {@code RexOver}-bearing {@link OpenSearchProject} (window needs gathered input);</li>
     *   <li>{@link OpenSearchJoin} and {@link OpenSearchUnion} (coordinator-gathered today);</li>
     *   <li>a nested {@link OpenSearchAggregate} (its FINAL/SINGLE output is gathered).</li>
     * </ul>
     * Pass-through Project / Filter are walked past; scans/values and anything else end the walk
     * as not-gather-forcing (split allowed).
     *
     * <p><b>Why a trait check cannot replace this walk</b> (measured — removing the call costs 7
     * {@code CannotPlanException}s in {@code WindowPlanShapeTests}, {@code AggregateSplitCostTests},
     * {@code TopKRewriterPlanShapeTests} and {@code LateMaterializationPlanShapeTests}). The obvious
     * simplification is to trust {@link #isPartitioned} alone, since that already reads the input's
     * distribution trait. But a {@code RelSubset} can ADVERTISE a partitioned trait while every operator
     * inside it is only implementable via a gather: a collated Sort or a window Project in the partitioned
     * subset is priced at infinity there, so the subset is reachable only through its singleton sibling.
     * The trait says "partitioned"; the cost says "not like this". Since {@link #onMatch} registers exactly
     * ONE alternative, believing the trait leaves the query with no plan at all. Registering BOTH
     * alternatives instead — the principled Cascades answer — was tried and measured too: no gain, and 4
     * regressions where a 1-row estimate let SINGLE-over-gather beat a legitimate split.
     */
    private static boolean childForcesGather(RelNode node) {
        RelNode cur = unwrapForWalk(node);
        while (cur != null) {
            // Gather-forcing operators: each returns infinite cost over non-SINGLETON input in its
            // own computeSelfCost (verified: Sort-collated/limited, RexOver-Project, Join, Union,
            // nested Aggregate), so Volcano gathers below them → our input is already singleton.
            if (cur instanceof OpenSearchSort sort) {
                return !sort.getCollation().getFieldCollations().isEmpty() || sort.fetch != null || sort.offset != null;
            }
            if (cur instanceof OpenSearchJoin) {
                // A join no longer forces a gather: under top-down it is legal at WORKER+HASH, so an
                // aggregate above a DISTRIBUTED join can and should split PARTIAL/FINAL (the q5/q10
                // shape). Defer to the input's actual distribution instead of assuming coordinator —
                // isPartitioned() reads the trait, and OpenSearchAggregate's own cost gate still rejects
                // SINGLE-over-partitioned, so an unsafe placement cannot survive.
                return false;
            }
            if (cur instanceof OpenSearchUnion || cur instanceof OpenSearchAggregate) {
                return true;
            }
            if (cur instanceof OpenSearchProject project) {
                if (project.containsOver()) return true;      // window → gathered input
                cur = unwrapForWalk(cur.getInput(0));          // pass-through project → keep walking
                continue;
            }
            // Pass-through, non-gathering: Filter (q37's split must still happen) → keep walking.
            if (cur instanceof OpenSearchFilter) {
                cur = unwrapForWalk(cur.getInput(0));
                continue;
            }
            // Terminals that do not force a gather: TableScan / StageInputScan / Values, or any
            // other shape we don't explicitly treat as gather-forcing. Conservative: allow split.
            return false;
        }
        return false;
    }

    /**
     * Resolves a node to its concrete rel for the {@link #childForcesGather} walk. During
     * Volcano, {@code getInput(0)} returns a {@link RelSubset}, not a concrete rel — its
     * {@code getInputs()} is empty, which would end the walk one hop below the aggregate and miss a
     * window sitting behind an intermediate op (e.g. a {@code where} Filter). Unwrap the HEP vertex,
     * then resolve a subset to its representative rel via {@code getBestOrOriginal()}.
     */
    private static RelNode unwrapForWalk(RelNode node) {
        RelNode unwrapped = RelNodeUtils.unwrapHep(node);
        return unwrapped instanceof RelSubset subset ? subset.getBestOrOriginal() : unwrapped;
    }
}
