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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.DistributedAggregateRewriter.FinalAggCallBuilder;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchUnion;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Splits an {@link OpenSearchAggregate} into PARTIAL + FINAL when the input is partitioned. */
public class OpenSearchAggregateSplitRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchAggregateSplitRule(PlannerContext context) {
        super(operand(OpenSearchAggregate.class, operand(RelNode.class, any())), "OpenSearchAggregateSplitRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        return aggregate.getMode() == AggregateMode.SINGLE;
    }

    /** Skip the PARTIAL/FINAL split when it would emit a row type that fails Volcano's typeMatchesInferred. */
    private static boolean shouldSkipPartialFinalSplit(OpenSearchAggregate aggregate) {
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
        if (!partitioned || childForcesGather(child) || shouldSkipPartialFinalSplit(aggregate)) {
            call.transformTo(singleOnSingleton);
            return;
        }

        List<AggregateCall> partialAggCalls = repairLossyReturnTypes(aggregate.getAggCallList(), child);
        RelTraitSet partialTraits = child.getTraitSet().replace(OpenSearchConvention.INSTANCE);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            aggregate.getCluster(),
            partialTraits,
            child,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            partialAggCalls,
            AggregateMode.PARTIAL,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations()
        );
        RelTraitSet finalTraits = partial.getTraitSet().replace(context.getDistributionTraitDef().coordSingleton());
        RelNode gathered = convert(partial, finalTraits);
        Map<Integer, List<RexLiteral>> finalExtraLiterals = captureLiteralArgsForFinal(aggregate.getAggCallList(), child);

        // Classify ORIGINAL aggCalls once and stash on FINAL for post-Volcano transformers.
        List<IntermediateField> intermediateFields = FinalAggCallBuilder.classify(aggregate.getAggCallList());

        // Build FINAL's aggCalls against gathered's row type so typeMatchesInferred passes.
        List<AggregateCall> finalAggCalls = FinalAggCallBuilder.buildFinalCalls(
            aggregate.getAggCallList(),
            intermediateFields,
            aggregate.getGroupSet().cardinality(),
            gathered,
            aggregate.getGroupSet().isEmpty()
        );

        OpenSearchAggregate finalAggregate = new OpenSearchAggregate(
            aggregate.getCluster(),
            finalTraits,
            gathered,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            finalAggCalls,
            AggregateMode.FINAL,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations(),
            finalExtraLiterals,
            intermediateFields
        );

        // Empty-group nullability gap (COUNT→SUM swap): wrap FINAL so its row type matches SINGLE's.
        RelNode finalAlternative = wrapWithCastIfNeeded(finalAggregate, aggregate);

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
                return dist.getType() == RelDistribution.Type.RANDOM_DISTRIBUTED;
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
            if (cur instanceof OpenSearchJoin || cur instanceof OpenSearchUnion || cur instanceof OpenSearchAggregate) {
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

    /** Wraps FINAL in a CAST-projection when any column type drifts from {@code expected}'s row type; type-only check, name differences pass through. */
    private static RelNode wrapWithCastIfNeeded(OpenSearchAggregate finalAggregate, OpenSearchAggregate expected) {
        RelDataType actualType = finalAggregate.getRowType();
        RelDataType expectedType = expected.getRowType();
        RexBuilder rexBuilder = finalAggregate.getCluster().getRexBuilder();

        List<RexNode> projects = new ArrayList<>(actualType.getFieldCount());
        boolean anyTypeDiffers = false;
        for (int idx = 0; idx < actualType.getFieldCount(); idx++) {
            RelDataType columnType = actualType.getFieldList().get(idx).getType();
            RelDataType targetType = expectedType.getFieldList().get(idx).getType();
            RexNode ref = new RexInputRef(idx, columnType);
            if (columnType.equals(targetType)) {
                projects.add(ref);
            } else {
                projects.add(rexBuilder.makeCast(targetType, ref));
                anyTypeDiffers = true;
            }
        }
        if (!anyTypeDiffers) return finalAggregate;

        return new OpenSearchProject(
            finalAggregate.getCluster(),
            finalAggregate.getTraitSet(),
            finalAggregate,
            projects,
            expectedType,
            finalAggregate.getViableBackends()
        );
    }

    /** Re-declare LIST/VALUES return type as {@code ARRAY<arg0>} (PPL lowers it to {@code ARRAY<VARCHAR>}). */
    private static List<AggregateCall> repairLossyReturnTypes(List<AggregateCall> aggCalls, RelNode input) {
        List<AggregateCall> rebuilt = null;
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall call = aggCalls.get(i);
            String name = call.getAggregation().getName();
            if (!"LIST".equalsIgnoreCase(name) && !"VALUES".equalsIgnoreCase(name)) continue;
            if (call.getArgList().isEmpty()) continue;
            org.apache.calcite.rel.type.RelDataType arg0Type = input.getRowType().getFieldList().get(call.getArgList().get(0)).getType();
            org.apache.calcite.rel.type.RelDataType repaired = input.getCluster().getTypeFactory().createArrayType(arg0Type, -1);
            if (repaired.equals(call.getType())) continue;
            if (rebuilt == null) rebuilt = new ArrayList<>(aggCalls);
            rebuilt.set(
                i,
                AggregateCall.create(
                    call.getAggregation(),
                    call.isDistinct(),
                    call.isApproximate(),
                    call.ignoreNulls(),
                    call.rexList,
                    call.getArgList(),
                    call.filterArg,
                    call.distinctKeys,
                    call.collation,
                    repaired,
                    call.getName()
                )
            );
        }
        return rebuilt != null ? rebuilt : aggCalls;
    }

    private static Map<Integer, List<RexLiteral>> captureLiteralArgsForFinal(List<AggregateCall> aggCalls, RelNode child) {
        if (!(RelNodeUtils.unwrapHep(child) instanceof Project project)) {
            return Map.of();
        }
        List<RexNode> projects = project.getProjects();
        Map<Integer, List<RexLiteral>> captured = new LinkedHashMap<>();
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall call = aggCalls.get(i);
            AggregateFunction fn = AggregateFunction.fromSqlAggFunction(call.getAggregation());
            if (fn == null || fn.getType() != AggregateFunction.Type.STATE_EXPANDING) continue;
            List<Integer> args = call.getArgList();
            if (args.size() < 2) continue;
            List<RexLiteral> literals = new ArrayList<>(args.size() - 1);
            boolean allLiteral = true;
            for (int a = 1; a < args.size(); a++) {
                int colIdx = args.get(a);
                if (colIdx < 0 || colIdx >= projects.size() || !(projects.get(colIdx) instanceof RexLiteral lit)) {
                    allLiteral = false;
                    break;
                }
                literals.add(lit);
            }
            if (allLiteral && !literals.isEmpty()) {
                captured.put(i, List.copyOf(literals));
            }
        }
        return captured;
    }
}
