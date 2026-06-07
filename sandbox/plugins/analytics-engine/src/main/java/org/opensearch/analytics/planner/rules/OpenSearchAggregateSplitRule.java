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
import org.apache.calcite.plan.RelTraitSet;
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
import org.opensearch.analytics.planner.rel.OpenSearchProject;
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

        if (shouldSkipPartialFinalSplit(aggregate)) {
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

        call.getPlanner().ensureRegistered(singleOnSingleton, aggregate);
        call.transformTo(finalAlternative);
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
