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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites single-arg {@code COUNT(DISTINCT x)} and PPL's {@code distinct_count_approx(x)} UDAF
 * marker to {@link SqlStdOperatorTable#APPROX_COUNT_DISTINCT} before the aggregate is marked by
 * {@link OpenSearchAggregateRule}, so substrait dispatch resolves by operator identity. Multi-arg
 * distinct falls through to coordinator-gather in {@link OpenSearchAggregateSplitRule}.
 *
 * @opensearch.internal
 */
public class OpenSearchDistinctCountRule extends RelOptRule {

    public OpenSearchDistinctCountRule() {
        super(operand(LogicalAggregate.class, any()), "OpenSearchDistinctCountRule");
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        LogicalAggregate agg = ruleCall.rel(0);
        return agg.getAggCallList().stream().anyMatch(OpenSearchDistinctCountRule::needsRewriteToApprox);
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        LogicalAggregate agg = ruleCall.rel(0);
        List<AggregateCall> rewritten = new ArrayList<>(agg.getAggCallList().size());
        boolean changed = false;
        for (AggregateCall call : agg.getAggCallList()) {
            if (needsRewriteToApprox(call)) {
                rewritten.add(rewriteToApprox(call, agg));
                changed = true;
            } else {
                rewritten.add(call);
            }
        }
        if (!changed) return;
        LogicalAggregate replacement = (LogicalAggregate) agg.copy(
            agg.getTraitSet(),
            agg.getInput(),
            agg.getGroupSet(),
            agg.getGroupSets(),
            rewritten
        );
        // Aggregate.typeMatchesInferred forces the new aggCall to BIGINT NOT NULL while HepPlanner
        // requires the replacement's row type to equal the original's; bridge with a casting Project.
        RelNode rewrittenNode = projectToOriginalRowType(ruleCall, agg, replacement);
        ruleCall.transformTo(rewrittenNode);
    }

    private static RelNode projectToOriginalRowType(RelOptRuleCall ruleCall, LogicalAggregate original, LogicalAggregate replacement) {
        if (replacement.getRowType().equals(original.getRowType())) {
            return replacement;
        }
        RelBuilder relBuilder = ruleCall.builder();
        relBuilder.push(replacement);
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        List<RelDataTypeField> origFields = original.getRowType().getFieldList();
        List<RelDataTypeField> newFields = replacement.getRowType().getFieldList();
        List<RexNode> projects = new ArrayList<>(origFields.size());
        List<String> names = new ArrayList<>(origFields.size());
        for (int i = 0; i < origFields.size(); i++) {
            RexNode ref = rexBuilder.makeInputRef(replacement, i);
            RelDataType targetType = origFields.get(i).getType();
            if (!newFields.get(i).getType().equals(targetType)) {
                ref = rexBuilder.makeCast(targetType, ref);
            }
            projects.add(ref);
            names.add(origFields.get(i).getName());
        }
        relBuilder.project(projects, names, /* forceProject */ true);
        return relBuilder.build();
    }

    /** True when the call is a single-arg COUNT(DISTINCT) or PPL's distinct_count_approx UDAF. */
    private static boolean needsRewriteToApprox(AggregateCall call) {
        return isSingleArgCountDistinct(call) || isPplDistinctCountApproxUdf(call);
    }

    private static boolean isSingleArgCountDistinct(AggregateCall call) {
        return call.getAggregation().getKind() == SqlKind.COUNT && call.isDistinct() && call.getArgList().size() == 1;
    }

    /** PPL's distinct_count_approx is a UDF named "APPROX_COUNT_DISTINCT" that is not the stdop. */
    private static boolean isPplDistinctCountApproxUdf(AggregateCall call) {
        return call.getAggregation() != SqlStdOperatorTable.APPROX_COUNT_DISTINCT
            && "APPROX_COUNT_DISTINCT".equals(call.getAggregation().getName())
            && call.getArgList().size() == 1;
    }

    private static AggregateCall rewriteToApprox(AggregateCall call, LogicalAggregate agg) {
        return AggregateCall.create(
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
            /* distinct */ false,
            /* approximate */ false,
            call.ignoreNulls(),
            call.rexList,
            call.getArgList(),
            call.filterArg,
            call.distinctKeys,
            call.collation,
            agg.getGroupSet().cardinality(),
            agg.getInput(),
            /* type */ null,
            call.getName()
        );
    }
}
