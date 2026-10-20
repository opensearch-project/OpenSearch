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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Rebinds PPL's reflective {@code CHECKED_LONG_SUM} marker to Calcite's canonical
 * {@link SqlStdOperatorTable#SUM} before aggregate marking and backend dispatch.
 *
 * @opensearch.internal
 */
public class OpenSearchCheckedLongSumRule extends RelOptRule {

    public OpenSearchCheckedLongSumRule() {
        super(operand(LogicalAggregate.class, any()), "OpenSearchCheckedLongSumRule");
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        LogicalAggregate aggregate = ruleCall.rel(0);
        return aggregate.getAggCallList().stream().anyMatch(call -> isCheckedLongSum(call.getAggregation()));
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        LogicalAggregate aggregate = ruleCall.rel(0);
        List<AggregateCall> rewritten = new ArrayList<>(aggregate.getAggCallList().size());
        for (AggregateCall call : aggregate.getAggCallList()) {
            rewritten.add(isCheckedLongSum(call.getAggregation()) ? rewrite(call, aggregate) : call);
        }
        LogicalAggregate replacement = aggregate.copy(
            aggregate.getTraitSet(),
            aggregate.getInput(),
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            rewritten
        );
        ruleCall.transformTo(projectToOriginalRowType(ruleCall, aggregate, replacement));
    }

    static boolean isCheckedLongSum(SqlAggFunction operator) {
        return operator != SqlStdOperatorTable.SUM
            && operator.getKind() == SqlKind.SUM
            && "CHECKED_LONG_SUM".equalsIgnoreCase(operator.getName());
    }

    private static AggregateCall rewrite(AggregateCall call, LogicalAggregate aggregate) {
        return AggregateCall.create(
            SqlStdOperatorTable.SUM,
            call.isDistinct(),
            call.isApproximate(),
            call.ignoreNulls(),
            call.rexList,
            call.getArgList(),
            call.filterArg,
            call.distinctKeys,
            call.collation,
            aggregate.getGroupSet().cardinality(),
            aggregate.getInput(),
            null,
            call.getName()
        );
    }

    private static RelNode projectToOriginalRowType(RelOptRuleCall ruleCall, LogicalAggregate original, LogicalAggregate replacement) {
        if (replacement.getRowType().equals(original.getRowType())) {
            return replacement;
        }
        RelBuilder relBuilder = ruleCall.builder();
        relBuilder.push(replacement);
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        List<RexNode> projects = new ArrayList<>(original.getRowType().getFieldCount());
        List<String> names = new ArrayList<>(original.getRowType().getFieldCount());
        for (RelDataTypeField field : original.getRowType().getFieldList()) {
            RexNode ref = rexBuilder.makeInputRef(replacement, field.getIndex());
            RelDataType replacementType = replacement.getRowType().getFieldList().get(field.getIndex()).getType();
            if (!replacementType.equals(field.getType())) {
                ref = rexBuilder.makeCast(field.getType(), ref);
            }
            projects.add(ref);
            names.add(field.getName());
        }
        relBuilder.project(projects, names, true);
        return relBuilder.build();
    }
}
