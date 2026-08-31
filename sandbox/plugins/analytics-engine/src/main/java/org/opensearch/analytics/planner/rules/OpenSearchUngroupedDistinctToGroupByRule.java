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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Rewrites a lone ungrouped single-arg exact {@code COUNT(DISTINCT x)} to {@code COUNT(*)} over an inner
 * {@code GROUP BY x}: the grouped inner dedups in parallel (hash-partitioned on x) and the outer count is a
 * plain partial/final split, keeping the query multi-core instead of collapsing the distinct merge onto one
 * partition. (What DataFusion's {@code SingleDistinctToGroupBy} would do, but the substrait path skips the
 * logical optimizer.) An {@code x IS NOT NULL} filter keeps {@code COUNT(*)} matching {@code COUNT(DISTINCT)}
 * NULL semantics. Narrow: grouped, approx, and multi-arg distinct are untouched.
 *
 * @opensearch.internal
 */
public class OpenSearchUngroupedDistinctToGroupByRule extends RelOptRule {

    public OpenSearchUngroupedDistinctToGroupByRule() {
        super(operand(LogicalAggregate.class, any()), "OpenSearchUngroupedDistinctToGroupByRule");
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        LogicalAggregate agg = ruleCall.rel(0);
        if (agg.getGroupCount() != 0 || agg.getAggCallList().size() != 1) return false;
        AggregateCall c = agg.getAggCallList().get(0);
        return c.getAggregation().getKind() == SqlKind.COUNT && c.isDistinct() && c.getArgList().size() == 1 && c.filterArg < 0;
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        LogicalAggregate agg = ruleCall.rel(0);
        int argIdx = agg.getAggCallList().get(0).getArgList().get(0);
        String outName = agg.getRowType().getFieldList().get(0).getName();

        RelBuilder relBuilder = ruleCall.builder();
        relBuilder.push(agg.getInput());
        relBuilder.filter(relBuilder.isNotNull(relBuilder.field(argIdx))); // COUNT(DISTINCT x) ignores NULLs
        relBuilder.aggregate(relBuilder.groupKey(ImmutableBitSet.of(argIdx))); // inner: distinct non-null x
        relBuilder.aggregate(relBuilder.groupKey(), relBuilder.count(false, outName)); // outer: COUNT(*) over distinct rows
        RelNode result = relBuilder.build();

        // COUNT(*) is BIGINT NOT NULL with the original field name, so the row type matches; guard rather
        // than risk HepPlanner's replacement-must-equal-original-row-type assertion.
        if (result.getRowType().equals(agg.getRowType())) {
            ruleCall.transformTo(result);
        }
    }
}
