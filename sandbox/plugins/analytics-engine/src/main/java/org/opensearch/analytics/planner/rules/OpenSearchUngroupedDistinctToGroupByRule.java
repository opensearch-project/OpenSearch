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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Rewrites a lone ungrouped single-arg exact {@code COUNT(DISTINCT x)} to {@code COUNT(*)} over an inner
 * {@code GROUP BY x}: the grouped inner dedups in parallel (hash-partitioned on x) and the outer count is a
 * plain partial/final split, keeping the query multi-core instead of collapsing the distinct merge onto one
 * partition. (What DataFusion's {@code SingleDistinctToGroupBy} would do, but the substrait path skips the
 * logical optimizer.) Narrow: grouped, approx, and multi-arg distinct are untouched.
 *
 * <p><b>Null handling — the load-bearing part for correctness.</b> {@code GROUP BY x} yields a single NULL
 * group for null {@code x}; {@code COUNT(DISTINCT x)} must exclude it. Rather than filtering {@code x IS NOT
 * NULL} directly on the group key, this rule projects a <em>derived</em> boolean indicator
 * {@code x IS NOT NULL} above the dedup and filters on THAT. The distinction is not cosmetic:
 * <ul>
 *   <li>A filter on the group key (or on the scan column below the dedup) inherits {@code x}'s physical
 *       storage, so on a composite (parquet + lucene) index the injected {@code IS NOT NULL} predicate is
 *       delegated to Lucene. On the empty-blocklist analytics ITs that delegation mis-binds on the
 *       decomposed shape and silently passes NULLs through — over-counting (numeric) or dropping the
 *       user's WHERE via stacked-filter mis-derivation (keyword / multi-index). This rule runs after
 *       {@code CoreRules.FILTER_MERGE}, so such an injected filter would also never merge with the query's
 *       existing WHERE / frontend-implicit {@code IS NOT NULL}.</li>
 *   <li>A projected {@code IS NOT NULL} is a computed column with no physical storage, so
 *       {@code OpenSearchFilterRule} keeps the filter on it native (never delegated). It deterministically
 *       drops the NULL group on every backend — even compensating for a broken upstream delegated NULL
 *       filter — while the user's WHERE stays a single (mergeable) filter below the dedup and delegates as
 *       usual. Correct on the plain-parquet, composite, delegation and multi-index paths alike, with the
 *       dedup parallelization intact.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class OpenSearchUngroupedDistinctToGroupByRule extends RelOptRule {

    /** Intermediate name for the derived {@code x IS NOT NULL} indicator; never surfaces in the output row type. */
    private static final String NOT_NULL_INDICATOR = "$dc_arg_not_null";

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
        // Inner: GROUP BY x → one row per distinct x (null x collapses to a single NULL group). Field 0 = x.
        relBuilder.aggregate(relBuilder.groupKey(ImmutableBitSet.of(argIdx)));
        // Project a DERIVED not-null indicator above the dedup, then filter on it. Because the indicator is a
        // computed column (not the group key / a scan column), the filter stays native and never delegates —
        // so it reliably drops the NULL group to match COUNT(DISTINCT) semantics on every backend/path.
        RexNode groupKey = relBuilder.field(0);
        relBuilder.project(List.of(groupKey, relBuilder.isNotNull(groupKey)), List.of(outName, NOT_NULL_INDICATOR), true);
        relBuilder.filter(relBuilder.field(1));
        // Outer: COUNT(*) over the surviving (non-null) distinct rows.
        relBuilder.aggregate(relBuilder.groupKey(), relBuilder.count(false, outName));
        RelNode result = relBuilder.build();

        // COUNT(*) is BIGINT NOT NULL with the original field name, so the row type matches; guard rather
        // than risk HepPlanner's replacement-must-equal-original-row-type assertion.
        if (result.getRowType().equals(agg.getRowType())) {
            ruleCall.transformTo(result);
        }
    }
}
