/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.immutables.value.Value;
import org.opensearch.analytics.plan.QueryPlanningException;
import org.opensearch.analytics.plan.operators.AggMode;
import org.opensearch.analytics.plan.operators.OpenSearchAggregate;

import java.util.ArrayList;
import java.util.List;

/**
 * Phase 4 of the query planning pipeline: splits an {@link OpenSearchAggregate} with
 * {@code mode == UNRESOLVED} into a {@code PARTIAL} aggregate followed by a {@code FINAL}
 * aggregate, enabling partial aggregation close to the data and final merging locally.
 *
 * <p>The {@code mode == UNRESOLVED} guard is placed in the operand predicate so Calcite
 * never selects this rule for {@code PARTIAL} or {@code FINAL} nodes, satisfying
 * Requirement 4.5 without any check in {@code onMatch()}.
 *
 * <p>In Calcite 1.41+, aggregate functions expose their splitter via
 * {@code fn.unwrap(SqlSplittableAggFunction.class)} rather than implementing the interface
 * directly. The merge function for each partial/final pair is derived directly:
 * COUNT → SUM0 (self-splittable functions like MIN/MAX/SUM keep the same function).
 *
 * <p>Requirements: 4.1, 4.2, 4.3, 4.4, 4.5
 */
@Value.Enclosing
public final class AggSplitRule extends RelRule<AggSplitRule.Config> {

    /** Singleton instance — use this in HepPlanner programs. */
    public static final AggSplitRule INSTANCE = AggSplitRule.Config.DEFAULT.toRule();

    private AggSplitRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchAggregate agg = call.rel(0);
        // Note: mode == UNRESOLVED guard is in the operand predicate, not here.
        // onMatch() is only called when the predicate passes.

        List<AggregateCall> partialAggCalls = new ArrayList<>();
        List<AggregateCall> mergeAggCalls = new ArrayList<>();
        int groupCount = agg.getGroupCount();

        // Track partial output field index — starts after group keys.
        int partialFieldIndex = groupCount;

        for (AggregateCall aggCall : agg.getAggCallList()) {
            SqlAggFunction fn = aggCall.getAggregation();

            // In Calcite 1.41+, some functions (e.g. COUNT) expose their splitter via
            // unwrap() rather than implementing SqlSplittableAggFunction directly.
            SqlSplittableAggFunction splittable = fn.unwrap(SqlSplittableAggFunction.class);
            if (splittable == null) {
                throw new QueryPlanningException(List.of(
                    "Aggregate function does not support partial/final split: "
                    + fn.getName()
                    + ". Supported: COUNT, SUM, SUM0, MIN, MAX."));
            }

            // PARTIAL aggCall: same function, same arguments, same type as original
            partialAggCalls.add(aggCall);

            // FINAL (merge) aggCall:
            // - COUNT(*) partial → SUM0(partial_count_col) final (BIGINT NOT NULL)
            //   SUM0 returns 0 (not null) when there are 0 rows.
            // - SUM/SUM0/MIN/MAX: self-splittable, merge function = partial function,
            //   same output type.
            //
            // We use the explicit-type AggregateCall.create overload to avoid
            // triggering recursive type computation via a RelNode input parameter.
            SqlAggFunction mergeFn = getMergeFn(fn);
            RelDataType mergeType = deriveMergeType(agg, aggCall, mergeFn);

            AggregateCall mergeCall = AggregateCall.create(
                mergeFn,
                false,                        // distinct
                List.of(partialFieldIndex),   // arg: the partial output column
                -1,                           // filterArg
                mergeType,
                aggCall.getName()
            );
            mergeAggCalls.add(mergeCall);
            partialFieldIndex++;
        }

        OpenSearchAggregate partial = new OpenSearchAggregate(
            agg.getCluster(), agg.getTraitSet(), agg.getInput(),
            agg.getGroupSet(), agg.getGroupSets(), partialAggCalls,
            "unresolved", AggMode.PARTIAL);

        OpenSearchAggregate finalAgg = new OpenSearchAggregate(
            agg.getCluster(), agg.getTraitSet(), partial,
            agg.getGroupSet(), agg.getGroupSets(), mergeAggCalls,
            "unresolved", AggMode.FINAL);

        call.transformTo(finalAgg);
    }

    /**
     * Returns the merge (final-stage) aggregate function for the given partial function.
     * COUNT partial → SUM0 final (sum the partial counts; SUM0 returns 0 not null for empty).
     * All others are self-splittable: the merge function is the same as the partial function.
     */
    private static SqlAggFunction getMergeFn(SqlAggFunction fn) {
        if (fn.getKind() == SqlKind.COUNT) {
            return SqlStdOperatorTable.SUM0;
        }
        return fn;
    }

    /**
     * Derives the output type for the merge (final-stage) aggregate call.
     * For COUNT→SUM0: BIGINT NOT NULL (SUM0 over a BIGINT partial COUNT column).
     * For self-splittable functions: same type as the original aggCall.
     */
    private static RelDataType deriveMergeType(OpenSearchAggregate agg,
                                                AggregateCall aggCall,
                                                SqlAggFunction mergeFn) {
        if (mergeFn.getKind() == SqlKind.SUM0) {
            return agg.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        }
        return aggCall.getType();
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableAggSplitRule.Config.builder()
            .build()
            .withOperandSupplier(b -> b
                .operand(OpenSearchAggregate.class)
                .predicate(agg -> agg.getMode() == AggMode.UNRESOLVED)
                .anyInputs())
            .as(Config.class);

        @Override
        default AggSplitRule toRule() {
            return new AggSplitRule(this);
        }
    }
}
