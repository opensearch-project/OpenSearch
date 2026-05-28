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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.opensearch.analytics.spi.OpenSearchAggregateOperators;

import java.util.ArrayList;
import java.util.List;

/**
 * HEP rule that decomposes the {@link OpenSearchAggregateOperators#STATS STATS} bundle aggregate
 * into the four primitive aggregates ({@code COUNT}, {@code MIN}, {@code MAX}, {@code SUM})
 * plus a {@link LogicalProject} that re-assembles them — together with a derived {@code AVG}
 * — into the struct shape the original {@code STATS} call declared.
 *
 * <p><b>Why decompose at HEP time?</b> {@code STATS} has no executor implementation in this
 * codebase and isn't a Calcite standard {@link org.apache.calcite.sql.SqlKind} that
 * Calcite's stock {@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule} knows
 * how to reduce. Doing the rewrite during HEP keeps the rest of the planner — marking,
 * Volcano split, partial/final resolver, fragment conversion, DataFusion execution —
 * unaware of {@code STATS}; everything downstream sees only primitives that already have
 * full support.
 *
 * <p><b>Output shape preserved.</b> The original {@code STATS(x)} column has type
 * {@code STRUCT(count, min, max, avg, sum)}. The {@link LogicalProject} on top of the
 * rebuilt {@link LogicalAggregate} reassembles the struct so the row-type contract holds.
 *
 * @opensearch.internal
 */
public final class OpenSearchStatsReduceRule extends RelOptRule {

    public static final OpenSearchStatsReduceRule INSTANCE = new OpenSearchStatsReduceRule();

    private OpenSearchStatsReduceRule() {
        super(operand(LogicalAggregate.class, any()), "OpenSearchStatsReduceRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalAggregate aggregate = call.rel(0);
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            if (isStats(aggCall.getAggregation())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate aggregate = call.rel(0);
        RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();

        int groupCount = aggregate.getGroupCount();
        boolean hasEmptyGroup = aggregate.getGroupSet().isEmpty();

        Expansion expansion = expandAggCalls(aggregate, hasEmptyGroup, groupCount);

        LogicalAggregate newAgg = LogicalAggregate.create(
            aggregate.getInput(),
            aggregate.getHints(),
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            expansion.newCalls
        );

        RelNode project = buildAssemblyProject(aggregate, newAgg, expansion.originalToNew, groupCount, rexBuilder, typeFactory);
        call.transformTo(project);
    }

    /**
     * Expands each STATS aggregate call into four primitives ({@code SUM}, {@code MIN},
     * {@code MAX}, {@code COUNT}, in that emission order); leaves non-STATS calls untouched.
     *
     * <p><b>Emission order matters for distributed correctness.</b> Calcite's
     * {@code Aggregate.typeMatchesInferred} assertion fires during the split rule's FINAL
     * aggregate construction. The FINAL keeps the original aggCall list with original
     * {@code argList} (e.g. {@code [1]}) but its input is the PARTIAL's output, where
     * column 1 is the first aggregate's output column. Putting {@code SUM} first means
     * column 1 of PARTIAL is {@code sum_p} — same Calcite type as the source field — so
     * when FINAL re-infers MIN/MAX/SUM/COUNT against it, the inferred types match the
     * stored types from when this rule created the calls. {@code DistributedAggregateRewriter}
     * corrects the semantically-incorrect argList post-Volcano. Putting {@code COUNT}
     * first would force column 1 of PARTIAL to be {@code BIGINT}, breaking FINAL's
     * MIN/MAX/SUM type assertions.
     *
     * <p>The returned mapping per original call is {@code [countIdx, minIdx, maxIdx, sumIdx]}
     * regardless of emission order — assembly looks up by role, not position.
     */
    private static Expansion expandAggCalls(LogicalAggregate aggregate, boolean hasEmptyGroup, int groupCount) {
        RelNode input = aggregate.getInput();
        List<AggregateCall> newCalls = new ArrayList<>();
        List<int[]> originalToNew = new ArrayList<>(aggregate.getAggCallList().size());
        for (AggregateCall original : aggregate.getAggCallList()) {
            if (isStats(original.getAggregation())) {
                int sumIdx = newCalls.size();
                newCalls.add(makePrimitive(SqlStdOperatorTable.SUM, original, input, hasEmptyGroup, groupCount));
                int minIdx = newCalls.size();
                newCalls.add(makePrimitive(SqlStdOperatorTable.MIN, original, input, hasEmptyGroup, groupCount));
                int maxIdx = newCalls.size();
                newCalls.add(makePrimitive(SqlStdOperatorTable.MAX, original, input, hasEmptyGroup, groupCount));
                int countIdx = newCalls.size();
                newCalls.add(makePrimitive(SqlStdOperatorTable.COUNT, original, input, hasEmptyGroup, groupCount));
                originalToNew.add(new int[] { countIdx, minIdx, maxIdx, sumIdx });
            } else {
                originalToNew.add(new int[] { newCalls.size() });
                newCalls.add(original);
            }
        }
        return new Expansion(newCalls, originalToNew);
    }

    /**
     * Result of {@link #expandAggCalls}: the new aggCall list and a per-original-call mapping
     * to indices in the new list ([countIdx, minIdx, maxIdx, sumIdx] for STATS, single-element
     * for everything else).
     */
    private record Expansion(List<AggregateCall> newCalls, List<int[]> originalToNew) {
    }

    /**
     * Builds the Project on top of {@code newAgg} that produces the original aggregate's row
     * type: group-by columns pass through unchanged; non-STATS aggregate columns are identity
     * refs into {@code newAgg}'s output; STATS aggregate columns are reassembled struct
     * expressions via {@link #buildStatsStruct}.
     */
    private static RelNode buildAssemblyProject(
        LogicalAggregate originalAgg,
        LogicalAggregate newAgg,
        List<int[]> originalToNew,
        int groupCount,
        RexBuilder rexBuilder,
        RelDataTypeFactory typeFactory
    ) {
        List<RelDataTypeField> originalFields = originalAgg.getRowType().getFieldList();
        List<RelDataTypeField> newAggFields = newAgg.getRowType().getFieldList();
        List<RexNode> projects = new ArrayList<>(originalFields.size());
        List<String> fieldNames = new ArrayList<>(originalFields.size());

        for (int i = 0; i < groupCount; i++) {
            projects.add(rexBuilder.makeInputRef(newAggFields.get(i).getType(), i));
            fieldNames.add(originalFields.get(i).getName());
        }

        for (int origIdx = 0; origIdx < originalAgg.getAggCallList().size(); origIdx++) {
            AggregateCall original = originalAgg.getAggCallList().get(origIdx);
            int[] mapping = originalToNew.get(origIdx);
            fieldNames.add(originalFields.get(groupCount + origIdx).getName());

            if (isStats(original.getAggregation())) {
                projects.add(buildStatsStruct(original, mapping, newAggFields, groupCount, rexBuilder, typeFactory));
            } else {
                int newColPos = groupCount + mapping[0];
                projects.add(rexBuilder.makeInputRef(newAggFields.get(newColPos).getType(), newColPos));
            }
        }

        return LogicalProject.create(newAgg, originalAgg.getHints(), projects, fieldNames);
    }

    /**
     * Builds a {@code ROW(count, min, max, avg, sum)} expression matching the original
     * STATS call's declared struct type. Field order matches legacy OpenSearch
     * {@code InternalStats}: count, min, max, avg, sum — so {@code statsFields.get(3)} is
     * the avg field type and {@code statsFields.get(4)} is the sum field type.
     */
    private static RexNode buildStatsStruct(
        AggregateCall originalStats,
        int[] mapping,
        List<RelDataTypeField> newAggFields,
        int groupCount,
        RexBuilder rexBuilder,
        RelDataTypeFactory typeFactory
    ) {
        RelDataType statsType = originalStats.getType();
        List<RelDataTypeField> statsFields = statsType.getFieldList();
        if (statsFields.size() != 5) {
            throw new IllegalStateException(
                "STATS return type must declare 5 fields (count, min, max, avg, sum), got: " + statsType
            );
        }

        int newCountPos = groupCount + mapping[0];
        int newMinPos = groupCount + mapping[1];
        int newMaxPos = groupCount + mapping[2];
        int newSumPos = groupCount + mapping[3];

        RexNode countRef = ensureType(rexBuilder, newAggFields.get(newCountPos).getType(), newCountPos, statsFields.get(0).getType());
        RexNode minRef = ensureType(rexBuilder, newAggFields.get(newMinPos).getType(), newMinPos, statsFields.get(1).getType());
        RexNode maxRef = ensureType(rexBuilder, newAggFields.get(newMaxPos).getType(), newMaxPos, statsFields.get(2).getType());
        RexNode sumRef = ensureType(rexBuilder, newAggFields.get(newSumPos).getType(), newSumPos, statsFields.get(4).getType());
        RexNode avgRef = buildAvgExpression(newSumPos, newCountPos, statsFields.get(3).getType(), newAggFields, rexBuilder, typeFactory);

        return rexBuilder.makeCall(statsType, SqlStdOperatorTable.ROW, List.of(countRef, minRef, maxRef, avgRef, sumRef));
    }

    /**
     * Computes {@code CAST(sum AS DOUBLE) / CAST(count AS DOUBLE)} cast back to
     * {@code targetAvgType}. Computing through DOUBLE casts gives a stable DOUBLE result
     * regardless of the input numeric type. The final cast is defensive — guards against
     * future drift between the divide result type and the AVG field type declared by STATS.
     */
    private static RexNode buildAvgExpression(
        int sumPos,
        int countPos,
        RelDataType targetAvgType,
        List<RelDataTypeField> newAggFields,
        RexBuilder rexBuilder,
        RelDataTypeFactory typeFactory
    ) {
        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RexNode rawSum = rexBuilder.makeInputRef(newAggFields.get(sumPos).getType(), sumPos);
        RexNode rawCount = rexBuilder.makeInputRef(newAggFields.get(countPos).getType(), countPos);
        RexNode sumAsDouble = rexBuilder.makeCast(doubleNullable, rawSum);
        RexNode countAsDouble = rexBuilder.makeCast(doubleNullable, rawCount);
        RexNode avgExpr = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumAsDouble, countAsDouble);
        return rexBuilder.makeCast(targetAvgType, avgExpr);
    }

    /**
     * Builds a primitive aggregate call (COUNT/MIN/MAX/SUM) over the same operand and filter
     * as the original {@code STATS} call. Empty {@code rexList} because primitive aggregates
     * have no per-call expressions; {@code null} for type and name lets Calcite infer them
     * from the operator and input.
     */
    private static AggregateCall makePrimitive(
        SqlAggFunction op,
        AggregateCall original,
        RelNode input,
        boolean hasEmptyGroup,
        int groupCount
    ) {
        return AggregateCall.create(
            op,
            original.isDistinct(),
            original.isApproximate(),
            original.ignoreNulls(),
            List.of(),
            original.getArgList(),
            original.filterArg,
            null,
            RelCollations.EMPTY,
            groupCount,
            input,
            null,
            null
        );
    }

    /**
     * Returns a {@link RexNode} of {@code targetType}: an input ref at {@code pos}, optionally
     * wrapped in a CAST when source and target differ modulo nullability. Used to coerce
     * primitive aggregate outputs into the field types declared by the STATS struct.
     */
    private static RexNode ensureType(RexBuilder rexBuilder, RelDataType sourceType, int pos, RelDataType targetType) {
        RexNode ref = rexBuilder.makeInputRef(sourceType, pos);
        if (SqlTypeUtil.equalSansNullability(rexBuilder.getTypeFactory(), sourceType, targetType)) {
            return ref;
        }
        return rexBuilder.makeCast(targetType, ref);
    }

    private static boolean isStats(SqlAggFunction op) {
        return op == OpenSearchAggregateOperators.STATS;
    }
}
