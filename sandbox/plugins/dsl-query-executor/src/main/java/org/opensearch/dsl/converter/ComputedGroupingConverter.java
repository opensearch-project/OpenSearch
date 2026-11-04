/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.ExpressionGrouping;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Materializes the computed group-key columns for expression-based bucket aggregations (today,
 * {@code range}) by appending them to the shared scan+filter base, so the {@code LogicalAggregate}
 * can GROUP BY a synthetic ordinal column that does not exist in the index mapping.
 *
 * <p>Runs <b>before</b> {@code PreAggregateConverter}: the metadata's group-by field names include
 * the synthetic column, which pre-aggregate null handling then resolves against this converter's
 * output. Columns are <b>appended</b> (never reordered), so every base field keeps its index and
 * the metric {@code AggregateCall}s resolved against the scan schema stay valid; the synthetic
 * column lands at {@code baseFieldCount + offset}, matching the index
 * {@link AggregationMetadata#getGroupByBitSet()} was assigned in {@code AggregationMetadataBuilder}.
 *
 * <p>The projected key is a range-ordinal CASE over {@code CAST(field AS DOUBLE)}; see
 * {@link #ordinalCase} and {@link #membershipCondition} for its exact shape.
 */
public class ComputedGroupingConverter extends AbstractDslConverter {

    /** Creates a computed-grouping converter. */
    public ComputedGroupingConverter() {}

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return ctx.getAggregationMetadata() != null && !ctx.getAggregationMetadata().getExpressionGroupings().isEmpty();
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        AggregationMetadata metadata = ctx.getAggregationMetadata();
        RexBuilder rexBuilder = ctx.getRexBuilder();
        RelDataTypeFactory typeFactory = ctx.getCluster().getTypeFactory();
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType rowType = input.getRowType();

        List<RexNode> projects = new ArrayList<>();
        List<String> names = new ArrayList<>();
        // Pass every base column through unchanged (same names, same positions).
        for (RelDataTypeField field : rowType.getFieldList()) {
            projects.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
            names.add(field.getName());
        }
        // Append one ordinal column per expression grouping, in the same order the metadata assigned indices.
        for (ExpressionGrouping grouping : metadata.getExpressionGroupings()) {
            RelDataTypeField source = rowType.getField(grouping.getSourceField(), false, false);
            if (source == null) {
                throw new ConversionException("Range field '" + grouping.getSourceField() + "' not found in schema");
            }
            RexNode fieldAsDouble = rexBuilder.makeCast(doubleType, rexBuilder.makeInputRef(source.getType(), source.getIndex()));
            projects.add(ordinalCase(rexBuilder, doubleType, intType, fieldAsDouble, grouping.getBounds()));
            names.add(grouping.getSyntheticColumn());
        }
        return LogicalProject.create(input, List.of(), projects, names);
    }

    /** Builds {@code CASE WHEN <in range i> THEN i ... ELSE NULL END}, ordinal i = declaration index. */
    private static RexNode ordinalCase(
        RexBuilder rexBuilder,
        RelDataType doubleType,
        RelDataType intType,
        RexNode fieldAsDouble,
        List<ExpressionGrouping.Bound> bounds
    ) {
        List<RexNode> operands = new ArrayList<>(bounds.size() * 2 + 1);
        for (int i = 0; i < bounds.size(); i++) {
            operands.add(membershipCondition(rexBuilder, doubleType, fieldAsDouble, bounds.get(i)));
            operands.add(rexBuilder.makeExactLiteral(BigDecimal.valueOf(i), intType));
        }
        operands.add(rexBuilder.makeNullLiteral(intType)); // ELSE: matched no range
        return rexBuilder.makeCall(SqlStdOperatorTable.CASE, operands);
    }

    /** {@code v >= from AND v < to}, dropping the comparison on any ±∞ side; {@code [-∞,+∞)} → TRUE. */
    private static RexNode membershipCondition(
        RexBuilder rexBuilder,
        RelDataType doubleType,
        RexNode fieldAsDouble,
        ExpressionGrouping.Bound bound
    ) {
        List<RexNode> conjuncts = new ArrayList<>(2);
        if (bound.from() != Double.NEGATIVE_INFINITY) {
            conjuncts.add(
                rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    fieldAsDouble,
                    rexBuilder.makeApproxLiteral(BigDecimal.valueOf(bound.from()), doubleType)
                )
            );
        }
        if (bound.to() != Double.POSITIVE_INFINITY) {
            conjuncts.add(
                rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN,
                    fieldAsDouble,
                    rexBuilder.makeApproxLiteral(BigDecimal.valueOf(bound.to()), doubleType)
                )
            );
        }
        if (conjuncts.isEmpty()) {
            return rexBuilder.makeLiteral(true);
        }
        return RexUtil.composeConjunction(rexBuilder, conjuncts);
    }
}
