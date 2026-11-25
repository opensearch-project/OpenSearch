/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.aggregation.AggregationMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Gives group fields classic terms null semantics by inserting operators between the shared
 * scan+filter base and the {@code LogicalAggregate}. SQL GROUP BY emits a NULL group for
 * documents missing a group field; classic terms instead excludes those documents — or, when
 * the aggregation configures a {@code missing} value, counts them in that value's bucket.
 *
 * <p>Per group field:
 * <ul>
 * <li>without {@code missing} — a {@code LogicalFilter(field IS NOT NULL)} below the aggregate,
 * so a NULL group never forms. The filter must sit below the aggregate: bounded plans keep only
 * the top-K groups, and a NULL group must never occupy a top-K slot. Non-nullable fields cannot
 * produce a NULL group and are skipped.</li>
 * <li>with {@code missing} — a {@code LogicalProject} replacing the field with
 * {@code CASE WHEN field IS NOT NULL THEN field ELSE missingValue END} (not COALESCE, which the
 * engine bridge cannot translate), so those documents group under the missing value. A missing
 * value equal to a real value merges into that value's bucket, matching classic semantics.</li>
 * </ul>
 *
 * <p>The projection preserves the input schema exactly (same field names and positions), so
 * grouping indices resolved against the base row type stay valid. Metric columns pass through
 * untouched: SQL aggregate functions skip NULL inputs, matching classic metric semantics.
 */
public class PreAggregateConverter extends AbstractDslConverter {

    /** Creates a pre-aggregate converter. */
    public PreAggregateConverter() {}

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return ctx.getAggregationMetadata() != null && !ctx.getAggregationMetadata().getGroupByFieldNames().isEmpty();
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        AggregationMetadata metadata = ctx.getAggregationMetadata();
        Map<String, Object> missingValues = metadata.getMissingValues();
        RexBuilder rexBuilder = ctx.getRexBuilder();
        RelDataType rowType = input.getRowType();

        RelNode result = input;

        // IS NOT NULL for group fields without a missing value
        List<RexNode> notNullChecks = new ArrayList<>();
        for (String fieldName : metadata.getGroupByFieldNames()) {
            if (missingValues.containsKey(fieldName)) {
                continue;
            }
            RelDataTypeField field = rowType.getField(fieldName, false, false);
            if (field == null) {
                throw new ConversionException("Group-by field '" + fieldName + "' not found in schema");
            }
            if (!field.getType().isNullable()) {
                continue; // a non-nullable column cannot produce a NULL group
            }
            notNullChecks.add(
                rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, rexBuilder.makeInputRef(field.getType(), field.getIndex()))
            );
        }
        if (!notNullChecks.isEmpty()) {
            result = LogicalFilter.create(result, RexUtil.composeConjunction(rexBuilder, notNullChecks));
        }

        // missing-value substitution for group fields that configure one
        if (!missingValues.isEmpty()) {
            List<RexNode> projects = new ArrayList<>();
            List<String> fieldNames = new ArrayList<>();
            for (RelDataTypeField field : rowType.getFieldList()) {
                if (missingValues.containsKey(field.getName())) {
                    projects.add(missingSubstitution(ctx, field, missingValues.get(field.getName())));
                } else {
                    projects.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
                }
                fieldNames.add(field.getName());
            }
            result = LogicalProject.create(result, List.of(), projects, fieldNames);
        }

        return result;
    }

    /**
     * Builds {@code CASE WHEN field IS NOT NULL THEN field ELSE missingValue END}, typed
     * non-nullable: neither branch can yield NULL, and the expression becomes a group key, so
     * its type records that the key can never be null.
     */
    private static RexNode missingSubstitution(ConversionContext ctx, RelDataTypeField field, Object missingValue)
        throws ConversionException {
        RexBuilder rexBuilder = ctx.getRexBuilder();
        RexNode fieldRef = rexBuilder.makeInputRef(field.getType(), field.getIndex());
        RexNode isNotNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, fieldRef);
        RelDataType nonNullableType = ctx.getCluster().getTypeFactory().createTypeWithNullability(field.getType(), false);
        RexNode missingLiteral;
        try {
            missingLiteral = rexBuilder.makeLiteral(missingValue, nonNullableType, true);
        } catch (RuntimeException | AssertionError e) {
            throw new ConversionException(
                "[missing] value ["
                    + missingValue
                    + "] for field '"
                    + field.getName()
                    + "' is incompatible with the field type "
                    + field.getType().getSqlTypeName(),
                e instanceof Exception ex ? ex : null
            );
        }
        return rexBuilder.makeCall(nonNullableType, SqlStdOperatorTable.CASE, List.of(isNotNull, fieldRef, missingLiteral));
    }
}
