/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.search.aggregations.pipeline.BucketHelpers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared utility that wraps metric column references with gap policy handling.
 *
 * <p>For {@code INSERT_ZEROS}: produces a {@link LogicalProject} that replaces
 * null values with 0 using {@code COALESCE(column, 0)}.
 *
 * <p>For {@code SKIP}: produces a {@link LogicalFilter} that excludes rows
 * where the metric column {@code IS NULL}.
 */
public class GapPolicyHandler {

    private GapPolicyHandler() {}

    /**
     * Applies gap policy handling to the input RelNode for the given column.
     *
     * @param policy   the gap policy
     * @param input    the input RelNode
     * @param colIndex the column index to apply the policy to
     * @param ctx      the conversion context
     * @return a new RelNode with gap policy applied
     */
    public static RelNode apply(BucketHelpers.GapPolicy policy, RelNode input, int colIndex,
                                ConversionContext ctx) {
        if (policy == BucketHelpers.GapPolicy.INSERT_ZEROS) {
            return projectWithCoalesce(input, colIndex, ctx);
        } else {
            // SKIP — filter out nulls
            return filterNotNull(input, colIndex, ctx);
        }
    }

    private static RelNode projectWithCoalesce(RelNode input, int colIndex, ConversionContext ctx) {
        RexBuilder rexBuilder = ctx.getRexBuilder();
        RelDataType rowType = input.getRowType();
        List<RelDataTypeField> fields = rowType.getFieldList();

        List<RexNode> projects = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            RexNode ref = rexBuilder.makeInputRef(field.getType(), i);
            if (i == colIndex) {
                // COALESCE(col, 0)
                RexNode zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO, field.getType());
                ref = rexBuilder.makeCall(SqlStdOperatorTable.COALESCE, ref, zero);
            }
            projects.add(ref);
            names.add(field.getName());
        }
        return LogicalProject.create(input, List.of(), projects, names, ImmutableSet.of());
    }

    private static RelNode filterNotNull(RelNode input, int colIndex, ConversionContext ctx) {
        RexBuilder rexBuilder = ctx.getRexBuilder();
        RelDataType rowType = input.getRowType();
        RelDataTypeField field = rowType.getFieldList().get(colIndex);
        RexNode ref = rexBuilder.makeInputRef(field.getType(), colIndex);
        RexNode isNotNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ref);
        return LogicalFilter.create(input, isNotNull);
    }
}
