/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.search.aggregations.pipeline.BucketHelpers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Prepares the sibling's metric columns for the second-level aggregate: applies
 * {@code gap_policy} and widens each referenced column to DOUBLE in one projection.
 *
 * <p>{@code skip} needs no rewrite: a bucket whose metric is SQL NULL is excluded from
 * both the numerator and denominator by the aggregate's native NULL handling — exactly
 * vanilla's skip semantics. {@code insert_zeros} rewrites the column to
 * {@code COALESCE(column, 0)}, so the bucket contributes zero and is counted.
 *
 * <p>The DOUBLE cast keeps the second-level call's declared and inferred types aligned
 * and makes the engine's AVG decomposition divide in floating point even when the
 * metric column is integral.
 *
 * <p>Vanilla additionally treats zero-doc-count buckets as gaps; those cannot occur here
 * because a SQL group only exists when rows exist, so a gap is always a NULL metric.
 */
public final class MetricColumnPreparer {

    private MetricColumnPreparer() {}

    /**
     * Wraps the input in a projection preparing the given metric columns. Non-referenced
     * columns pass through unchanged; referenced columns keep their name and position.
     *
     * @param input the shaped sibling output (post filter/sort)
     * @param rexBuilder the rex builder
     * @param policiesByColumn gap policy per referenced metric column index
     * @return the input, or a projection over it
     */
    public static RelNode prepare(RelNode input, RexBuilder rexBuilder, Map<Integer, BucketHelpers.GapPolicy> policiesByColumn) {
        if (policiesByColumn.isEmpty()) {
            return input;
        }
        RelDataType doubleType = rexBuilder.getTypeFactory()
            .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true);
        List<RelDataTypeField> fields = input.getRowType().getFieldList();
        List<RexNode> projects = new ArrayList<>(fields.size());
        List<String> names = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            RexNode ref = rexBuilder.makeInputRef(field.getType(), i);
            BucketHelpers.GapPolicy policy = policiesByColumn.get(i);
            if (policy != null) {
                if (policy == BucketHelpers.GapPolicy.INSERT_ZEROS) {
                    RexNode zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO, field.getType());
                    ref = rexBuilder.makeCall(SqlStdOperatorTable.COALESCE, ref, zero);
                }
                // matchNullability=false keeps the target's nullable DOUBLE even over
                // NOT NULL columns (_count), so the AVG call's inferred type is stable.
                ref = rexBuilder.makeCast(doubleType, ref, false);
            }
            projects.add(ref);
            names.add(field.getName());
        }
        return LogicalProject.create(input, List.of(), projects, names);
    }
}
