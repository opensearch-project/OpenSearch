/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.ExpressionGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a {@link LogicalAggregate} from pre-computed {@link AggregationMetadata}.
 * The metadata is produced by the tree walker and set on the context before this runs.
 */
public class AggregateConverter {

    /** Creates an aggregate converter. */
    public AggregateConverter() {}

    /**
     * Builds a LogicalAggregate from the given metadata.
     *
     * @param input the input plan (scan + filter)
     * @param metadata pre-computed aggregation metadata for one granularity
     * @param rexBuilder the RexBuilder for creating expressions
     * @return the LogicalAggregate node
     * @throws ConversionException if expression building fails
     */
    public RelNode convert(RelNode input, AggregationMetadata metadata, RexBuilder rexBuilder) throws ConversionException {
        RelNode planInput = input;

        if (hasExpressionGrouping(metadata)) {
            planInput = addProjectForExpressions(input, metadata, rexBuilder);
        }

        return LogicalAggregate.create(
            planInput,
            metadata.getGroupByBitSet(),
            null,
            metadata.getAggregateCalls()
        );
    }

    private static boolean hasExpressionGrouping(AggregationMetadata metadata) {
        return metadata.getGroupings().stream().anyMatch(g -> g instanceof ExpressionGrouping);
    }

    private static RelNode addProjectForExpressions(RelNode input, AggregationMetadata metadata, RexBuilder rexBuilder)
            throws ConversionException {
        RelDataType inputRowType = input.getRowType();
        List<RexNode> projects = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        for (RelDataTypeField field : inputRowType.getFieldList()) {
            projects.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
            fieldNames.add(field.getName());
        }

        for (GroupingInfo grouping : metadata.getGroupings()) {
            if (grouping instanceof ExpressionGrouping exprGrouping) {
                RexNode expr = exprGrouping.buildExpression(inputRowType, rexBuilder);
                projects.add(expr);
                fieldNames.add(exprGrouping.getProjectedColumnName());
            }
        }

        return LogicalProject.create(input, List.of(), projects, fieldNames);
    }
}
