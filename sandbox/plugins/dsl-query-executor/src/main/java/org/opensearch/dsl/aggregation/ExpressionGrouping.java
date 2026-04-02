/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.converter.ConversionException;

/**
 * Grouping strategy for expression-based bucket aggregations (histogram, date_histogram).
 * Expressions are computed from input fields and added as new columns by LogicalProject.
 */
public interface ExpressionGrouping extends GroupingInfo {

    /**
     * Builds a Calcite expression that computes the grouping value.
     * @param inputRowType the input schema
     * @param builder the RexBuilder for constructing expressions
     * @return the computed expression node
     */
    RexNode buildExpression(RelDataType inputRowType, RexBuilder builder)
            throws ConversionException;

    /**
     * Returns the name of the projected column that will hold the computed expression result.
     * Used for sorting and child key filters.
     */
    String getProjectedColumnName();
}
