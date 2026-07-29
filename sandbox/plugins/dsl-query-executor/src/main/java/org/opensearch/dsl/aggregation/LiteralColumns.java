/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

/**
 * Allocates literal-derived columns in the aggregate's input: constants for aggregate
 * calls whose arguments are literals, and {@code COALESCE(field, value)} columns for the
 * {@code missing} request parameter. Calcite {@code AggregateCall} arguments are input
 * column indices, so both kinds must exist as projected columns below the
 * {@code LogicalAggregate}; the converter materializes every allocated column in a
 * pre-aggregate {@code LogicalProject}.
 */
public interface LiteralColumns {

    /**
     * Returns the input column index that will carry the given constant, allocating a new
     * column on first use. Equal values share one column.
     *
     * @param value the literal value
     * @return the column index in the aggregate's input row
     */
    int columnFor(double value);

    /**
     * Returns the input column index that will carry the given exact-integer constant,
     * allocating a new column on first use. Equal values share one column.
     *
     * @param value the literal value
     * @return the column index in the aggregate's input row
     */
    int integerColumnFor(long value);

    /**
     * Returns the input column index that will carry {@code COALESCE(field, missingValue)},
     * allocating a new column on first use. Equal (field, value) pairs share one column.
     *
     * @param fieldIndex input index of the field to coalesce
     * @param missingValue substitute for SQL NULL (the {@code missing} request parameter)
     * @return the column index in the aggregate's input row
     */
    int coalescedColumnFor(int fieldIndex, double missingValue);
}
