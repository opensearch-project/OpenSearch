/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.dsl.converter.ConversionException;

import java.util.List;

/**
 * Represents a grouping contribution from a bucket aggregation.
 * Implementations provide field-based grouping (terms) or
 * expression-based grouping (histogram, range) without modifying this interface.
 */
public interface GroupingInfo {

    /** Returns the logical field names this grouping contributes. */
    List<String> getFieldNames();

    /**
     * Resolves this grouping to column indices in the input schema.
     *
     * @param inputRowType the schema before aggregation
     * @return column indices for the GROUP BY bit set
     * @throws ConversionException if field lookup fails
     */
    List<Integer> resolveIndices(RelDataType inputRowType) throws ConversionException;
}
