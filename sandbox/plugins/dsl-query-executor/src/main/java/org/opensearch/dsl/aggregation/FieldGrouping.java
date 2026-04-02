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
 * Grouping strategy for field-based bucket aggregations (terms, multi_terms, missing, rare_terms).
 * Fields already exist in the input schema and can be resolved to column indices.
 */
public interface FieldGrouping extends GroupingInfo {

    /**
     * Resolves field names to column indices in the input row type.
     * @param inputRowType the input schema
     * @return list of column indices corresponding to the grouping fields
     */
    List<Integer> resolveIndices(RelDataType inputRowType) throws ConversionException;
}
