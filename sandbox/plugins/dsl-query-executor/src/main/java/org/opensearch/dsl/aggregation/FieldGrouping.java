/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import java.util.List;

/**
 * Field-based grouping: GROUP BY field1, field2, ...
 * Used by terms and multi_terms bucket aggregations.
 */
public class FieldGrouping implements GroupingInfo {

    private final List<String> fieldNames;

    /**
     * Creates a field grouping.
     *
     * @param fieldNames the field names to group by
     */
    public FieldGrouping(List<String> fieldNames) {
        this.fieldNames = List.copyOf(fieldNames);
    }

    @Override
    public List<String> getFieldNames() {
        return fieldNames;
    }
}
