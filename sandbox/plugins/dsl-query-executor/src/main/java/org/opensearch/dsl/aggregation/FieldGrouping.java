/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import java.util.List;
import java.util.Map;

/**
 * Field-based grouping: GROUP BY field1, field2, ...
 */
public class FieldGrouping implements GroupingInfo {

    private final List<String> fieldNames;
    private final Map<String, Object> missingByField;

    /**
     * Creates a field grouping with no {@code missing} substitutions.
     *
     * @param fieldNames the field names to group by
     */
    public FieldGrouping(List<String> fieldNames) {
        this(fieldNames, Map.of());
    }

    /**
     * Creates a field grouping.
     *
     * @param fieldNames the field names to group by
     * @param missingByField the {@code missing} null-substitution value per field; fields
     *        absent from the map exclude null-valued documents from grouping
     */
    public FieldGrouping(List<String> fieldNames, Map<String, Object> missingByField) {
        this.fieldNames = List.copyOf(fieldNames);
        this.missingByField = Map.copyOf(missingByField);
    }

    @Override
    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public Map<String, Object> getMissingByField() {
        return missingByField;
    }
}
