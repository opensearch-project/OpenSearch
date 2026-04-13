/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.dsl.converter.ConversionException;

import java.util.ArrayList;
import java.util.List;

/**
 * Field-based grouping for simple field references without expressions.
 * Used for terms and multi_terms aggregations.
 */
public class SimpleFieldGrouping implements FieldGrouping {

    private final List<String> fieldNames;

    /**
     * Creates a field grouping for the specified fields.
     * @param fieldNames the fields to group by
     */
    public SimpleFieldGrouping(List<String> fieldNames) {
        this.fieldNames = List.copyOf(fieldNames);
    }

    @Override
    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public List<Integer> resolveIndices(RelDataType inputRowType) throws ConversionException {
        List<Integer> indices = new ArrayList<>(fieldNames.size());
        for (String name : fieldNames) {
            RelDataTypeField field = inputRowType.getField(name, true, false);
            if (field == null) {
                throw new ConversionException("Group-by field '" + name + "' not found in schema");
            }
            indices.add(field.getIndex());
        }
        return indices;
    }
}
