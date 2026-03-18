/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.Map;

/**
 * Per-format view of field capability assignments resolved by the composite engine.
 * Maps fieldName → MappedFieldType that this format is responsible for.
 *
 * <p>Used by DocumentInput implementations to decide whether to write a given field.
 * If a field name has no entry, this format should skip it entirely.
 */
@ExperimentalApi
public class FieldAssignments {

    private final Map<String, MappedFieldType> fieldTypes;

    public FieldAssignments(Map<String, MappedFieldType> fieldTypes) {
        this.fieldTypes = Map.copyOf(fieldTypes);
    }

    /**
     * Returns true if this format should handle the given field name.
     */
    public boolean shouldHandle(String fieldName) {
        return fieldTypes.containsKey(fieldName);
    }

    /**
     * Returns the MappedFieldType for a given field name, or null if none.
     */
    public MappedFieldType getFieldType(String fieldName) {
        return fieldTypes.get(fieldName);
    }
}
