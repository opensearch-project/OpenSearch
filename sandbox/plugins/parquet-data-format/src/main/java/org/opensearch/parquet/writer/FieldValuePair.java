/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.opensearch.index.mapper.MappedFieldType;

/**
 * Represents a field-value pair collected during document input processing.
 */
public class FieldValuePair {

    private final MappedFieldType fieldType;
    private final Object value;

    public FieldValuePair(MappedFieldType fieldType, Object value) {
        if (fieldType == null) {
            throw new IllegalArgumentException("fieldType cannot be null");
        }
        this.fieldType = fieldType;
        this.value = value;
    }

    public MappedFieldType getFieldType() {
        return fieldType;
    }

    public Object getValue() {
        return value;
    }
}
