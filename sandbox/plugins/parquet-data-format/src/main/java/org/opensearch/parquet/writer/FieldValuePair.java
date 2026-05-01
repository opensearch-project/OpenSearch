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
 * Immutable pair of an OpenSearch {@link MappedFieldType} and its parsed value.
 *
 * <p>Represents a single field entry collected by {@link ParquetDocumentInput} during
 * document indexing. The field type is used to resolve the corresponding Arrow vector
 * type via {@link org.opensearch.parquet.fields.ArrowFieldRegistry}, and the value is
 * written into that vector during document transfer to the VSR.
 *
 * <p>The field type must not be null (enforced by constructor); the value may be null
 * for nullable fields.
 */
public class FieldValuePair {

    private final MappedFieldType fieldType;
    private volatile Object value;

    /**
     * Creates a new FieldValuePair.
     *
     * @param fieldType the mapped field type
     * @param value the parsed field value
     */
    public FieldValuePair(MappedFieldType fieldType, Object value) {
        if (fieldType == null) {
            throw new IllegalArgumentException("fieldType cannot be null");
        }
        this.fieldType = fieldType;
        this.value = value;
    }

    /**
     * Returns the field type.
     *
     * @return the mapped field type
     */
    public MappedFieldType getFieldType() {
        return fieldType;
    }

    void setValue(Object value) {
        if (value.getClass() != this.value.getClass()) {
            throw new IllegalArgumentException("Cannot change value type");
        }
        this.value = value;
    }

    /**
     * Returns the value.
     *
     * @return the parsed field value
     */
    public Object getValue() {
        return value;
    }
}
