/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields;

import org.apache.lucene.document.Document;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.Objects;

/**
 * Abstract base class for Lucene field implementations that handle conversion
 * between OpenSearch field types and Lucene document fields.
 */
public abstract class LuceneField {

    /** Creates a new LuceneField. */
    public LuceneField() {}

    /**
     * Adds the parsed field value as one or more Lucene fields to the given document.
     *
     * @param fieldType the mapped field type
     * @param document the Lucene document to add fields to
     * @param parseValue the parsed value to write
     */
    protected abstract void addToDocument(MappedFieldType fieldType, Document document, Object parseValue);

    /**
     * Creates and processes a field entry. Validates inputs before delegating to subclass.
     *
     * @param fieldType the mapped field type
     * @param document the Lucene document to add fields to
     * @param parseValue the parsed value to write
     */
    public final void createField(MappedFieldType fieldType, Document document, Object parseValue) {
        Objects.requireNonNull(fieldType, "MappedFieldType cannot be null");
        Objects.requireNonNull(document, "Document cannot be null");
        addToDocument(fieldType, document, parseValue);
    }
}
