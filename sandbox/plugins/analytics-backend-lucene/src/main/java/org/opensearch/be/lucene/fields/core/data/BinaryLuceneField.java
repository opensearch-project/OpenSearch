/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Lucene field for binary data.
 * Conditionally adds a {@link StoredField} for retrieval, matching the pattern in
 * {@link org.opensearch.index.mapper.BinaryFieldMapper#parseCreateField}.
 */
public class BinaryLuceneField extends LuceneField {

    /** Creates a new BinaryLuceneField. */
    public BinaryLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        byte[] value = (byte[]) parseValue;
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), value));
        }
    }
}
