/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.metadata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Lucene field for _size metadata stored as a 32-bit integer.
 * Conditionally adds an {@link IntPoint} for range queries, a {@link SortedNumericDocValuesField}
 * (or its skip-list variant) for sorting/aggregations, and a {@link StoredField}.
 */
public class SizeLuceneField extends LuceneField {

    /** Creates a new SizeLuceneField. */
    public SizeLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        int value = ((Number) parseValue).intValue();
        if (fieldType.isSearchable()) {
            document.add(new IntPoint(fieldType.name(), value));
        }
        if (fieldType.hasDocValues()) {
            if (fieldType.hasSkipList()) {
                document.add(SortedNumericDocValuesField.indexedField(fieldType.name(), value));
            } else {
                document.add(new SortedNumericDocValuesField(fieldType.name(), value));
            }
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), value));
        }
    }
}
