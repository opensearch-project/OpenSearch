/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data.number;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Lucene field for 64-bit signed long values.
 * Conditionally adds a {@link LongPoint} for range queries, a {@link SortedNumericDocValuesField}
 * (or its skip-list variant) for sorting/aggregations, and a {@link StoredField}
 * based on the field's mapping configuration.
 */
public class LongLuceneField extends LuceneField {

    /** Creates a new LongLuceneField. */
    public LongLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        long value = ((Number) parseValue).longValue();
        if (fieldType.isSearchable()) {
            document.add(new LongPoint(fieldType.name(), value));
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
