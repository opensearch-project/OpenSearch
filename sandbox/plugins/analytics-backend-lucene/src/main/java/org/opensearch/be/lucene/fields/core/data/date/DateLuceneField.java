/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data.date;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Lucene field for date values stored as millisecond timestamps.
 * Conditionally adds a {@link LongPoint} for range queries, a {@link SortedNumericDocValuesField}
 * (or its skip-list variant) for sorting/aggregations, and a {@link StoredField}, matching the
 * pattern in {@link org.opensearch.index.mapper.DateFieldMapper#parseCreateField}.
 */
public class DateLuceneField extends LuceneField {

    /** Creates a new DateLuceneField. */
    public DateLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        long timestamp = (long) parseValue;
        if (fieldType.isSearchable()) {
            document.add(new LongPoint(fieldType.name(), timestamp));
        }
        if (fieldType.hasDocValues()) {
            if (fieldType.hasSkipList()) {
                document.add(SortedNumericDocValuesField.indexedField(fieldType.name(), timestamp));
            } else {
                document.add(new SortedNumericDocValuesField(fieldType.name(), timestamp));
            }
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), timestamp));
        }
    }
}
