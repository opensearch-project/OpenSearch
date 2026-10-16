/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data.number;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Lucene field for double-precision floating-point values.
 * Conditionally adds a {@link DoublePoint} for range queries, a {@link SortedNumericDocValuesField}
 * (or its skip-list variant, using sortable double-to-long encoding) for sorting/aggregations,
 * and a {@link StoredField} based on the field's mapping configuration.
 */
public class DoubleLuceneField extends LuceneField {

    /** Creates a new DoubleLuceneField. */
    public DoubleLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        double value = ((Number) parseValue).doubleValue();
        if (fieldType.isSearchable()) {
            document.add(new DoublePoint(fieldType.name(), value));
        }
        if (fieldType.hasDocValues()) {
            if (fieldType.hasSkipList()) {
                document.add(SortedNumericDocValuesField.indexedField(fieldType.name(), NumericUtils.doubleToSortableLong(value)));
            } else {
                document.add(new SortedNumericDocValuesField(fieldType.name(), NumericUtils.doubleToSortableLong(value)));
            }
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), value));
        }
    }
}
