/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data.number;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Lucene field for single-precision floating-point values.
 * Conditionally adds a {@link FloatPoint} for range queries, a {@link SortedNumericDocValuesField}
 * (or its skip-list variant, using sortable float-to-int encoding) for sorting/aggregations,
 * and a {@link StoredField} based on the field's mapping configuration.
 */
public class FloatLuceneField extends LuceneField {

    /** Creates a new FloatLuceneField. */
    public FloatLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        float value = ((Number) parseValue).floatValue();
        if (fieldType.isSearchable()) {
            document.add(new FloatPoint(fieldType.name(), value));
        }
        if (fieldType.hasDocValues()) {
            if (fieldType.hasSkipList()) {
                document.add(SortedNumericDocValuesField.indexedField(fieldType.name(), NumericUtils.floatToSortableInt(value)));
            } else {
                document.add(new SortedNumericDocValuesField(fieldType.name(), NumericUtils.floatToSortableInt(value)));
            }
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), value));
        }
    }
}
