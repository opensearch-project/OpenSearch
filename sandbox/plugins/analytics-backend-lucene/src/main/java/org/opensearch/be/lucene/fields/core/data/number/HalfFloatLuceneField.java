/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data.number;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Lucene field for half-precision (16-bit) floating-point values.
 * The incoming value is a {@link Float} as parsed by the NumberFieldMapper.
 * Conditionally adds a {@link HalfFloatPoint} for range queries, a {@link SortedNumericDocValuesField}
 * (or its skip-list variant, using {@link HalfFloatPoint#halfFloatToSortableShort}) for sorting/aggregations,
 * and a {@link StoredField} based on the field's mapping configuration.
 */
public class HalfFloatLuceneField extends LuceneField {

    /** Creates a new HalfFloatLuceneField. */
    public HalfFloatLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        float value = ((Number) parseValue).floatValue();
        if (fieldType.isSearchable()) {
            document.add(new HalfFloatPoint(fieldType.name(), value));
        }
        if (fieldType.hasDocValues()) {
            if (fieldType.hasSkipList()) {
                document.add(SortedNumericDocValuesField.indexedField(fieldType.name(), HalfFloatPoint.halfFloatToSortableShort(value)));
            } else {
                document.add(new SortedNumericDocValuesField(fieldType.name(), HalfFloatPoint.halfFloatToSortableShort(value)));
            }
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), value));
        }
    }
}
