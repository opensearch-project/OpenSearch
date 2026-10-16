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
import org.apache.lucene.sandbox.document.BigIntegerPoint;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.common.Numbers;
import org.opensearch.index.mapper.MappedFieldType;

import java.math.BigInteger;

/**
 * Lucene field for 64-bit unsigned long values.
 * The incoming value is a {@link Number} (typically {@link BigInteger}) as parsed by the NumberFieldMapper.
 * Conditionally adds a {@link BigIntegerPoint} for range queries, a {@link SortedNumericDocValuesField}
 * (or its skip-list variant, using the raw long bits) for sorting/aggregations, and a {@link StoredField}
 * based on the field's mapping configuration.
 */
public class UnsignedLongLuceneField extends LuceneField {

    /** Creates a new UnsignedLongLuceneField. */
    public UnsignedLongLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        BigInteger value = Numbers.toUnsignedLongExact((Number) parseValue);
        if (fieldType.isSearchable()) {
            document.add(new BigIntegerPoint(fieldType.name(), value));
        }
        if (fieldType.hasDocValues()) {
            if (fieldType.hasSkipList()) {
                document.add(SortedNumericDocValuesField.indexedField(fieldType.name(), value.longValue()));
            } else {
                document.add(new SortedNumericDocValuesField(fieldType.name(), value.longValue()));
            }
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), value.toString()));
        }
    }
}
