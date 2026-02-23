/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.core.data.number;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ParseContext;

public class HalfFloatLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue) {
        final NumberFieldMapper.NumberFieldType fieldType = (NumberFieldMapper.NumberFieldType) mappedFieldType;
        final Number value = (Number) parseValue;
        if (fieldType.isSearchable()) {
            document.add(new HalfFloatPoint(fieldType.name(), value.floatValue()));
        }
        if (fieldType.hasDocValues()) {
            if (fieldType.isSkiplist()) {
                document.add(SortedNumericDocValuesField.indexedField(
                    fieldType.name(),
                    HalfFloatPoint.halfFloatToSortableShort(value.floatValue())
                ));
            } else {
                document.add(new SortedNumericDocValuesField(
                    fieldType.name(),
                    HalfFloatPoint.halfFloatToSortableShort(value.floatValue())
                ));
            }
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), value.floatValue()));
        }
    }
}
