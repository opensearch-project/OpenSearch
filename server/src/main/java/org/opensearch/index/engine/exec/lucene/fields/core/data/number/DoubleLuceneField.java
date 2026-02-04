/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.core.data.number;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ParseContext;

public class DoubleLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue) {
        final NumberFieldMapper.NumberFieldType fieldType = (NumberFieldMapper.NumberFieldType) mappedFieldType;
        final Number value = (Number) parseValue;
        if (fieldType.isSearchable()) {
            document.add(new DoublePoint(fieldType.name(), value.doubleValue()));
        }
        if (fieldType.hasDocValues()) {
            if (fieldType.isSkiplist()) {
                document.add(SortedNumericDocValuesField.indexedField(
                    fieldType.name(),
                    NumericUtils.doubleToSortableLong(value.doubleValue())
                ));
            } else {
                document.add(new SortedNumericDocValuesField(fieldType.name(), NumericUtils.doubleToSortableLong(value.doubleValue())));
            }
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), value.doubleValue()));
        }
    }
}
