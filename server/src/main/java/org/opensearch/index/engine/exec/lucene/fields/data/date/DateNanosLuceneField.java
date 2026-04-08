/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data.date;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Set;

public class DateNanosLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType fieldType, ParseContext.Document document, Object parseValue) {
        final long timestamp = (long) parseValue;
        if (fieldType.isSearchable()) {
            document.add(new LongPoint(fieldType.name(), timestamp));
        }
        if (fieldType.hasDocValues()) {
            document.add(new SortedNumericDocValuesField(fieldType.name(), timestamp));
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), timestamp));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }
}
