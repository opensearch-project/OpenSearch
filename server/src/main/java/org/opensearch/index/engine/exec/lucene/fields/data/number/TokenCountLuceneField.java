/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data.number;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Set;

public class TokenCountLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType fieldType, ParseContext.Document document, Object parseValue) {
        final Number value = (Number) parseValue;
        if (fieldType.isSearchable()) {
            document.add(new IntPoint(fieldType.name(), value.intValue()));
        }
        if (fieldType.hasDocValues()) {
            document.add(new SortedNumericDocValuesField(fieldType.name(), value.intValue()));
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), value.intValue()));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }
}
