/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data;

import org.apache.lucene.document.StoredField;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Set;

public class BinaryLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType fieldType, ParseContext.Document document, Object parseValue) {
        final byte[] value = (byte[]) parseValue;
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), value));
        }
        if (fieldType.hasDocValues()) {
            BinaryFieldMapper.CustomBinaryDocValuesField field =
                (BinaryFieldMapper.CustomBinaryDocValuesField) document.getByKey(fieldType.name());
            if (field == null) {
                field = new BinaryFieldMapper.CustomBinaryDocValuesField(fieldType.name(), value);
                document.addWithKey(fieldType.name(), field);
            } else {
                field.add(value);
            }
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.DOC_VALUES);
    }
}
