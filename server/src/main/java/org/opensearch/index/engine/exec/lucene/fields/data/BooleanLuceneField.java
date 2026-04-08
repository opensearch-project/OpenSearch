/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Set;

public class BooleanLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType fieldType, ParseContext.Document document, Object parseValue) {
        final Boolean value = (Boolean) parseValue;
        if (fieldType.isSearchable()) {
            FieldType ft = new FieldType();
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.DOCS);
            ft.setTokenized(false);
            ft.freeze();
            document.add(new Field(fieldType.name(), value ? "T" : "F", ft));
        }
        if (fieldType.hasDocValues()) {
            document.add(new SortedNumericDocValuesField(fieldType.name(), value ? 1 : 0));
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), value ? "T" : "F"));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }
}
