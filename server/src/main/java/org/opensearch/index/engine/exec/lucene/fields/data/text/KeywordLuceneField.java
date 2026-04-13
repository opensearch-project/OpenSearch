/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data.text;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class KeywordLuceneField extends LuceneField {

    private static final Map<Long, FieldType> FIELD_TYPE_CACHE = new ConcurrentHashMap<>();

    @Override
    public void createField(MappedFieldType fieldType, ParseContext.Document document, Object parseValue) {
        String value = (String) parseValue;
        final BytesRef binaryValue = new BytesRef(value);

        boolean shouldIndex = fieldType.isSearchable();
        boolean shouldStore = fieldType.isStored();

        if (shouldIndex || shouldStore) {
            FieldType luceneFieldType = getFieldType(fieldType);
            document.add(new Field(fieldType.name(), binaryValue, luceneFieldType));
        }

        if (fieldType.hasDocValues()) {
            document.add(new SortedSetDocValuesField(fieldType.name(), binaryValue));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }

    private FieldType getFieldType(MappedFieldType fieldType) {
        long key = (fieldType.isStored() ? 1L : 0L) | (fieldType.isSearchable() ? 2L : 0L);
        return FIELD_TYPE_CACHE.computeIfAbsent(key, k -> {
            FieldType ft = new FieldType();
            ft.setTokenized(false);
            ft.setStored(fieldType.isStored());
            ft.setOmitNorms(true);
            ft.setIndexOptions(fieldType.isSearchable() ? IndexOptions.DOCS : IndexOptions.NONE);
            ft.freeze();
            return ft;
        });
    }

}
