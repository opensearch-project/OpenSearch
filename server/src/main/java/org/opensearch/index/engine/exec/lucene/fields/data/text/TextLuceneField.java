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
import org.apache.lucene.index.IndexOptions;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TextLuceneField extends LuceneField {

    private static final Map<Long, FieldType> FIELD_TYPE_CACHE = new ConcurrentHashMap<>();

    @Override
    public void createField(MappedFieldType fieldType, ParseContext.Document document, Object parseValue) {
        final String value = (String) parseValue;

        boolean shouldIndex = fieldType.isSearchable();
        boolean shouldStore = fieldType.isStored();

        if (shouldIndex || shouldStore) {
            FieldType luceneFieldType = getFieldType(fieldType);
            Field field = new Field(fieldType.name(), value, luceneFieldType);
            document.add(field);
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX);
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
