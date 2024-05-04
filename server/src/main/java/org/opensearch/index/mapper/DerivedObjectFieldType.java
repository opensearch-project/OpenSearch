/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.script.DerivedFieldScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DerivedObjectFieldType extends DerivedFieldType {

    DerivedObjectFieldType(
        DerivedField derivedField,
        FieldMapper typeFieldMapper,
        Function<Object, IndexableField> fieldFunction,
        IndexAnalyzers indexAnalyzers
    ) {
        super(derivedField, typeFieldMapper, fieldFunction, indexAnalyzers);
    }

    @Override
    TextFieldMapper.TextFieldType getSourceIndexedFieldType(String sourceIndexedField, QueryShardContext context) {
        if (sourceIndexedField == null || sourceIndexedField.isEmpty()) {
            return null;
        }
        // TODO error handling
        MappedFieldType mappedFieldType = context.fieldMapper(sourceIndexedField);
        if (!(mappedFieldType instanceof TextFieldMapper.TextFieldType)) {
            // TODO: throw validation error?
            return null;
        }
        return (TextFieldMapper.TextFieldType) mappedFieldType;
    }

    @Override
    public DerivedFieldValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
        }
        Function<Object, Object> valueForDisplay = DerivedFieldSupportedTypes.getValueForDisplayGenerator(getType());
        String subFieldName = name().substring(name().indexOf(".") + 1);
        return new DerivedObjectFieldValueFetcher(
            subFieldName,
            getDerivedFieldLeafFactory(derivedField.getScript(), context, searchLookup == null ? context.lookup() : searchLookup),
            valueForDisplay
        );
    }

    public static class DerivedObjectFieldValueFetcher extends DerivedFieldValueFetcher {
        private final String subField;

        public DerivedObjectFieldValueFetcher(
            String subField,
            DerivedFieldScript.LeafFactory derivedFieldScriptFactory,
            Function<Object, Object> valueForDisplay
        ) {
            super(derivedFieldScriptFactory, valueForDisplay);
            this.subField = subField;
        }

        @Override
        public List<Object> fetchValuesInternal(SourceLookup lookup) {
            List<Object> jsonObjects = super.fetchValuesInternal(lookup);
            // TODO add check for valid json and error handling around the same if mismatch
            // parse the value of field from the json object and return that instead
            List<Object> result = new ArrayList<>();
            for (Object o : jsonObjects) {
                Map<String, Object> s = XContentHelper.convertToMap(JsonXContent.jsonXContent, (String) o, false);
                result.add(getNestedField(s, subField));
            }
            return result;
        }

        private static Object getNestedField(Map<String, Object> obj, String key) {
            String[] keyParts = key.split("\\.");
            Map<String, Object> currentObj = obj;
            for (int i = 0; i < keyParts.length - 1; i++) {
                Object value = currentObj.get(keyParts[i]);
                if (value instanceof Map) {
                    currentObj = (Map<String, Object>) value;
                } else {
                    return null;
                }
            }
            return currentObj.get(keyParts[keyParts.length - 1]);
        }
    }
}
