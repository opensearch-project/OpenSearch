/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.opensearch.OpenSearchParseException;
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

/**
 * ObjectDerivedFieldType is similar to object field type in context of derived fields.
 * It is not a primitive field type and doesn't support any queries directly. However, any nested derived field with parent as
 * ObjectDerivedFieldType will make use of it to run query once the field type is inferred.
 */
public class ObjectDerivedFieldType extends DerivedFieldType {

    ObjectDerivedFieldType(
        DerivedField derivedField,
        FieldMapper typeFieldMapper,
        Function<Object, IndexableField> fieldFunction,
        IndexAnalyzers indexAnalyzers
    ) {
        super(derivedField, typeFieldMapper, derivedField.getType().equals(DerivedFieldSupportedTypes.DATE.getName()) ? (o -> {
            // this is needed to support date type for nested fields, they need to be converted to long to create
            // IndexableField
            if (o instanceof String) {
                return fieldFunction.apply(((DateFieldMapper) typeFieldMapper).fieldType().parse((String) o));
            } else {
                return fieldFunction.apply(o);
            }
        }) : fieldFunction, indexAnalyzers);
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
        return new ObjectDerivedFieldValueFetcher(
            subFieldName,
            getDerivedFieldLeafFactory(derivedField.getScript(), context, searchLookup == null ? context.lookup() : searchLookup),
            valueForDisplay
        );
    }

    public static class ObjectDerivedFieldValueFetcher extends DerivedFieldValueFetcher {
        private final String subField;

        // TODO add it as part of index setting?
        private final boolean failOnInvalidJsonObjects;

        public ObjectDerivedFieldValueFetcher(
            String subField,
            DerivedFieldScript.LeafFactory derivedFieldScriptFactory,
            Function<Object, Object> valueForDisplay
        ) {
            super(derivedFieldScriptFactory, valueForDisplay);
            this.subField = subField;
            this.failOnInvalidJsonObjects = true;
        }

        @Override
        public List<Object> fetchValuesInternal(SourceLookup lookup) {
            List<Object> jsonObjects = super.fetchValuesInternal(lookup);
            List<Object> result = new ArrayList<>();
            for (Object o : jsonObjects) {
                try {
                    Map<String, Object> s = XContentHelper.convertToMap(JsonXContent.jsonXContent, (String) o, false);
                    result.add(getNestedField(s, subField));
                } catch (OpenSearchParseException e) {
                    if (failOnInvalidJsonObjects) {
                        throw e;
                    }
                    // TODO cannot log warnings as it can bloat up the logs. Add to some stats?
                }
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
