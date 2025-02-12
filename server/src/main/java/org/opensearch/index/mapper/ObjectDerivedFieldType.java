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
import org.opensearch.common.time.DateFormatter;
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
 * Represents a derived field in OpenSearch, which behaves similarly to an Object field type within the context of derived fields.
 * It is not a primitive field type and does not directly support queries. However, any nested derived fields contained within a DerivedField object
 * are also classified as Object derived fields, which support queries depending on their inferred type.
 *
 * <p>
 * For example, consider the following mapping:
 * <pre>
 * mappings:
 *     derived:
 *         regular_field:
 *             type: keyword
 *             script: "emit(keyword_string)"
 *         derived_obj:
 *             type: object
 *             script: "emit(json_obj)"
 * </pre>
 *
 * Here, we have a regular keyword derived field and an object type derived field. Any nested field within `derived_obj` does not need to be explicitly defined.
 * Their type will be inferred, and the value will be extracted from the `json_obj` emitted by the script associated with the parent object `derived_obj`.
 * The {@link ObjectDerivedFieldValueFetcher} is used for this purpose, which accepts `sub_field` and can extract the nested fields from JSON.
 *
 * <p>
 * For instance, if `derived_obj` emits the following document:
 * <pre>
 * "derived_obj" : {
 *     "sub_field_1": "value 1",
 *     "sub_field_2": {
 *         "sub_field_3": "value_3"
 *     }
 * }
 * </pre>
 *
 * Then nested fields such as `sub_field_1` and `sub_field_3` can be used in the query as `derived_obj.sub_field_1` and `derived_obj.sub_field_2.sub_field_3` respectively.
 * Both of these nested derived fields will be an instance of `ObjectDerivedFieldType`; however, their mapped field type will be inferred based on the type of value they hold, to support queries on them.
 *
 * @see FieldTypeInference for details on the type inference logic used in derived fields.
 */
public class ObjectDerivedFieldType extends DerivedFieldType {

    ObjectDerivedFieldType(
        DerivedField derivedField,
        FieldMapper typeFieldMapper,
        Function<Object, IndexableField> fieldFunction,
        IndexAnalyzers indexAnalyzers
    ) {
        super(derivedField, typeFieldMapper, derivedField.getType().equals(DerivedFieldSupportedTypes.DATE.getName()) ? (o -> {
            // this is needed to support date type for nested fields as they are required to be converted to long to create
            // IndexableField
            if (o instanceof String) {
                return fieldFunction.apply(((DateFieldMapper) typeFieldMapper).fieldType().parse((String) o));
            } else {
                return fieldFunction.apply(o);
            }
        }) : fieldFunction, indexAnalyzers);
    }

    @Override
    public DerivedFieldValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
        }
        Function<Object, Object> valueForDisplay = DerivedFieldSupportedTypes.getValueForDisplayGenerator(
            getType(),
            derivedField.getFormat() != null ? DateFormatter.forPattern(derivedField.getFormat()) : null
        );

        Function<Object, Object> dateFormatter = derivedField.getType().equals(DerivedFieldSupportedTypes.DATE.getName()) ? (o -> {
            // this is needed to support date type for nested fields as they are required to be converted to long
            if (o instanceof String) {
                return ((DateFieldMapper) typeFieldMapper).fieldType().parse((String) o);
            } else {
                return o;
            }
        }) : null;

        String subFieldName = name().substring(name().indexOf(".") + 1);
        return new ObjectDerivedFieldValueFetcher(
            subFieldName,
            getDerivedFieldLeafFactory(derivedField.getScript(), context, searchLookup == null ? context.lookup() : searchLookup),
            valueForDisplay,
            derivedField.getIgnoreMalformed(),
            dateFormatter
        );
    }

    static class ObjectDerivedFieldValueFetcher extends DerivedFieldValueFetcher {
        private final String subField;

        // TODO add it as part of index setting?
        private final boolean ignoreOnMalFormed;

        private final Function<Object, Object> dateFormatter;

        ObjectDerivedFieldValueFetcher(
            String subField,
            DerivedFieldScript.LeafFactory derivedFieldScriptFactory,
            Function<Object, Object> valueForDisplay,
            boolean ignoreOnMalFormed
        ) {
            super(derivedFieldScriptFactory, valueForDisplay);
            this.subField = subField;
            this.ignoreOnMalFormed = ignoreOnMalFormed;
            this.dateFormatter = null;
        }

        ObjectDerivedFieldValueFetcher(
            String subField,
            DerivedFieldScript.LeafFactory derivedFieldScriptFactory,
            Function<Object, Object> valueForDisplay,
            boolean ignoreOnMalFormed,
            Function<Object, Object> dateFormatter
        ) {
            super(derivedFieldScriptFactory, valueForDisplay);
            this.subField = subField;
            this.ignoreOnMalFormed = ignoreOnMalFormed;
            this.dateFormatter = dateFormatter;
        }

        @Override
        public List<Object> fetchValuesInternal(SourceLookup lookup) {
            List<Object> jsonObjects = super.fetchValuesInternal(lookup);
            List<Object> result = new ArrayList<>();
            for (Object o : jsonObjects) {
                try {
                    if (o == null) {
                        continue;
                    }
                    Map<String, Object> s = XContentHelper.convertToMap(JsonXContent.jsonXContent, (String) o, false);
                    Object nestedFieldObj = getNestedField(s, subField);
                    if (nestedFieldObj instanceof List) {
                        result.addAll((List<?>) nestedFieldObj);
                    } else {
                        result.add(dateFormatter != null ? dateFormatter.apply(nestedFieldObj) : nestedFieldObj);
                    }
                } catch (OpenSearchParseException e) {
                    if (!ignoreOnMalFormed) {
                        throw e;
                    }
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
