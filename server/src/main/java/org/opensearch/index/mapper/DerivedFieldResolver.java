/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.regex.Regex;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.script.Script;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@PublicApi(since = "2.15.0")
public class DerivedFieldResolver {
    private final QueryShardContext queryShardContext;
    private final Map<String, MappedFieldType> derivedFieldTypeMap = new ConcurrentHashMap<>();
    private final FieldTypeInference typeInference;
    private static final Logger logger = LogManager.getLogger(DerivedFieldResolver.class);

    public DerivedFieldResolver(
        QueryShardContext queryShardContext,
        Map<String, Object> derivedFieldsObject,
        List<DerivedField> derivedFields
    ) {
        this(
            queryShardContext,
            derivedFieldsObject,
            derivedFields,
            new FieldTypeInference(
                queryShardContext.index().getName(),
                queryShardContext.getMapperService(),
                queryShardContext.getIndexReader()
            )
        );
    }

    public DerivedFieldResolver(
        QueryShardContext queryShardContext,
        Map<String, Object> derivedFieldsObject,
        List<DerivedField> derivedFields,
        FieldTypeInference typeInference
    ) {
        this.queryShardContext = queryShardContext;
        initializeDerivedFieldTypes(derivedFieldsObject);
        initializeDerivedFieldTypesFromList(derivedFields);
        this.typeInference = typeInference;
    }

    private void initializeDerivedFieldTypes(Map<String, Object> derivedFieldsObject) {
        if (derivedFieldsObject != null) {
            Map<String, Object> derivedFieldObject = new HashMap<>();
            derivedFieldObject.put(DerivedFieldMapper.CONTENT_TYPE, derivedFieldsObject);
            derivedFieldTypeMap.putAll(getAllDerivedFieldTypeFromObject(derivedFieldObject));
        }
    }

    private void initializeDerivedFieldTypesFromList(List<DerivedField> derivedFields) {
        if (derivedFields != null) {
            for (DerivedField derivedField : derivedFields) {
                derivedFieldTypeMap.put(derivedField.getName(), getDerivedFieldType(derivedField));
            }
        }
    }

    public Set<String> resolvePattern(String pattern) {
        Set<String> matchingDerivedFields = new HashSet<>();
        for (String fieldName : derivedFieldTypeMap.keySet()) {
            if (!matchingDerivedFields.contains(fieldName) && Regex.simpleMatch(pattern, fieldName)) {
                matchingDerivedFields.add(fieldName);
            }
        }
        return matchingDerivedFields;
    }

    public MappedFieldType resolve(String fieldName) {
        MappedFieldType fieldType = derivedFieldTypeMap.get(fieldName);
        if (fieldType != null) {
            return fieldType;
        }

        fieldType = queryShardContext.getMapperService().fieldType(fieldName);
        if (fieldType != null) {
            return fieldType;
        }

        if (fieldName.contains(".")) {
            return resolveNestedField(fieldName);
        }
        return null;
    }

    private MappedFieldType resolveNestedField(String fieldName) {
        DerivedFieldType parentDerivedField = getParentDerivedField(fieldName);
        if (parentDerivedField == null) {
            return null;
        }
        ValueFetcher valueFetcher = getValueFetcher(fieldName, parentDerivedField.derivedField.getScript());
        Mapper inferredFieldMapper;
        try {
            inferredFieldMapper = typeInference.infer(valueFetcher);
        } catch (IOException e) {
            logger.warn(e);
            return null;
        }
        if (inferredFieldMapper == null) {
            return null;
        }
        return getDerivedFieldType(
            new DerivedField(
                fieldName,
                inferredFieldMapper.typeName(),
                parentDerivedField.derivedField.getScript(),
                parentDerivedField.derivedField.getSourceIndexedField()
            )
        );
    }

    private DerivedFieldType getParentDerivedField(String fieldName) {
        String parentFieldName = fieldName.split("\\.")[0];
        DerivedFieldType parentDerivedFieldType = (DerivedFieldType) derivedFieldTypeMap.get(parentFieldName);
        if (parentDerivedFieldType == null) {
            parentDerivedFieldType = (DerivedFieldType) queryShardContext.getMapperService().fieldType(parentFieldName);
        }
        return parentDerivedFieldType;
    }

    private ValueFetcher getValueFetcher(String fieldName, Script script) {
        String subFieldName = fieldName.substring(fieldName.indexOf(".") + 1);
        return new DerivedObjectFieldType.DerivedObjectFieldValueFetcher(
            subFieldName,
            DerivedFieldType.getDerivedFieldLeafFactory(script, queryShardContext, queryShardContext.lookup()),
            o -> o // raw object returned will be used to infer the type without modifying it
        );
    }

    private Map<String, DerivedFieldType> getAllDerivedFieldTypeFromObject(Map<String, Object> derivedFieldObject) {
        Map<String, DerivedFieldType> derivedFieldTypes = new HashMap<>();
        DocumentMapper documentMapper = queryShardContext.getMapperService()
            .documentMapperParser()
            .parse(DerivedFieldMapper.CONTENT_TYPE, derivedFieldObject);
        if (documentMapper != null && documentMapper.mappers() != null) {
            for (Mapper mapper : documentMapper.mappers()) {
                if (mapper instanceof DerivedFieldMapper) {
                    DerivedFieldType derivedFieldType = ((DerivedFieldMapper) mapper).fieldType();
                    derivedFieldTypes.put(derivedFieldType.name(), derivedFieldType);
                }
            }
        }
        return derivedFieldTypes;
    }

    private DerivedFieldType getDerivedFieldType(DerivedField derivedField) {
        Mapper.BuilderContext builderContext = new Mapper.BuilderContext(
            queryShardContext.getMapperService().getIndexSettings().getSettings(),
            new ContentPath(1)
        );
        DerivedFieldMapper.Builder builder = new DerivedFieldMapper.Builder(
            derivedField,
            queryShardContext.getMapperService().getIndexAnalyzers()
        );
        return builder.build(builderContext).fieldType();
    }
}
