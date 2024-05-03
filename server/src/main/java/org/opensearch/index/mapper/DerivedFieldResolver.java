/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.script.Script;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

@PublicApi(since = "2.14.0")
public class DerivedFieldResolver {
    private final QueryShardContext queryShardContext;
    private final Map<String, MappedFieldType> derivedFieldTypeMap = new ConcurrentHashMap<>();
    private final FieldTypeInference typeInference;

    public DerivedFieldResolver(
        QueryShardContext queryShardContext,
        Map<String, Object> derivedFieldsObject,
        List<DerivedField> derivedFields
    ) {
        this.queryShardContext = queryShardContext;
        if (derivedFieldsObject != null) {
            Map<String, Object> derivedFieldObject = new HashMap<>();
            derivedFieldObject.put(DerivedFieldMapper.CONTENT_TYPE, derivedFieldsObject);
            derivedFieldTypeMap.putAll(getAllDerivedFieldTypeFromObject(derivedFieldObject));
        }
        if (derivedFields != null) {
            for (DerivedField derivedField : derivedFields) {
                derivedFieldTypeMap.put(derivedField.getName(), getDerivedFieldType(derivedField));
            }
        }
        this.typeInference = new FieldTypeInference(queryShardContext);
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
        if (derivedFieldTypeMap.containsKey(fieldName)) {
            return derivedFieldTypeMap.get(fieldName);
        }
        MappedFieldType derivedFieldType = queryShardContext.getMapperService().fieldType(fieldName);
        if (derivedFieldType != null) {
            return derivedFieldType;
        }
        if (fieldName.contains(".")) {
            DerivedFieldType parentDerivedField = getParentDerivedField(fieldName);
            if (parentDerivedField == null) {
                return null;
            }
            String subFieldName = fieldName.substring(fieldName.indexOf(".") + 1);
            Mapper inferredFieldMapper = typeInference.infer(subFieldName, parentDerivedField.derivedField.getScript());
            Mapper.BuilderContext builderContext = new Mapper.BuilderContext(
                this.queryShardContext.getMapperService().getIndexSettings().getSettings(),
                new ContentPath(1)
            );
            derivedFieldType = new DerivedObjectFieldType(
                new DerivedField(
                    fieldName,
                    inferredFieldMapper.typeName(),
                    parentDerivedField.derivedField.getScript(),
                    parentDerivedField.derivedField.getSourceIndexedField()
                ),
                DerivedFieldSupportedTypes.getFieldMapperFromType(
                    inferredFieldMapper.typeName(),
                    fieldName,
                    builderContext,
                    queryShardContext.getIndexAnalyzers()
                ),
                DerivedFieldSupportedTypes.getIndexableFieldGeneratorType(inferredFieldMapper.typeName(), fieldName),
                queryShardContext.getIndexAnalyzers()
            );
            if (derivedFieldType != null) {
                derivedFieldTypeMap.put(fieldName, derivedFieldType);
            }
            return derivedFieldType;
        }
        return null;
    }

    private DerivedFieldType getParentDerivedField(String fieldName) {
        String parentFieldName = fieldName.split("\\.")[0];
        DerivedFieldType parentDerivedFieldType = (DerivedFieldType) derivedFieldTypeMap.get(parentFieldName);
        if (parentDerivedFieldType == null) {
            parentDerivedFieldType = (DerivedFieldType) this.queryShardContext.getMapperService().fieldType(parentFieldName);
        }
        return parentDerivedFieldType;
    }

    private Map<String, DerivedFieldType> getAllDerivedFieldTypeFromObject(Map<String, Object> derivedFieldObject) {
        Map<String, DerivedFieldType> derivedFieldTypes = new HashMap<>();
        DocumentMapper documentMapper = this.queryShardContext.getMapperService()
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
            this.queryShardContext.getMapperService().getIndexSettings().getSettings(),
            new ContentPath(1)
        );
        DerivedFieldMapper.Builder builder = new DerivedFieldMapper.Builder(
            derivedField,
            this.queryShardContext.getMapperService().getIndexAnalyzers()
        );
        return builder.build(builderContext).fieldType();
    }

    static class FieldTypeInference {
        private final QueryShardContext queryShardContext;

        public FieldTypeInference(QueryShardContext queryShardContext) {
            this.queryShardContext = queryShardContext;
        }

        public Mapper infer(String name, Script script) {
            try {
                DerivedObjectFieldType.DerivedObjectFieldValueFetcher valueFetcher =
                    new DerivedObjectFieldType.DerivedObjectFieldValueFetcher(
                        name,
                        DerivedFieldType.getDerivedFieldLeafFactory(script, queryShardContext, queryShardContext.lookup()),
                        o -> o // raw object returned will be used to infer the type without modifying it
                    );
                int iter = 0;
                int totalDocs = queryShardContext.getIndexReader().numDocs();
                // this will lead to the probability of more than 0.95 to select on the document containing this field,
                // when at least 5% of the overall documents contain the field
                int limit = Math.min(totalDocs, 50);
                int[] docs = getSortedRandomNum(limit, totalDocs, 10000);
                int offset = 0;
                int leaf = 0;
                LeafReaderContext leafReaderContext = queryShardContext.getIndexReader().leaves().get(leaf);
                valueFetcher.setNextReader(leafReaderContext);
                SourceLookup sourceLookup = new SourceLookup();
                while (iter < limit) {
                    int docID = docs[iter] - offset;
                    if (docID >= leafReaderContext.reader().numDocs()) {
                        leaf++;
                        offset += leafReaderContext.reader().numDocs();
                        docID = docs[iter] - offset;
                        leafReaderContext = queryShardContext.getIndexReader().leaves().get(leaf);
                        valueFetcher.setNextReader(leafReaderContext);
                    }
                    sourceLookup.setSegmentAndDocument(leafReaderContext, docID);
                    List<Object> objects = valueFetcher.fetchValues(sourceLookup);
                    Mapper inferredMapper = inferTypeFromObject(name, objects.get(0));
                    if (inferredMapper == null) {
                        iter++;
                        continue;
                    }
                    return inferredMapper;
                }
            } catch (IllegalArgumentException e) {
                // TODO remove illegal argument exception from DerivedFieldSupportedTypes and let consumers handle themselves.
                // If inference didn't work, defaulting to keyword field type
                // TODO: warning?
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            // the field isn't found in documents where it was checked
            // TODO: should fallback to keyword field type>
            throw new MapperException(
                "Unable to infer the derived field ["
                    + name
                    + "] within object type. "
                    + "Ensure the field is present in majority of the documents"
            );
        }

        public static int[] getSortedRandomNum(int k, int n, int attempts) {

            Set<Integer> generatedNumbers = new HashSet<>();
            Random random = new Random();
            int itr = 0;
            while (generatedNumbers.size() < k && itr++ < attempts) {
                int randomNumber = random.nextInt(n);
                generatedNumbers.add(randomNumber);
            }
            int[] result = new int[k];
            int i = 0;
            for (int number : generatedNumbers) {
                result[i++] = number;
            }
            Arrays.sort(result);
            return result;
        }

        private Mapper inferTypeFromObject(String name, Object o) throws IOException {
            // TODO error handling - 1. missing value? 2. Multi-valued field?
            if (o == null) {
                return null;
            }
            DocumentMapper mapper = queryShardContext.getMapperService().documentMapper();
            SourceToParse sourceToParse = new SourceToParse(
                queryShardContext.index().getName(),
                "_id",
                BytesReference.bytes(jsonBuilder().startObject().field(name, o).endObject()),
                JsonXContent.jsonXContent.mediaType()
            );
            ParsedDocument parsedDocument = mapper.parse(sourceToParse);
            Mapping mapping = parsedDocument.dynamicMappingsUpdate();
            return mapping.root.getMapper(name);
        }
    }
}
