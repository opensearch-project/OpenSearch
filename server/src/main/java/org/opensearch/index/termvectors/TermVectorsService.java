/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.termvectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.memory.MemoryIndex;
import org.opensearch.OpenSearchException;
import org.opensearch.action.termvectors.TermVectorsFilter;
import org.opensearch.action.termvectors.TermVectorsRequest;
import org.opensearch.action.termvectors.TermVectorsResponse;
import org.opensearch.common.Nullable;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.mapper.StringFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.shard.IndexShard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Service for Term Vectors
 *
 * @opensearch.internal
 */
public class TermVectorsService {

    private TermVectorsService() {}

    public static TermVectorsResponse getTermVectors(IndexShard indexShard, TermVectorsRequest request) {
        return getTermVectors(indexShard, request, System::nanoTime);
    }

    static TermVectorsResponse getTermVectors(IndexShard indexShard, TermVectorsRequest request, LongSupplier nanoTimeSupplier) {
        final long startTime = nanoTimeSupplier.getAsLong();
        final TermVectorsResponse termVectorsResponse = new TermVectorsResponse(indexShard.shardId().getIndex().getName(), request.id());
        final Term uidTerm = new Term(IdFieldMapper.NAME, Uid.encodeId(request.id()));

        Fields termVectorsByField = null;
        TermVectorsFilter termVectorsFilter = null;

        /* handle potential wildcards in fields */
        if (request.selectedFields() != null) {
            handleFieldWildcards(indexShard, request);
        }

        try (
            Engine.GetResult get = indexShard.get(
                new Engine.Get(request.realtime(), false, request.id(), uidTerm).version(request.version())
                    .versionType(request.versionType())
            );
            Engine.Searcher searcher = indexShard.acquireSearcher("term_vector")
        ) {
            Fields topLevelFields = fields(get.searcher() != null ? get.searcher().getIndexReader() : searcher.getIndexReader());
            DocIdAndVersion docIdAndVersion = get.docIdAndVersion();
            /* from an artificial document */
            if (request.doc() != null) {
                termVectorsByField = generateTermVectorsFromDoc(indexShard, request);
                termVectorsResponse.setArtificial(true);
                termVectorsResponse.setExists(true);
            }
            /* or from an existing document */
            else if (docIdAndVersion != null) {
                // fields with stored term vectors
                TermVectors termVectors = docIdAndVersion.reader.termVectors();
                termVectorsByField = termVectors.get(docIdAndVersion.docId);
                Set<String> selectedFields = request.selectedFields();
                // generate tvs for fields where analyzer is overridden
                if (selectedFields == null && request.perFieldAnalyzer() != null) {
                    selectedFields = getFieldsToGenerate(request.perFieldAnalyzer(), termVectorsByField);
                }
                // fields without term vectors
                if (selectedFields != null) {
                    termVectorsByField = addGeneratedTermVectors(indexShard, get, termVectorsByField, request, selectedFields);
                }
                termVectorsResponse.setDocVersion(docIdAndVersion.version);
                termVectorsResponse.setExists(true);
            }
            /* no term vectors generated or found */
            else {
                termVectorsResponse.setExists(false);
            }
            /* if there are term vectors, optional compute dfs and/or terms filtering */
            if (termVectorsByField != null) {
                if (request.filterSettings() != null) {
                    termVectorsFilter = new TermVectorsFilter(termVectorsByField, topLevelFields, request.selectedFields());
                    termVectorsFilter.setSettings(request.filterSettings());
                    try {
                        termVectorsFilter.selectBestTerms();
                    } catch (IOException e) {
                        throw new OpenSearchException("failed to select best terms", e);
                    }
                }
                // write term vectors
                termVectorsResponse.setFields(
                    termVectorsByField,
                    request.selectedFields(),
                    request.getFlags(),
                    topLevelFields,
                    termVectorsFilter
                );
            }
            termVectorsResponse.setTookInMillis(TimeUnit.NANOSECONDS.toMillis(nanoTimeSupplier.getAsLong() - startTime));
        } catch (Exception ex) {
            throw new OpenSearchException("failed to execute term vector request", ex);
        }
        return termVectorsResponse;
    }

    public static Fields fields(IndexReader reader) {
        return new Fields() {
            @Override
            public Iterator<String> iterator() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Terms terms(String field) throws IOException {
                return MultiTerms.getTerms(reader, field);
            }

            @Override
            public int size() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static void handleFieldWildcards(IndexShard indexShard, TermVectorsRequest request) {
        Set<String> fieldNames = new HashSet<>();
        for (String pattern : request.selectedFields()) {
            fieldNames.addAll(indexShard.mapperService().simpleMatchToFullName(pattern));
        }
        request.selectedFields(fieldNames.toArray(Strings.EMPTY_ARRAY));
    }

    private static boolean isValidField(MappedFieldType fieldType) {
        // must be a string
        if (fieldType == null || fieldType.unwrap() instanceof StringFieldType == false) {
            return false;
        }
        // and must be indexed
        if (fieldType.isSearchable() == false) {
            return false;
        }
        return true;
    }

    private static Fields addGeneratedTermVectors(
        IndexShard indexShard,
        Engine.GetResult get,
        Fields termVectorsByField,
        TermVectorsRequest request,
        Set<String> selectedFields
    ) throws IOException {
        /* only keep valid fields */
        Set<String> validFields = new HashSet<>();
        for (String field : selectedFields) {
            MappedFieldType fieldType = indexShard.mapperService().fieldType(field);
            if (!isValidField(fieldType)) {
                continue;
            }
            // already retrieved, only if the analyzer hasn't been overridden at the field
            if (fieldType.getTextSearchInfo().termVectors() != TextSearchInfo.TermVector.NONE
                && (request.perFieldAnalyzer() == null || !request.perFieldAnalyzer().containsKey(field))) {
                continue;
            }
            validFields.add(field);
        }

        if (validFields.isEmpty()) {
            return termVectorsByField;
        }

        /* generate term vectors from fetched document fields */
        String[] getFields = validFields.toArray(new String[validFields.size() + 1]);
        getFields[getFields.length - 1] = SourceFieldMapper.NAME;
        GetResult getResult = indexShard.getService().get(get, request.id(), getFields, null);
        Fields generatedTermVectors = generateTermVectors(
            indexShard,
            getResult.sourceAsMap(),
            getResult.getFields().values(),
            request.offsets(),
            request.perFieldAnalyzer(),
            validFields
        );

        /* merge with existing Fields */
        if (termVectorsByField == null) {
            return generatedTermVectors;
        } else {
            return mergeFields(termVectorsByField, generatedTermVectors);
        }
    }

    private static Analyzer getAnalyzerAtField(IndexShard indexShard, String field, @Nullable Map<String, String> perFieldAnalyzer) {
        MapperService mapperService = indexShard.mapperService();
        Analyzer analyzer;
        if (perFieldAnalyzer != null && perFieldAnalyzer.containsKey(field)) {
            analyzer = mapperService.getIndexAnalyzers().get(perFieldAnalyzer.get(field));
        } else {
            MappedFieldType fieldType = mapperService.fieldType(field);
            analyzer = fieldType.indexAnalyzer();
        }
        if (analyzer == null) {
            analyzer = mapperService.getIndexAnalyzers().getDefaultIndexAnalyzer();
        }
        return analyzer;
    }

    private static Set<String> getFieldsToGenerate(Map<String, String> perAnalyzerField, Fields fieldsObject) {
        Set<String> selectedFields = new HashSet<>();
        for (String fieldName : fieldsObject) {
            if (perAnalyzerField.containsKey(fieldName)) {
                selectedFields.add(fieldName);
            }
        }
        return selectedFields;
    }

    private static Fields generateTermVectors(
        IndexShard indexShard,
        Map<String, Object> source,
        Collection<DocumentField> getFields,
        boolean withOffsets,
        @Nullable Map<String, String> perFieldAnalyzer,
        Set<String> fields
    ) throws IOException {
        Map<String, Collection<Object>> values = new HashMap<>();
        for (DocumentField getField : getFields) {
            String field = getField.getName();
            if (fields.contains(field)) { // some fields are returned even when not asked for, eg. _timestamp
                values.put(field, getField.getValues());
            }
        }
        if (source != null) {
            for (String field : fields) {
                if (values.containsKey(field) == false) {
                    List<Object> v = XContentMapValues.extractRawValues(field, source);
                    if (v.isEmpty() == false) {
                        values.put(field, v);
                    }
                }
            }
        }

        /* store document in memory index */
        MemoryIndex index = new MemoryIndex(withOffsets);
        for (Map.Entry<String, Collection<Object>> entry : values.entrySet()) {
            String field = entry.getKey();
            Analyzer analyzer = getAnalyzerAtField(indexShard, field, perFieldAnalyzer);
            if (entry.getValue() instanceof List) {
                for (Object text : entry.getValue()) {
                    index.addField(field, text.toString(), analyzer);
                }
            } else {
                index.addField(field, entry.getValue().toString(), analyzer);
            }
        }
        /* and read vectors from it */
        TermVectors termVectors = index.createSearcher().getIndexReader().termVectors();
        return termVectors.get(0);
    }

    private static Fields generateTermVectorsFromDoc(IndexShard indexShard, TermVectorsRequest request) throws IOException {
        // parse the document, at the moment we do update the mapping, just like percolate
        ParsedDocument parsedDocument = parseDocument(
            indexShard,
            indexShard.shardId().getIndexName(),
            request.doc(),
            request.xContentType(),
            request.routing()
        );

        // select the right fields and generate term vectors
        ParseContext.Document doc = parsedDocument.rootDoc();
        Set<String> seenFields = new HashSet<>();
        Collection<DocumentField> documentFields = new HashSet<>();
        for (IndexableField field : doc.getFields()) {
            MappedFieldType fieldType = indexShard.mapperService().fieldType(field.name());
            if (!isValidField(fieldType)) {
                continue;
            }
            if (request.selectedFields() != null && !request.selectedFields().contains(field.name())) {
                continue;
            }
            if (seenFields.contains(field.name())) {
                continue;
            } else {
                seenFields.add(field.name());
            }
            String[] values = getValues(doc.getFields(field.name()));
            documentFields.add(new DocumentField(field.name(), Arrays.asList((Object[]) values)));
        }
        return generateTermVectors(
            indexShard,
            XContentHelper.convertToMap(parsedDocument.source(), true, request.xContentType()).v2(),
            documentFields,
            request.offsets(),
            request.perFieldAnalyzer(),
            seenFields
        );
    }

    /**
     * Returns an array of values of the field specified as the method parameter.
     * This method returns an empty array when there are no
     * matching fields.  It never returns null.
     * @param fields The <code>IndexableField</code> to get the values from
     * @return a <code>String[]</code> of field values
     */
    public static String[] getValues(IndexableField[] fields) {
        List<String> result = new ArrayList<>();
        for (IndexableField field : fields) {
            if (field.fieldType().indexOptions() != IndexOptions.NONE) {
                if (field.binaryValue() != null) {
                    result.add(field.binaryValue().utf8ToString());
                } else {
                    result.add(field.stringValue());
                }
            }
        }
        return result.toArray(new String[0]);
    }

    private static ParsedDocument parseDocument(
        IndexShard indexShard,
        String index,
        BytesReference doc,
        MediaType mediaType,
        String routing
    ) {
        MapperService mapperService = indexShard.mapperService();
        DocumentMapperForType docMapper = mapperService.documentMapperWithAutoCreate();
        ParsedDocument parsedDocument = docMapper.getDocumentMapper()
            .parse(new SourceToParse(index, "_id_for_tv_api", doc, mediaType, routing));
        if (docMapper.getMapping() != null) {
            parsedDocument.addDynamicMappingsUpdate(docMapper.getMapping());
        }
        return parsedDocument;
    }

    private static Fields mergeFields(Fields fields1, Fields fields2) throws IOException {
        ParallelFields parallelFields = new ParallelFields();
        for (String fieldName : fields2) {
            Terms terms = fields2.terms(fieldName);
            if (terms != null) {
                parallelFields.addField(fieldName, terms);
            }
        }
        for (String fieldName : fields1) {
            if (parallelFields.fields.containsKey(fieldName)) {
                continue;
            }
            Terms terms = fields1.terms(fieldName);
            if (terms != null) {
                parallelFields.addField(fieldName, terms);
            }
        }
        return parallelFields;
    }

    /**
     * Poached from Lucene ParallelLeafReader
     *
     * @opensearch.internal
     */
    private static final class ParallelFields extends Fields {
        final Map<String, Terms> fields = new TreeMap<>();

        ParallelFields() {}

        void addField(String fieldName, Terms terms) {
            fields.put(fieldName, terms);
        }

        @Override
        public Iterator<String> iterator() {
            return Collections.unmodifiableSet(fields.keySet()).iterator();
        }

        @Override
        public Terms terms(String field) {
            return fields.get(field);
        }

        @Override
        public int size() {
            return fields.size();
        }
    }
}
