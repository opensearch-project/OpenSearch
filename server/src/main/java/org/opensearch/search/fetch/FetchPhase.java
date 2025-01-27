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

package org.opensearch.search.fetch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BitSet;
import org.opensearch.common.CheckedBiConsumer;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.index.fieldvisitor.CustomFieldsVisitor;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.search.SearchContextSourcePrinter;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.FetchSubPhase.HitContext;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.InnerHitsContext;
import org.opensearch.search.fetch.subphase.InnerHitsPhase;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptyMap;

/**
 * Fetch phase of a search request, used to fetch the actual top matching documents to be returned to the client, identified
 * after reducing all the matches returned by the query phase
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class FetchPhase {
    private static final Logger LOGGER = LogManager.getLogger(FetchPhase.class);

    private final FetchSubPhase[] fetchSubPhases;

    public FetchPhase(List<FetchSubPhase> fetchSubPhases) {
        this.fetchSubPhases = fetchSubPhases.toArray(new FetchSubPhase[fetchSubPhases.size() + 1]);
        this.fetchSubPhases[fetchSubPhases.size()] = new InnerHitsPhase(this);
    }

    public void execute(SearchContext context) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(context));
        }

        if (context.isCancelled()) {
            throw new TaskCancelledException("cancelled task with reason: " + context.getTask().getReasonCancelled());
        }

        if (context.docIdsToLoadSize() == 0) {
            // no individual hits to process, so we shortcut
            context.fetchResult()
                .hits(new SearchHits(new SearchHit[0], context.queryResult().getTotalHits(), context.queryResult().getMaxScore()));
            return;
        }

        DocIdToIndex[] docs = new DocIdToIndex[context.docIdsToLoadSize()];
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            docs[index] = new DocIdToIndex(context.docIdsToLoad()[context.docIdsToLoadFrom() + index], index);
        }
        // make sure that we iterate in doc id order
        Arrays.sort(docs);

        Map<String, Set<String>> storedToRequestedFields = new HashMap<>();
        FieldsVisitor fieldsVisitor = createStoredFieldsVisitor(context, storedToRequestedFields);

        FetchContext fetchContext = new FetchContext(context);

        SearchHit[] hits = new SearchHit[context.docIdsToLoadSize()];

        List<FetchSubPhaseProcessor> processors = getProcessors(context.shardTarget(), fetchContext);

        int currentReaderIndex = -1;
        LeafReaderContext currentReaderContext = null;
        CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader = null;
        boolean hasSequentialDocs = hasSequentialDocs(docs);
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            if (context.isCancelled()) {
                throw new TaskCancelledException("cancelled task with reason: " + context.getTask().getReasonCancelled());
            }
            int docId = docs[index].docId;
            try {
                int readerIndex = ReaderUtil.subIndex(docId, context.searcher().getIndexReader().leaves());
                if (currentReaderIndex != readerIndex) {
                    currentReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
                    currentReaderIndex = readerIndex;
                    if (currentReaderContext.reader() instanceof SequentialStoredFieldsLeafReader
                        && hasSequentialDocs
                        && docs.length >= 10) {
                        // All the docs to fetch are adjacent but Lucene stored fields are optimized
                        // for random access and don't optimize for sequential access - except for merging.
                        // So we do a little hack here and pretend we're going to do merges in order to
                        // get better sequential access.
                        SequentialStoredFieldsLeafReader lf = (SequentialStoredFieldsLeafReader) currentReaderContext.reader();
                        fieldReader = lf.getSequentialStoredFieldsReader()::document;
                    } else {
                        fieldReader = currentReaderContext.reader().storedFields()::document;
                    }
                    for (FetchSubPhaseProcessor processor : processors) {
                        processor.setNextReader(currentReaderContext);
                    }
                }
                assert currentReaderContext != null;
                HitContext hit = prepareHitContext(
                    context,
                    fetchContext.searchLookup(),
                    fieldsVisitor,
                    docId,
                    storedToRequestedFields,
                    currentReaderContext,
                    fieldReader
                );
                for (FetchSubPhaseProcessor processor : processors) {
                    processor.process(hit);
                }
                hits[docs[index].index] = hit.hit();
            } catch (Exception e) {
                throw new FetchPhaseExecutionException(context.shardTarget(), "Error running fetch phase for doc [" + docId + "]", e);
            }
        }
        if (context.isCancelled()) {
            throw new TaskCancelledException("cancelled task with reason: " + context.getTask().getReasonCancelled());
        }

        TotalHits totalHits = context.queryResult().getTotalHits();
        context.fetchResult().hits(new SearchHits(hits, totalHits, context.queryResult().getMaxScore()));

    }

    List<FetchSubPhaseProcessor> getProcessors(SearchShardTarget target, FetchContext context) {
        try {
            List<FetchSubPhaseProcessor> processors = new ArrayList<>();
            for (FetchSubPhase fsp : fetchSubPhases) {
                FetchSubPhaseProcessor processor = fsp.getProcessor(context);
                if (processor != null) {
                    processors.add(processor);
                }
            }
            return processors;
        } catch (Exception e) {
            throw new FetchPhaseExecutionException(target, "Error building fetch sub-phases", e);
        }
    }

    static class DocIdToIndex implements Comparable<DocIdToIndex> {
        final int docId;
        final int index;

        DocIdToIndex(int docId, int index) {
            this.docId = docId;
            this.index = index;
        }

        @Override
        public int compareTo(DocIdToIndex o) {
            return Integer.compare(docId, o.docId);
        }
    }

    protected FieldsVisitor createStoredFieldsVisitor(SearchContext context, Map<String, Set<String>> storedToRequestedFields) {
        StoredFieldsContext storedFieldsContext = context.storedFieldsContext();

        if (storedFieldsContext == null) {
            // no fields specified, default to return source if no explicit indication
            if (!context.hasScriptFields() && !context.hasFetchSourceContext()) {
                context.fetchSourceContext(new FetchSourceContext(true));
            }
            boolean loadSource = sourceRequired(context);
            return new FieldsVisitor(
                loadSource,
                context.hasFetchSourceContext() ? context.fetchSourceContext().includes() : null,
                context.hasFetchSourceContext() ? context.fetchSourceContext().excludes() : null
            );
        } else if (storedFieldsContext.fetchFields() == false) {
            // disable stored fields entirely
            return null;
        } else {
            for (String fieldNameOrPattern : context.storedFieldsContext().fieldNames()) {
                if (fieldNameOrPattern.equals(SourceFieldMapper.NAME)) {
                    FetchSourceContext fetchSourceContext = context.hasFetchSourceContext()
                        ? context.fetchSourceContext()
                        : FetchSourceContext.FETCH_SOURCE;
                    context.fetchSourceContext(new FetchSourceContext(true, fetchSourceContext.includes(), fetchSourceContext.excludes()));
                    continue;
                }

                Collection<String> fieldNames = context.mapperService().simpleMatchToFullName(fieldNameOrPattern);
                for (String fieldName : fieldNames) {
                    MappedFieldType fieldType = context.fieldType(fieldName);
                    if (fieldType == null) {
                        // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                        if (context.getObjectMapper(fieldName) != null) {
                            throw new IllegalArgumentException("field [" + fieldName + "] isn't a leaf field");
                        }
                    } else {
                        String storedField = fieldType.name();
                        Set<String> requestedFields = storedToRequestedFields.computeIfAbsent(storedField, key -> new HashSet<>());
                        requestedFields.add(fieldName);
                    }
                }
            }
            boolean loadSource = sourceRequired(context);
            if (storedToRequestedFields.isEmpty()) {
                // empty list specified, default to disable _source if no explicit indication
                return new FieldsVisitor(
                    loadSource,
                    context.hasFetchSourceContext() ? context.fetchSourceContext().includes() : null,
                    context.hasFetchSourceContext() ? context.fetchSourceContext().excludes() : null
                );
            } else {
                return new CustomFieldsVisitor(
                    storedToRequestedFields.keySet(),
                    loadSource,
                    context.hasFetchSourceContext() ? context.fetchSourceContext().includes() : null,
                    context.hasFetchSourceContext() ? context.fetchSourceContext().excludes() : null
                );
            }
        }
    }

    private boolean sourceRequired(SearchContext context) {
        return context.sourceRequested() || context.fetchFieldsContext() != null;
    }

    private int findRootDocumentIfNested(SearchContext context, LeafReaderContext subReaderContext, int subDocId) throws IOException {
        if (context.mapperService().hasNested()) {
            BitSet bits = context.bitsetFilterCache().getBitSetProducer(Queries.newNonNestedFilter()).getBitSet(subReaderContext);
            if (!bits.get(subDocId)) {
                return bits.nextSetBit(subDocId);
            }
        }
        return -1;
    }

    private HitContext prepareHitContext(
        SearchContext context,
        SearchLookup lookup,
        FieldsVisitor fieldsVisitor,
        int docId,
        Map<String, Set<String>> storedToRequestedFields,
        LeafReaderContext subReaderContext,
        CheckedBiConsumer<Integer, FieldsVisitor, IOException> storedFieldReader
    ) throws IOException {
        int rootDocId = findRootDocumentIfNested(context, subReaderContext, docId - subReaderContext.docBase);
        if (rootDocId == -1) {
            return prepareNonNestedHitContext(
                context,
                lookup,
                fieldsVisitor,
                docId,
                storedToRequestedFields,
                subReaderContext,
                storedFieldReader
            );
        } else {
            return prepareNestedHitContext(context, docId, rootDocId, storedToRequestedFields, subReaderContext, storedFieldReader);
        }
    }

    /**
     * Resets the provided {@link HitContext} with information on the current
     * document. This includes the following:
     *   - Adding an initial {@link SearchHit} instance.
     *   - Loading the document source and setting it on {@link SourceLookup}. This allows
     *     fetch subphases that use the hit context to access the preloaded source.
     */
    private HitContext prepareNonNestedHitContext(
        SearchContext context,
        SearchLookup lookup,
        FieldsVisitor fieldsVisitor,
        int docId,
        Map<String, Set<String>> storedToRequestedFields,
        LeafReaderContext subReaderContext,
        CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader
    ) throws IOException {
        int subDocId = docId - subReaderContext.docBase;
        DocumentMapper documentMapper = context.mapperService().documentMapper();
        Text typeText = documentMapper.typeText();

        if (fieldsVisitor == null) {
            SearchHit hit = new SearchHit(docId, null, null, null);
            return new HitContext(hit, subReaderContext, subDocId, lookup.source());
        } else {
            SearchHit hit;
            loadStoredFields(context::fieldType, fieldReader, fieldsVisitor, subDocId);
            String id = fieldsVisitor.id();
            if (fieldsVisitor.fields().isEmpty() == false) {
                Map<String, DocumentField> docFields = new HashMap<>();
                Map<String, DocumentField> metaFields = new HashMap<>();
                fillDocAndMetaFields(context, fieldsVisitor, storedToRequestedFields, docFields, metaFields);
                hit = new SearchHit(docId, id, docFields, metaFields);
            } else {
                hit = new SearchHit(docId, id, emptyMap(), emptyMap());
            }

            HitContext hitContext = new HitContext(hit, subReaderContext, subDocId, lookup.source());
            if (fieldsVisitor.source() != null) {
                hitContext.sourceLookup().setSource(fieldsVisitor.source());
            }
            return hitContext;
        }
    }

    /**
     * Resets the provided {@link HitContext} with information on the current
     * nested document. This includes the following:
     *   - Adding an initial {@link SearchHit} instance.
     *   - Loading the document source, filtering it based on the nested document ID, then
     *     setting it on {@link SourceLookup}. This allows fetch subphases that use the hit
     *     context to access the preloaded source.
     */
    @SuppressWarnings("unchecked")
    private HitContext prepareNestedHitContext(
        SearchContext context,
        int nestedTopDocId,
        int rootDocId,
        Map<String, Set<String>> storedToRequestedFields,
        LeafReaderContext subReaderContext,
        CheckedBiConsumer<Integer, FieldsVisitor, IOException> storedFieldReader
    ) throws IOException {
        // Also if highlighting is requested on nested documents we need to fetch the _source from the root document,
        // otherwise highlighting will attempt to fetch the _source from the nested doc, which will fail,
        // because the entire _source is only stored with the root document.
        boolean needSource = sourceRequired(context) || context.highlight() != null;

        String rootId;
        Map<String, Object> rootSourceAsMap = null;
        MediaType rootSourceContentType = null;

        int nestedDocId = nestedTopDocId - subReaderContext.docBase;

        if (context instanceof InnerHitsContext.InnerHitSubContext) {
            InnerHitsContext.InnerHitSubContext innerHitsContext = (InnerHitsContext.InnerHitSubContext) context;
            rootId = innerHitsContext.getId();

            if (needSource) {
                SourceLookup rootLookup = innerHitsContext.getRootLookup();
                rootSourceAsMap = rootLookup.loadSourceIfNeeded();
                rootSourceContentType = rootLookup.sourceContentType();
            }
        } else {
            FieldsVisitor rootFieldsVisitor = new FieldsVisitor(needSource);
            loadStoredFields(context::fieldType, storedFieldReader, rootFieldsVisitor, rootDocId);
            rootFieldsVisitor.postProcess(context::fieldType);
            rootId = rootFieldsVisitor.id();

            if (needSource) {
                if (rootFieldsVisitor.source() != null) {
                    Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(rootFieldsVisitor.source(), false);
                    rootSourceAsMap = tuple.v2();
                    rootSourceContentType = tuple.v1();
                } else {
                    rootSourceAsMap = Collections.emptyMap();
                }
            }
        }

        Map<String, DocumentField> docFields = emptyMap();
        Map<String, DocumentField> metaFields = emptyMap();
        if (context.hasStoredFields() && !context.storedFieldsContext().fieldNames().isEmpty()) {
            FieldsVisitor nestedFieldsVisitor = new CustomFieldsVisitor(storedToRequestedFields.keySet(), false);
            loadStoredFields(context::fieldType, storedFieldReader, nestedFieldsVisitor, nestedDocId);
            if (nestedFieldsVisitor.fields().isEmpty() == false) {
                docFields = new HashMap<>();
                metaFields = new HashMap<>();
                fillDocAndMetaFields(context, nestedFieldsVisitor, storedToRequestedFields, docFields, metaFields);
            }
        }

        DocumentMapper documentMapper = context.mapperService().documentMapper();

        ObjectMapper nestedObjectMapper = documentMapper.findNestedObjectMapper(nestedDocId, context, subReaderContext);
        assert nestedObjectMapper != null;
        SearchHit.NestedIdentity nestedIdentity = getInternalNestedIdentity(
            context,
            nestedDocId,
            subReaderContext,
            context.mapperService(),
            nestedObjectMapper
        );

        SearchHit hit = new SearchHit(nestedTopDocId, rootId, nestedIdentity, docFields, metaFields);
        HitContext hitContext = new HitContext(hit, subReaderContext, nestedDocId, new SourceLookup());  // Use a clean, fresh SourceLookup
                                                                                                         // for the nested context

        if (rootSourceAsMap != null && rootSourceAsMap.isEmpty() == false) {
            // Isolate the nested json array object that matches with nested hit and wrap it back into the same json
            // structure with the nested json array object being the actual content. The latter is important, so that
            // features like source filtering and highlighting work consistent regardless of whether the field points
            // to a json object array for consistency reasons on how we refer to fields
            Map<String, Object> nestedSourceAsMap = new HashMap<>();
            Map<String, Object> current = nestedSourceAsMap;
            for (SearchHit.NestedIdentity nested = nestedIdentity; nested != null; nested = nested.getChild()) {
                String nestedPath = nested.getField().string();
                current.put(nestedPath, new HashMap<>());
                Object extractedValue = XContentMapValues.extractValue(nestedPath, rootSourceAsMap);
                List<?> nestedParsedSource;
                if (extractedValue instanceof List) {
                    // nested field has an array value in the _source
                    nestedParsedSource = (List<?>) extractedValue;
                } else if (extractedValue instanceof Map) {
                    // nested field has an object value in the _source. This just means the nested field has just one inner object,
                    // which is valid, but uncommon.
                    nestedParsedSource = Collections.singletonList(extractedValue);
                } else {
                    throw new IllegalStateException("extracted source isn't an object or an array");
                }
                if ((nestedParsedSource.get(0) instanceof Map) == false
                    && nestedObjectMapper.parentObjectMapperAreNested(context.mapperService()) == false) {
                    // When one of the parent objects are not nested then XContentMapValues.extractValue(...) extracts the values
                    // from two or more layers resulting in a list of list being returned. This is because nestedPath
                    // encapsulates two or more object layers in the _source.
                    //
                    // This is why only the first element of nestedParsedSource needs to be checked.
                    throw new IllegalArgumentException(
                        "Cannot execute inner hits. One or more parent object fields of nested field ["
                            + nestedObjectMapper.name()
                            + "] are not nested. All parent fields need to be nested fields too"
                    );
                }
                rootSourceAsMap = (Map<String, Object>) nestedParsedSource.get(nested.getOffset());
                if (nested.getChild() == null) {
                    current.put(nestedPath, rootSourceAsMap);
                } else {
                    Map<String, Object> next = new HashMap<>();
                    current.put(nestedPath, next);
                    current = next;
                }
            }

            hitContext.sourceLookup().setSource(nestedSourceAsMap);
            hitContext.sourceLookup().setSourceContentType(rootSourceContentType);
        }
        return hitContext;
    }

    private SearchHit.NestedIdentity getInternalNestedIdentity(
        SearchContext context,
        int nestedSubDocId,
        LeafReaderContext subReaderContext,
        MapperService mapperService,
        ObjectMapper nestedObjectMapper
    ) throws IOException {
        int currentParent = nestedSubDocId;
        ObjectMapper nestedParentObjectMapper;
        ObjectMapper current = nestedObjectMapper;
        String originalName = nestedObjectMapper.name();
        SearchHit.NestedIdentity nestedIdentity = null;
        do {
            Query parentFilter;
            nestedParentObjectMapper = current.getParentObjectMapper(mapperService);
            if (nestedParentObjectMapper != null) {
                if (nestedParentObjectMapper.nested().isNested() == false) {
                    current = nestedParentObjectMapper;
                    continue;
                }
                parentFilter = nestedParentObjectMapper.nestedTypeFilter();
            } else {
                parentFilter = Queries.newNonNestedFilter();
            }

            Query childFilter = nestedObjectMapper.nestedTypeFilter();
            if (childFilter == null) {
                current = nestedParentObjectMapper;
                continue;
            }
            BitSet childIter = context.bitsetFilterCache()
                .getBitSetProducer(context.searcher().rewrite(childFilter))
                .getBitSet(subReaderContext);
            if (childIter == null) {
                current = nestedParentObjectMapper;
                continue;
            }

            BitSet parentBits = context.bitsetFilterCache().getBitSetProducer(parentFilter).getBitSet(subReaderContext);

            int offset = 0;

            /*
             * Starts from the previous parent and finds the offset of the
             * <code>nestedSubDocID</code> within the nested children. Nested documents
             * are indexed in the same order than in the source array so the offset
             * of the nested child is the number of nested document with the same parent
             * that appear before him.
             */
            int previousParent = parentBits.prevSetBit(currentParent);
            for (int docId = childIter.nextSetBit(previousParent + 1); docId < nestedSubDocId
                && docId != DocIdSetIterator.NO_MORE_DOCS; docId = childIter.nextSetBit(docId + 1)) {
                offset++;
            }
            currentParent = nestedSubDocId;
            current = nestedObjectMapper = nestedParentObjectMapper;
            int currentPrefix = current == null ? 0 : current.name().length() + 1;
            nestedIdentity = new SearchHit.NestedIdentity(originalName.substring(currentPrefix), offset, nestedIdentity);
            if (current != null) {
                originalName = current.name();
            }
        } while (current != null);
        return nestedIdentity;
    }

    private void loadStoredFields(
        Function<String, MappedFieldType> fieldTypeLookup,
        CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader,
        FieldsVisitor fieldVisitor,
        int docId
    ) throws IOException {
        fieldVisitor.reset();
        fieldReader.accept(docId, fieldVisitor);
        fieldVisitor.postProcess(fieldTypeLookup);
    }

    private static void fillDocAndMetaFields(
        SearchContext context,
        FieldsVisitor fieldsVisitor,
        Map<String, Set<String>> storedToRequestedFields,
        Map<String, DocumentField> docFields,
        Map<String, DocumentField> metaFields
    ) {
        for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
            String storedField = entry.getKey();
            List<Object> storedValues = entry.getValue();
            if (storedToRequestedFields.containsKey(storedField)) {
                for (String requestedField : storedToRequestedFields.get(storedField)) {
                    if (context.mapperService().isMetadataField(requestedField)) {
                        metaFields.put(requestedField, new DocumentField(requestedField, storedValues));
                    } else {
                        docFields.put(requestedField, new DocumentField(requestedField, storedValues));
                    }
                }
            } else {
                if (context.mapperService().isMetadataField(storedField)) {
                    metaFields.put(storedField, new DocumentField(storedField, storedValues));
                } else {
                    docFields.put(storedField, new DocumentField(storedField, storedValues));
                }
            }
        }
    }

    /**
     * Returns <code>true</code> if the provided <code>docs</code> are
     * stored sequentially (Dn = Dn-1 + 1).
     */
    static boolean hasSequentialDocs(DocIdToIndex[] docs) {
        return docs.length > 0 && docs[docs.length - 1].docId - docs[0].docId == docs.length - 1;
    }
}
