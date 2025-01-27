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

package org.opensearch.index.get;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.OpenSearchException;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;
import org.opensearch.common.util.set.Sets;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.TranslogLeafReader;
import org.opensearch.index.fieldvisitor.CustomFieldsVisitor;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.shard.AbstractIndexShardComponent;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Gets an index shard
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ShardGetService extends AbstractIndexShardComponent {
    private final MapperService mapperService;
    private final MeanMetric existsMetric = new MeanMetric();
    private final MeanMetric missingMetric = new MeanMetric();
    private final CounterMetric currentMetric = new CounterMetric();
    private final IndexShard indexShard;

    public ShardGetService(IndexSettings indexSettings, IndexShard indexShard, MapperService mapperService) {
        super(indexShard.shardId(), indexSettings);
        this.mapperService = mapperService;
        this.indexShard = indexShard;
    }

    public GetStats stats() {
        return new GetStats(
            existsMetric.count(),
            TimeUnit.NANOSECONDS.toMillis(existsMetric.sum()),
            missingMetric.count(),
            TimeUnit.NANOSECONDS.toMillis(missingMetric.sum()),
            currentMetric.count()
        );
    }

    public GetResult get(
        String id,
        String[] gFields,
        boolean realtime,
        long version,
        VersionType versionType,
        FetchSourceContext fetchSourceContext
    ) {
        return get(id, gFields, realtime, version, versionType, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, fetchSourceContext);
    }

    private GetResult get(
        String id,
        String[] gFields,
        boolean realtime,
        long version,
        VersionType versionType,
        long ifSeqNo,
        long ifPrimaryTerm,
        FetchSourceContext fetchSourceContext
    ) {
        currentMetric.inc();
        try {
            long now = System.nanoTime();
            GetResult getResult = innerGet(id, gFields, realtime, version, versionType, ifSeqNo, ifPrimaryTerm, fetchSourceContext);

            if (getResult.isExists()) {
                existsMetric.inc(System.nanoTime() - now);
            } else {
                missingMetric.inc(System.nanoTime() - now);
            }
            return getResult;
        } finally {
            currentMetric.dec();
        }
    }

    public GetResult getForUpdate(String id, long ifSeqNo, long ifPrimaryTerm) {
        return get(
            id,
            new String[] { RoutingFieldMapper.NAME },
            true,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            ifSeqNo,
            ifPrimaryTerm,
            FetchSourceContext.FETCH_SOURCE
        );
    }

    /**
     * Returns {@link GetResult} based on the specified {@link org.opensearch.index.engine.Engine.GetResult} argument.
     * This method basically loads specified fields for the associated document in the engineGetResult.
     * This method load the fields from the Lucene index and not from transaction log and therefore isn't realtime.
     * <p>
     * Note: Call <b>must</b> release engine searcher associated with engineGetResult!
     */
    public GetResult get(Engine.GetResult engineGetResult, String id, String[] fields, FetchSourceContext fetchSourceContext) {
        if (!engineGetResult.exists()) {
            return new GetResult(shardId.getIndexName(), id, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, -1, false, null, null, null);
        }

        currentMetric.inc();
        try {
            long now = System.nanoTime();
            fetchSourceContext = normalizeFetchSourceContent(fetchSourceContext, fields);
            GetResult getResult = innerGetLoadFromStoredFields(id, fields, fetchSourceContext, engineGetResult, mapperService);
            if (getResult.isExists()) {
                existsMetric.inc(System.nanoTime() - now);
            } else {
                missingMetric.inc(System.nanoTime() - now); // This shouldn't happen...
            }
            return getResult;
        } finally {
            currentMetric.dec();
        }
    }

    /**
     * decides what needs to be done based on the request input and always returns a valid non-null FetchSourceContext
     */
    private FetchSourceContext normalizeFetchSourceContent(@Nullable FetchSourceContext context, @Nullable String[] gFields) {
        if (context != null) {
            return context;
        }
        if (gFields == null) {
            return FetchSourceContext.FETCH_SOURCE;
        }
        for (String field : gFields) {
            if (SourceFieldMapper.NAME.equals(field)) {
                return FetchSourceContext.FETCH_SOURCE;
            }
        }
        return FetchSourceContext.DO_NOT_FETCH_SOURCE;
    }

    private GetResult innerGet(
        String id,
        String[] gFields,
        boolean realtime,
        long version,
        VersionType versionType,
        long ifSeqNo,
        long ifPrimaryTerm,
        FetchSourceContext fetchSourceContext
    ) {
        fetchSourceContext = normalizeFetchSourceContent(fetchSourceContext, gFields);

        Term uidTerm = new Term(IdFieldMapper.NAME, Uid.encodeId(id));

        try (
            Engine.GetResult get = indexShard.get(
                new Engine.Get(realtime, true, id, uidTerm).version(version)
                    .versionType(versionType)
                    .setIfSeqNo(ifSeqNo)
                    .setIfPrimaryTerm(ifPrimaryTerm)
            )
        ) {
            if (get == null || get.exists() == false) {
                return new GetResult(shardId.getIndexName(), id, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, -1, false, null, null, null);
            }
            // break between having loaded it from translog (so we only have _source), and having a document to load
            return innerGetLoadFromStoredFields(id, gFields, fetchSourceContext, get, mapperService);
        }
    }

    private GetResult innerGetLoadFromStoredFields(
        String id,
        String[] storedFields,
        FetchSourceContext fetchSourceContext,
        Engine.GetResult get,
        MapperService mapperService
    ) {
        assert get.exists() : "method should only be called if document could be retrieved";

        // check first if stored fields to be loaded don't contain an object field
        DocumentMapper docMapper = mapperService.documentMapper();
        if (storedFields != null) {
            for (String field : storedFields) {
                Mapper fieldMapper = docMapper.mappers().getMapper(field);
                if (fieldMapper == null) {
                    if (docMapper.objectMappers().get(field) != null) {
                        // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                        throw new IllegalArgumentException("field [" + field + "] isn't a leaf field");
                    }
                }
            }
        }

        Map<String, DocumentField> documentFields = null;
        Map<String, DocumentField> metadataFields = null;
        BytesReference source = null;
        DocIdAndVersion docIdAndVersion = get.docIdAndVersion();
        // force fetching source if we read from translog and need to recreate stored fields
        boolean forceSourceForComputingTranslogStoredFields = get.isFromTranslog()
            && storedFields != null
            && Stream.of(storedFields).anyMatch(f -> TranslogLeafReader.ALL_FIELD_NAMES.contains(f) == false);
        FieldsVisitor fieldVisitor = buildFieldsVisitors(
            storedFields,
            forceSourceForComputingTranslogStoredFields ? FetchSourceContext.FETCH_SOURCE : fetchSourceContext
        );
        if (fieldVisitor != null) {
            try {
                docIdAndVersion.reader.storedFields().document(docIdAndVersion.docId, fieldVisitor);
            } catch (IOException e) {
                throw new OpenSearchException("Failed to get id [" + id + "]", e);
            }
            source = fieldVisitor.source();

            // in case we read from translog, some extra steps are needed to make _source consistent and to load stored fields
            if (get.isFromTranslog()) {
                // Fast path: if only asked for the source or stored fields that have been already provided by TranslogLeafReader,
                // just make source consistent by reapplying source filters from mapping (possibly also nulling the source)
                if (forceSourceForComputingTranslogStoredFields == false) {
                    try {
                        source = indexShard.mapperService().documentMapper().sourceMapper().applyFilters(source, null);
                    } catch (IOException e) {
                        throw new OpenSearchException("Failed to reapply filters for [" + id + "] after reading from translog", e);
                    }
                } else {
                    // Slow path: recreate stored fields from original source
                    assert source != null : "original source in translog must exist";
                    SourceToParse sourceToParse = new SourceToParse(
                        shardId.getIndexName(),
                        id,
                        source,
                        MediaTypeRegistry.xContentType(source),
                        fieldVisitor.routing()
                    );
                    ParsedDocument doc = indexShard.mapperService().documentMapper().parse(sourceToParse);
                    assert doc.dynamicMappingsUpdate() == null : "mapping updates should not be required on already-indexed doc";
                    // update special fields
                    doc.updateSeqID(docIdAndVersion.seqNo, docIdAndVersion.primaryTerm);
                    doc.version().setLongValue(docIdAndVersion.version);

                    // retrieve stored fields from parsed doc
                    fieldVisitor = buildFieldsVisitors(storedFields, fetchSourceContext);
                    for (IndexableField indexableField : doc.rootDoc().getFields()) {
                        IndexableFieldType fieldType = indexableField.fieldType();
                        if (fieldType.stored()) {
                            FieldInfo fieldInfo = new FieldInfo(
                                indexableField.name(),
                                0,
                                false,
                                false,
                                false,
                                IndexOptions.NONE,
                                DocValuesType.NONE,
                                DocValuesSkipIndexType.NONE,
                                -1,
                                Collections.emptyMap(),
                                0,
                                0,
                                0,
                                0,
                                VectorEncoding.FLOAT32,
                                VectorSimilarityFunction.EUCLIDEAN,
                                false,
                                false
                            );
                            StoredFieldVisitor.Status status = fieldVisitor.needsField(fieldInfo);
                            if (status == StoredFieldVisitor.Status.YES) {
                                if (indexableField.numericValue() != null) {
                                    fieldVisitor.objectField(fieldInfo, indexableField.numericValue());
                                } else if (indexableField.binaryValue() != null) {
                                    fieldVisitor.binaryField(fieldInfo, indexableField.binaryValue());
                                } else if (indexableField.stringValue() != null) {
                                    fieldVisitor.objectField(fieldInfo, indexableField.stringValue());
                                }
                            } else if (status == StoredFieldVisitor.Status.STOP) {
                                break;
                            }
                        }
                    }
                    // retrieve source (with possible transformations, e.g. source filters
                    source = fieldVisitor.source();
                }
            }

            // put stored fields into result objects
            if (!fieldVisitor.fields().isEmpty()) {
                fieldVisitor.postProcess(mapperService::fieldType);
                documentFields = new HashMap<>();
                metadataFields = new HashMap<>();
                for (Map.Entry<String, List<Object>> entry : fieldVisitor.fields().entrySet()) {
                    if (mapperService.isMetadataField(entry.getKey())) {
                        metadataFields.put(entry.getKey(), new DocumentField(entry.getKey(), entry.getValue()));
                    } else {
                        documentFields.put(entry.getKey(), new DocumentField(entry.getKey(), entry.getValue()));
                    }
                }
            }
        }

        if (source != null) {
            // apply request-level source filtering
            if (fetchSourceContext.fetchSource() == false) {
                source = null;
            } else if (fetchSourceContext.includes().length > 0 || fetchSourceContext.excludes().length > 0) {
                Map<String, Object> sourceAsMap;
                // TODO: The source might be parsed and available in the sourceLookup but that one uses unordered maps so different.
                // Do we care?
                Tuple<XContentType, Map<String, Object>> typeMapTuple = XContentHelper.convertToMap(source, true);
                XContentType sourceContentType = typeMapTuple.v1();
                sourceAsMap = typeMapTuple.v2();
                sourceAsMap = XContentMapValues.filter(sourceAsMap, fetchSourceContext.includes(), fetchSourceContext.excludes());
                try {
                    source = BytesReference.bytes(MediaTypeRegistry.contentBuilder(sourceContentType).map(sourceAsMap));
                } catch (IOException e) {
                    throw new OpenSearchException("Failed to get id [" + id + "] with includes/excludes set", e);
                }
            }
        }

        if (!fetchSourceContext.fetchSource()) {
            source = null;
        }

        if (source != null && get.isFromTranslog()) {
            // reapply source filters from mapping (possibly also nulling the source)
            try {
                source = docMapper.sourceMapper().applyFilters(source, null);
            } catch (IOException e) {
                throw new OpenSearchException("Failed to reapply filters for [" + id + "] after reading from translog", e);
            }
        }

        if (source != null && (fetchSourceContext.includes().length > 0 || fetchSourceContext.excludes().length > 0)) {
            Map<String, Object> sourceAsMap;
            // TODO: The source might parsed and available in the sourceLookup but that one uses unordered maps so different. Do we care?
            Tuple<XContentType, Map<String, Object>> typeMapTuple = XContentHelper.convertToMap(source, true);
            XContentType sourceContentType = typeMapTuple.v1();
            sourceAsMap = typeMapTuple.v2();
            sourceAsMap = XContentMapValues.filter(sourceAsMap, fetchSourceContext.includes(), fetchSourceContext.excludes());
            try {
                source = BytesReference.bytes(MediaTypeRegistry.contentBuilder(sourceContentType).map(sourceAsMap));
            } catch (IOException e) {
                throw new OpenSearchException("Failed to get id [" + id + "] with includes/excludes set", e);
            }
        }

        return new GetResult(
            shardId.getIndexName(),
            id,
            get.docIdAndVersion().seqNo,
            get.docIdAndVersion().primaryTerm,
            get.version(),
            get.exists(),
            source,
            documentFields,
            metadataFields
        );
    }

    private static FieldsVisitor buildFieldsVisitors(String[] fields, FetchSourceContext fetchSourceContext) {
        if (fields == null || fields.length == 0) {
            return fetchSourceContext.fetchSource() ? new FieldsVisitor(true) : null;
        }

        return new CustomFieldsVisitor(Sets.newHashSet(fields), fetchSourceContext.fetchSource());
    }
}
