/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.datafusion.search.*;
import org.opensearch.datafusion.search.AsyncRecordBatchIterator;
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.index.engine.*;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.mapper.*;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.SearchResultsCollector;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.vectorized.execution.search.DataFormat;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static java.util.Collections.emptyMap;

public class DatafusionEngine extends SearchExecEngine<DatafusionContext, DatafusionSearcher,
    DatafusionReaderManager, DatafusionQuery> implements Closeable {

    private static final Logger logger = LogManager.getLogger(DatafusionEngine.class);

    private DataFormat dataFormat;
    private DatafusionReaderManager datafusionReaderManager;
    private DataFusionService datafusionService;
    private CacheManager cacheManager;
    private final RootAllocator rootAllocator;

    public DatafusionEngine(DataFormat dataFormat, Collection<FileMetadata> formatCatalogSnapshot, DataFusionService dataFusionService, ShardPath shardPath) throws IOException {
        this.dataFormat = dataFormat;
        
        // Create shard ID from index name and shard ID
        String shardId = shardPath.getShardId().getIndexName() + "[" + shardPath.getShardId().getId() + "]";
        
        this.datafusionReaderManager = new DatafusionReaderManager(
            shardPath.getDataPath().toString(), 
            formatCatalogSnapshot, 
            dataFormat.getName(),
            shardId
        );
        this.datafusionService = dataFusionService;
        this.cacheManager = datafusionService.getCacheManager();
        this.rootAllocator = new RootAllocator(Long.MAX_VALUE);
        if (this.cacheManager != null) {
            datafusionReaderManager.setOnFilesAdded(files -> {
                // Handle new files added during refresh
                cacheManager.addFilesToCacheManager(files);
            });
        }
    }

    @Override
    public DatafusionContext createContext(ReaderContext readerContext, ShardSearchRequest request, SearchShardTarget searchShardTarget, SearchShardTask task, BigArrays bigArrays, SearchContext originalContext) throws IOException {
        DatafusionContext datafusionContext = new DatafusionContext(readerContext, request, searchShardTarget, task, this, bigArrays, originalContext);
        // Parse source
        datafusionContext.datafusionQuery(new DatafusionQuery(request.shardId().getIndexName(), request.source().queryPlanIR(), new ArrayList<>()));
        return datafusionContext;
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher> acquireSearcherSupplier(Function<DatafusionSearcher, DatafusionSearcher> wrapper) throws EngineException {
        return acquireSearcherSupplier(wrapper, Engine.SearcherScope.EXTERNAL);
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher> acquireSearcherSupplier(Function<DatafusionSearcher, DatafusionSearcher> wrapper, Engine.SearcherScope scope) throws EngineException {
        // TODO : wrapper is ignored
        EngineSearcherSupplier<DatafusionSearcher> searcher = null;
        // TODO : refcount needs to be revisited - add proper tests for exception etc
        try {
            DatafusionReader reader = datafusionReaderManager.acquire();
            searcher = new DatafusionSearcherSupplier(null) {
                @Override
                protected DatafusionSearcher acquireSearcherInternal(String source) {
                    return new DatafusionSearcher(source, reader,
                         () -> {});

                }

                @Override
                protected void doClose() {
                    try {
                        datafusionReaderManager.release(reader);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };
        } catch (Exception ex) {
            logger.error("Failed to acquire searcher {}", ex.toString(), ex);
            // TODO
        }
        return searcher;
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source) throws EngineException {
        return acquireSearcher(source, Engine.SearcherScope.EXTERNAL);
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source, Engine.SearcherScope scope) throws EngineException {
        return acquireSearcher(source, scope, Function.identity());
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source, Engine.SearcherScope scope, Function<DatafusionSearcher, DatafusionSearcher> wrapper) throws EngineException {
        DatafusionSearcherSupplier releasable = null;
        try {
            DatafusionSearcherSupplier searcherSupplier = releasable = (DatafusionSearcherSupplier) acquireSearcherSupplier(wrapper, scope);
            DatafusionSearcher searcher = searcherSupplier.acquireSearcher(source);
            releasable = null;

            return new DatafusionSearcher(
                source,
                searcher.getReader(),
                () -> Releasables.close(searcher, searcherSupplier)
            );
        } finally {
            Releasables.close(releasable);
        }
    }

    @Override
    public DatafusionReaderManager getReferenceManager(Engine.SearcherScope scope) {
        return datafusionReaderManager;
    }

    @Override
    public CatalogSnapshotAwareRefreshListener getRefreshListener(Engine.SearcherScope scope) {
        return datafusionReaderManager;
    }

    @Override
    public FileDeletionListener getFileDeletionListener(Engine.SearcherScope scope) {
        return datafusionReaderManager;
    }

    @Override
    public boolean assertSearcherIsWarmedUp(String source, Engine.SearcherScope scope) {
        return false;
    }

    @Override
    public void close() {
        rootAllocator.close();
    }


    @Override
    public Map<String, Object[]> executeQueryPhase(DatafusionContext context) {
        Map<String, Object[]> finalRes = new HashMap<>();
        List<Long> rowIdResult = new ArrayList<>();
        RecordBatchStream stream = null;

        try {
            DatafusionSearcher datafusionSearcher = context.getEngineSearcher();
            long streamPointer = datafusionSearcher.search(context.getDatafusionQuery(), datafusionService.getRuntimePointer());
            stream = new RecordBatchStream(streamPointer, datafusionService.getRuntimePointer(), rootAllocator);

            // We can have some collectors passed like this which can collect the results and convert to InternalAggregation
            // Is the possible? need to check

            SearchResultsCollector<RecordBatchIterator> collector = iterator -> {
                while (iterator.hasNext()) {
                    VectorSchemaRoot root = iterator.next();
                    for (Field field : root.getSchema().getFields()) {
                        String fieldName = field.getName();
                        FieldVector fieldVector = root.getVector(fieldName);
                        Object[] fieldValues = new Object[fieldVector.getValueCount()];
                        if (fieldName.equals(CompositeDataFormatWriter.ROW_ID)) {
                            FieldVector rowIdVector = root.getVector(fieldName);
                            for(int i=0; i<fieldVector.getValueCount(); i++) {
                                rowIdResult.add((long) rowIdVector.getObject(i));
                                fieldValues[i] = fieldVector.getObject(i);
                            }
                        }
                        else {
                            for (int i = 0; i < fieldVector.getValueCount(); i++) {
                                fieldValues[i] = fieldVector.getObject(i);
                            }
                        }
                        finalRes.put(fieldName, fieldValues);
                    }
                }
            };

            collector.collect(new RecordBatchIterator(stream));

//            logger.info("Final Results:");
//            for (Map.Entry<String, Object[]> entry : finalRes.entrySet()) {
//                logger.info("{}: {}", entry.getKey(), java.util.Arrays.toString(entry.getValue()));
//            }


//            logger.info("Memory Pool Allocation Post Query ShardID:{}", context.getQueryShardContext().getShardId());
//            printMemoryPoolAllocation(datafusionService.getRuntimePointer());


//            logger.info("Final Results:");
//            for (Map.Entry<String, Object[]> entry : finalRes.entrySet()) {
//                logger.info("{}: {}", entry.getKey(), java.util.Arrays.toString(entry.getValue()));
//            }

        } catch (Exception exception) {
            logger.error("Failed to execute Substrait query plan", exception);
            throw new RuntimeException(exception);
        } finally {
            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        context.queryResult().topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(rowIdResult.size(), TotalHits.Relation.EQUAL_TO), rowIdResult.stream().map(d-> new ScoreDoc(d.intValue(), Float.NaN, context.indexShard().shardId().getId())).toList().toArray(ScoreDoc[]::new)) , Float.NaN), new DocValueFormat[0]);
        return finalRes;
    }

    @Override
    public void executeQueryPhaseAsync(DatafusionContext context, Executor executor, ActionListener<Map<String, Object[]>> listener) {
        try {
            DatafusionSearcher datafusionSearcher = context.getEngineSearcher();
            datafusionSearcher.searchAsync(context.getDatafusionQuery(), datafusionService.getRuntimePointer()).whenCompleteAsync((streamPointer, error)-> {
                Map<String, Object[]> finalRes = new HashMap<>();
                List<Long> rowIdResult = new ArrayList<>();
                if(streamPointer == null) {
                    throw new RuntimeException(error);
                }
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                RecordBatchStream stream = new RecordBatchStream(streamPointer, datafusionService.getRuntimePointer() , allocator);
                SearchResultsCollector<RecordBatchStream> collector = new SearchResultsCollector<RecordBatchStream>() {
                    @Override
                    public void collect(RecordBatchStream value) {
                        VectorSchemaRoot root = value.getVectorSchemaRoot();
                        for (Field field : root.getSchema().getFields()) {
                            String fieldName = field.getName();
                            FieldVector fieldVector = root.getVector(fieldName);
                            Object[] fieldValues = new Object[fieldVector.getValueCount()];
                            if (fieldName.equals(CompositeDataFormatWriter.ROW_ID)) {
                                FieldVector rowIdVector = root.getVector(fieldName);
                                for(int i=0; i<fieldVector.getValueCount(); i++) {
                                    rowIdResult.add((long) rowIdVector.getObject(i));
                                    fieldValues[i] = fieldVector.getObject(i);
                                }
                            }
                            else {
                                for (int i = 0; i < fieldVector.getValueCount(); i++) {
                                    fieldValues[i] = fieldVector.getObject(i);
                                }
                            }
                            finalRes.put(fieldName, fieldValues);
                        }
                    }
                };
                loadNextBatch(stream, executor, collector, finalRes, allocator, listener, context, rowIdResult);
            });

//            logger.info("Memory Pool Allocation Post Query ShardID:{}", context.getQueryShardContext().getShardId());
//            printMemoryPoolAllocation(datafusionService.getRuntimePointer());


//            logger.info("Final Results:");
//            for (Map.Entry<String, Object[]> entry : finalRes.entrySet()) {
//                logger.info("{}: {}", entry.getKey(), java.util.Arrays.toString(entry.getValue()));
//            }

        } catch (Exception exception) {
            logger.error("Failed to execute Substrait query plan", exception);
            throw new RuntimeException(exception);
        }
        //return finalRes;
    }

    private void loadNextBatch(
        RecordBatchStream stream,
        Executor executor,
        SearchResultsCollector<RecordBatchStream> collector,
        Map<String, Object[]> finalRes,
        RootAllocator allocator,
        ActionListener<Map<String, Object[]>> listener,
        DatafusionContext context,
        List<Long> rowIdResult
    ) {
        AsyncRecordBatchIterator iterator = new AsyncRecordBatchIterator(stream);
        iterator.nextAsync(ActionListener.wrap(hasMore -> {
            if (hasMore) {
                try {
                    collector.collect(stream);
                    // Recursively load next batch - TODO : anyway to Change this to iteration ?
                    loadNextBatch(stream, executor, collector, finalRes, allocator, listener, context, rowIdResult);
                } catch (Exception e) {
                    cleanup(stream, allocator);
                    listener.onFailure(e);
                }
            } else {
                cleanup(stream, allocator);
                context.queryResult().topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(rowIdResult.size(),
                    TotalHits.Relation.EQUAL_TO), rowIdResult.stream().map(d-> new ScoreDoc(d.intValue(),
                    Float.NaN, context.indexShard().shardId().getId())).toList().toArray(ScoreDoc[]::new)) , Float.NaN), new DocValueFormat[0]);
                listener.onResponse(finalRes);
            }
        }, error -> {
            cleanup(stream, allocator);
            listener.onFailure(new RuntimeException("Error loading batch", error));
        }));
    }
    private void cleanup(RecordBatchStream stream, RootAllocator allocator) {
        try {
            if (stream != null) stream.close();
            if (allocator != null) allocator.close();
        } catch (Exception e) {
            logger.error("Cleanup error", e);
        }
    }


    /**
     * Executes fetch phase, DataFusion query should contain projections for fields
     * @param context DataFusion context
     * @throws IOException
     */
    @Override
    public void executeFetchPhase(DatafusionContext context) throws IOException {

        List<Long> rowIds = Arrays.stream(context.docIdsToLoad()).mapToObj(Long::valueOf).toList();
        if (rowIds.isEmpty()) {
            // no individual hits to process, so we shortcut
            context.fetchResult()
                .hits(new SearchHits(new SearchHit[0], context.queryResult().getTotalHits(), context.queryResult().getMaxScore()));
            return;
        }

        // preprocess
        context.getDatafusionQuery().setFetchPhaseContext(rowIds);
        List<String> projections = new ArrayList<>(List.of(context.request().source().fetchSource().includes()));
        projections.add(CompositeDataFormatWriter.ROW_ID);
        context.getDatafusionQuery().setProjections(projections);
        DatafusionSearcher datafusionSearcher = context.getEngineSearcher();
        long streamPointer = datafusionSearcher.search(context.getDatafusionQuery(), datafusionService.getRuntimePointer());
        RecordBatchStream stream = new RecordBatchStream(streamPointer, datafusionService.getRuntimePointer(), rootAllocator);

        Map<Long, Integer> rowIdToIndex = new HashMap<>();
        for (int idx = 0; idx < rowIds.size(); idx++) {
            rowIdToIndex.put(rowIds.get(idx), idx);
        }

        MapperService mapperService = context.mapperService();
        MappingLookup mappingLookup = mapperService.documentMapper().mappers();
        SearchResultsCollector<RecordBatchIterator> collector = iterator -> {
            List<BytesReference> byteRefs = new ArrayList<>();
            SearchHit[] hits = new SearchHit[rowIds.size()];
            int totalHits = 0;
            while (iterator.hasNext()) {
                VectorSchemaRoot vectorSchemaRoot = iterator.next();
                List<FieldVector> fieldVectorList = vectorSchemaRoot.getFieldVectors();
                for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
                    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                    String _id = "_id";
                    Long row_id = null;

                    try {
                        for (FieldVector valueVectors : fieldVectorList) {
                            if (valueVectors.getName().equals(CompositeDataFormatWriter.ROW_ID)) {
                                row_id = (long) valueVectors.getObject(i);
                                continue;
                            }
                            Mapper mapper = mappingLookup.getMapper(valueVectors.getName());
                            DerivedFieldGenerator derivedFieldGenerator = mapper.derivedFieldGenerator();

                            Object value = valueVectors.getObject(i);
                            if(valueVectors instanceof ViewVarCharVector) {
                                BytesRef bytesRef = new BytesRef(((ViewVarCharVector) valueVectors).get(i));
                                derivedFieldGenerator.generate(builder, List.of(bytesRef)); // TODO: // Currently keyword field mapper do not have derived field converter from byte[] to BytesRef
                            } else {
                                derivedFieldGenerator.generate(builder, List.of(value));
                            }
                            if (valueVectors.getName().equals(IdFieldMapper.NAME)) {
                                BytesRef idRef = new BytesArray((byte[]) value).toBytesRef();
                                _id = Uid.decodeId(idRef.bytes, idRef.offset, idRef.length);
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Failed to derive source for doc id [{i}]: {}", i, e);
                        throw new OpenSearchException("Failed to derive source for doc id [" + i + "]", e);
                    } finally {
                        builder.endObject();
                    }
                    assert row_id != null || rowIds.get(i) != null;
                    assert _id != null;
                    BytesReference document = BytesReference.bytes(builder);
                    byteRefs.add(document);
                    SearchHit hit = new SearchHit(Math.toIntExact(rowIds.get(i)), _id, emptyMap(), emptyMap());
                    hit.sourceRef(document);
                    FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext(hit, null, Math.toIntExact(rowIds.get(i)), new SourceLookup()); //TODO: make source lookup one per thread
                    hitContext.sourceLookup().setSource(document);
                    int index = rowIdToIndex.get(row_id);
                    hits[index] = hit;
                    totalHits++;
                }
            }
            context.fetchResult().hits(new SearchHits(hits, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), context.queryResult().getMaxScore()));
        };

        try {
            collector.collect(new RecordBatchIterator(stream));
        } catch (IOException exception) {
            logger.error("Failed to perform fetch phase", exception);
            throw new RuntimeException(exception);
        } finally {
            try {
                stream.close();
            } catch (Exception e) {
                logger.error("Failed to close stream", e);
                throw new RuntimeException(e);
            }
        }
    }
}
