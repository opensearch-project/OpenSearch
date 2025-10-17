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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.datafusion.search.DatafusionContext;
import org.opensearch.datafusion.search.DatafusionQuery;
import org.opensearch.datafusion.search.DatafusionQueryPhaseExecutor;
import org.opensearch.datafusion.search.DatafusionReader;
import org.opensearch.datafusion.search.DatafusionReaderManager;
import org.opensearch.datafusion.search.DatafusionSearcher;
import org.opensearch.datafusion.search.DatafusionSearcherSupplier;
import org.opensearch.index.engine.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineSearcherSupplier;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.mapper.*;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.SearchResultsCollector;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QueryPhaseExecutor;
import org.opensearch.vectorized.execution.search.DataFormat;
import org.opensearch.search.query.GenericQueryPhaseSearcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Function;

public class DatafusionEngine extends SearchExecEngine<DatafusionContext, DatafusionSearcher,
    DatafusionReaderManager, DatafusionQuery> {

    private static final Logger logger = LogManager.getLogger(DatafusionEngine.class);

    private DataFormat dataFormat;
    private DatafusionReaderManager datafusionReaderManager;
    private DataFusionService datafusionService;

    public DatafusionEngine(DataFormat dataFormat, Collection<FileMetadata> formatCatalogSnapshot, DataFusionService dataFusionService, ShardPath shardPath) throws IOException {
        this.dataFormat = dataFormat;

        this.datafusionReaderManager = new DatafusionReaderManager(shardPath.getDataPath().toString(), formatCatalogSnapshot, dataFormat.getName());
        this.datafusionService = dataFusionService;
    }

    @Override
    public GenericQueryPhaseSearcher<DatafusionContext, DatafusionSearcher, DatafusionQuery> getQueryPhaseSearcher() {
        return new DatafusionQueryPhaseSearcher();
    }

    @Override
    public QueryPhaseExecutor<DatafusionContext> getQueryPhaseExecutor() {
        return new DatafusionQueryPhaseExecutor();
    }

    @Override
    public DatafusionContext createContext(ReaderContext readerContext, ShardSearchRequest request, SearchShardTarget searchShardTarget, SearchShardTask task, BigArrays bigArrays) throws IOException {
        DatafusionContext datafusionContext = new DatafusionContext(readerContext, request, searchShardTarget, task, this, bigArrays);
        // Parse source
        datafusionContext.datafusionQuery(new DatafusionQuery(request.source().queryPlanIR(), new ArrayList<>()));
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
                    return new DatafusionSearcher(source, reader, () -> {});
                }

                @Override
                protected void doClose() {
                    try {
                        reader.decRef();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };
        } catch (Exception ex) {
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
    public boolean assertSearcherIsWarmedUp(String source, Engine.SearcherScope scope) {
        return false;
    }

    @Override
    public void executeQueryPhase(DatafusionContext context) {
        Map<String, Object[]> finalRes = new HashMap<>();
        ArrayList<Long> rowIdResult = new ArrayList<>();

        try {
            DatafusionSearcher datafusionSearcher = context.getEngineSearcher();
            long streamPointer = datafusionSearcher.search(context.getDatafusionQuery(), datafusionService.getTokioRuntimePointer());
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            RecordBatchStream stream = new RecordBatchStream(streamPointer, datafusionService.getTokioRuntimePointer() , allocator);

            // We can have some collectors passed like this which can collect the results and convert to InternalAggregation
            // Is the possible? need to check

            SearchResultsCollector<RecordBatchStream> collector = new SearchResultsCollector<RecordBatchStream>() {
                @Override
                public void collect(RecordBatchStream value) {
                    VectorSchemaRoot root = value.getVectorSchemaRoot();
                    for (Field field : root.getSchema().getFields()) {
                        String fieldName = field.getName();
                        FieldVector fieldVector = root.getVector(fieldName);
                        Object[] fieldValues = new Object[fieldVector.getValueCount()];
                        if (fieldName.equals("___row_id")) {
                            IntVector rowIdVector = (IntVector) root.getVector(fieldName);
                            for(int i=0; i<fieldVector.getValueCount(); i++) {
                                rowIdResult.add((long) rowIdVector.get(i));
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

            while (stream.loadNextBatch().join()) {
                collector.collect(stream);
            }

            for (Map.Entry<String, Object[]> entry : finalRes.entrySet()) {
                logger.info("{}: {}", entry.getKey(), java.util.Arrays.toString(entry.getValue()));
            }

        } catch (Exception exception) {
            logger.error("Failed to execute Substrait query plan", exception);
        }
        context.setDfQueryPhaseResult(rowIdResult);
        context.setDFResults(finalRes);
    }


    /**
     * Executes fetch phase, DataFusion query should contain projections for fields
     * @param context DataFusion context
     * @throws IOException
     */
    @Override
    public void executeFetchPhase(DatafusionContext context) throws IOException {
        List<Long> rowIds = context.getDfQueryResult();

        // preprocess
        context.getDatafusionQuery().setFetchPhaseContext(rowIds);
        DatafusionSearcher datafusionSearcher = context.getEngineSearcher();
        long streamPointer = datafusionSearcher.search(context.getDatafusionQuery(), datafusionService.getTokioRuntimePointer()); // update to handle fetchPhase query
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        RecordBatchStream stream = new RecordBatchStream(streamPointer, datafusionService.getTokioRuntimePointer() , allocator);

        // postprocess
        context.setDfFetchPhaseResult(generateByteRefs(context, stream));
    }

    private List<BytesReference> generateByteRefs(DatafusionContext context, RecordBatchStream recordBatchStream) throws IOException {
        MapperService mapperService = context.mapperService();
        List<BytesReference> byteRefs = new ArrayList<>();
        while(recordBatchStream.loadNextBatch().join()) {
            VectorSchemaRoot vectorSchemaRoot = recordBatchStream.getVectorSchemaRoot();
            List<FieldVector> fieldVectorList = vectorSchemaRoot.getFieldVectors();
            for(int i=0; i<vectorSchemaRoot.getRowCount(); i++) {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

                try {
                    for (FieldVector valueVectors : fieldVectorList) {

                        MappingLookup mappingLookup = mapperService.documentMapper().mappers();
                        Mapper mapper = mappingLookup.getMapper(valueVectors.getName());
                        DerivedFieldGenerator derivedFieldGenerator = mapper.derivedFieldGenerator();

                        derivedFieldGenerator.generate(builder, List.of(valueVectors.getObject(i)));
                    }
                } catch (Exception e) {
                    throw new OpenSearchException("Failed to derive source for doc id [" + i + "]", e);
                } finally {
                    builder.endObject();
                }
                System.out.println("Object: ");
                System.out.println(builder.toString());
                byteRefs.add(BytesReference.bytes(builder));
            }
        }
        return byteRefs;
    }
}
