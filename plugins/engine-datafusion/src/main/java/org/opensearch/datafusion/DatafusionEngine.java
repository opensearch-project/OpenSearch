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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.BigArrays;
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
import org.opensearch.index.shard.ShardPath;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.SearchResultsCollector;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QueryPhaseExecutor;
import org.opensearch.vectorized.execution.search.DataFormat;
import org.opensearch.search.query.GenericQueryPhaseSearcher;
import org.opensearch.vectorized.execution.search.spi.SessionConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class DatafusionEngine extends SearchExecEngine<DatafusionContext, DatafusionSearcher,
    DatafusionReaderManager, DatafusionQuery> {

    private static final Logger logger = LogManager.getLogger(DatafusionEngine.class);

    private DataFormat dataFormat;
    private DatafusionReaderManager datafusionReaderManager;
    private final SessionConfig formatSessionConfig;
    private DataFusionService datafusionService;

    public static final Setting<Boolean> CLUSTER_DATAFUSION_STATISTICS_ENABLED = Setting.boolSetting(
        "cluster.datafusion.statistics.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> INDEX_DATAFUSION_STATISTICS_ENABLED = Setting.boolSetting(
        "index.datafusion.statistics.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    public static final Setting<Integer> CLUSTER_DATAFUSION_BATCH_SIZE = Setting.intSetting(
        "cluster.datafusion.batch_size",
        2048,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );


    // If
    public DatafusionEngine(DataFormat dataFormat,
                            SessionConfig formatSessionConfig,
                            Collection<FileMetadata> formatCatalogSnapshot, DataFusionService dataFusionService, ShardPath shardPath) throws IOException {
        this.dataFormat = dataFormat;
        this.formatSessionConfig = formatSessionConfig;
        this.datafusionReaderManager = new DatafusionReaderManager(shardPath.getDataPath().toString(), formatCatalogSnapshot, dataFormat.getName());
        this.datafusionService = dataFusionService;
    }

    public SessionConfig getFormatSessionConfig() {
        return formatSessionConfig;
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
    public DatafusionContext createContext(ReaderContext readerContext, ClusterService clusterService, ShardSearchRequest request, SearchShardTarget searchShardTarget, SearchShardTask task, BigArrays bigArrays, SearchContext originalContext) throws IOException {
        DatafusionContext datafusionContext = new DatafusionContext(
            readerContext,
            clusterService,
            request,
            searchShardTarget,
            task,
            this,
            bigArrays, originalContext);
        // Parse source
        datafusionContext.datafusionQuery(new DatafusionQuery(dataFormat.getName(), request.source().queryPlanIR(), new ArrayList<>()));
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
    public Map<String, Object[]> execute(DatafusionContext context) {
        Map<String, Object[]> finalRes = new HashMap<>();
        try {
            DatafusionSearcher datafusionSearcher = context.getEngineSearcher();
            long streamPointer = datafusionSearcher.search(context.getDatafusionQuery(), context.readEngineConfig(), datafusionService.getTokioRuntimePointer());
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            RecordBatchStream stream = new RecordBatchStream(streamPointer, datafusionService.getTokioRuntimePointer() , allocator);

            // We can have some collectors passed like this which can collect the results and convert to InternalAggregation
            // Is the possible? need to check

            SearchResultsCollector<RecordBatchStream> collector = new SearchResultsCollector<RecordBatchStream>() {
                @Override
                public void collect(RecordBatchStream value) {
                    VectorSchemaRoot root = value.getVectorSchemaRoot();
                    for (Field field : root.getSchema().getFields()) {
                        String filedName = field.getName();
                        FieldVector fieldVector = root.getVector(filedName);
                        Object[] fieldValues = new Object[fieldVector.getValueCount()];
                        for (int i = 0; i < fieldVector.getValueCount(); i++) {
                            fieldValues[i] = fieldVector.getObject(i);
                        }
                        finalRes.put(filedName, fieldValues);
                    }
                }
            };

            while (stream.loadNextBatch().join()) {
                collector.collect(stream);
            }

            logger.info("Final Results:");
            for (Map.Entry<String, Object[]> entry : finalRes.entrySet()) {
                logger.info("{}: {}", entry.getKey(), java.util.Arrays.toString(entry.getValue()));
            }

        } catch (Exception exception) {
            logger.error("Failed to execute Substrait query plan", exception);
        }
        return finalRes;
    }
}
