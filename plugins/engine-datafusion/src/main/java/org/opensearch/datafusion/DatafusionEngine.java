/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.datafusion.search.DatafusionContext;
import org.opensearch.datafusion.search.DatafusionQuery;
import org.opensearch.datafusion.search.DatafusionReader;
import org.opensearch.datafusion.search.DatafusionReaderManager;
import org.opensearch.datafusion.search.DatafusionSearcher;
import org.opensearch.datafusion.search.DatafusionSearcherSupplier;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.exec.engine.FileMetadata;
import org.opensearch.index.engine.exec.format.DataFormat;
import org.opensearch.index.engine.exec.read.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.exec.read.EngineSearcherSupplier;
import org.opensearch.index.engine.exec.read.SearchExecEngine;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Base Datafusion engine for search
 */
public class DatafusionEngine extends SearchExecEngine<DatafusionContext, DatafusionSearcher, DatafusionReaderManager> {

    private static final Logger logger = LogManager.getLogger(DatafusionEngine.class);

    private DataFormat dataFormat;
    private DatafusionReaderManager datafusionReaderManager;
    private DatafusionService datafusionService;

    public DatafusionEngine(
        DataFormat dataFormat,
        Collection<FileMetadata> formatCatalogSnapshot,
        DatafusionService dataFusionService,
        ShardPath shardPath
    ) throws IOException {
        this.dataFormat = dataFormat;

        this.datafusionReaderManager = new DatafusionReaderManager(
            shardPath.getDataPath().toString(),
            formatCatalogSnapshot,
            dataFormat.name()
        );
        this.datafusionService = dataFusionService;
    }

    @Override
    public DatafusionContext createContext(
        ReaderContext readerContext,
        ShardSearchRequest request,
        SearchShardTarget searchShardTarget,
        SearchShardTask task,
        BigArrays bigArrays
    ) throws IOException {
        DatafusionContext datafusionContext = new DatafusionContext(readerContext, request, searchShardTarget, task, this, bigArrays);
        // Parse source
        datafusionContext.datafusionQuery(new DatafusionQuery(null/*TODO*/, new ArrayList<>()));
        return datafusionContext;
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher> acquireSearcherSupplier(Function<DatafusionSearcher, DatafusionSearcher> wrapper)
        throws EngineException {
        return acquireSearcherSupplier(wrapper, Engine.SearcherScope.EXTERNAL);
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher> acquireSearcherSupplier(
        Function<DatafusionSearcher, DatafusionSearcher> wrapper,
        Engine.SearcherScope scope
    ) throws EngineException {
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
    public DatafusionSearcher acquireSearcher(
        String source,
        Engine.SearcherScope scope,
        Function<DatafusionSearcher, DatafusionSearcher> wrapper
    ) throws EngineException {
        DatafusionSearcherSupplier releasable = null;
        try {
            DatafusionSearcherSupplier searcherSupplier = releasable = (DatafusionSearcherSupplier) acquireSearcherSupplier(wrapper, scope);
            DatafusionSearcher searcher = searcherSupplier.acquireSearcher(source);
            releasable = null;
            return new DatafusionSearcher(source, searcher.getReader(), () -> Releasables.close(searcher, searcherSupplier));
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
            datafusionSearcher.search(context.getDatafusionQuery());
            long streamPtr = context.getStreamNativePtr();
            // TODO : process stream to form result
        } catch (Exception exception) {
            logger.error("Failed to execute Substrait query plan", exception);
        }
        return finalRes;
    }

}
