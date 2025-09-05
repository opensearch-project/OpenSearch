/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.lease.Releasables;
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
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QueryPhaseExecutor;
import org.opensearch.vectorized.execution.search.DataFormat;
import org.opensearch.search.query.GenericQueryPhaseSearcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

public class DatafusionEngine extends SearchExecEngine<DatafusionContext, DatafusionSearcher,
    DatafusionReaderManager, DatafusionQuery> {

    private DataFormat dataFormat;
    private DatafusionReaderManager datafusionReaderManager;

    public DatafusionEngine(DataFormat dataFormat, Collection<FileMetadata> formatCatalogSnapshot) throws IOException {
        this.dataFormat = dataFormat;
        this.datafusionReaderManager = new DatafusionReaderManager("TODO://FigureOutPath", formatCatalogSnapshot);
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
    public DatafusionContext createContext(ReaderContext readerContext, ShardSearchRequest request, SearchShardTask task) throws IOException {
        DatafusionContext datafusionContext = new DatafusionContext(readerContext, request, task, this);
        // Parse source
        datafusionContext.datafusionQuery(new DatafusionQuery(request.source().getSubstraitBytes(), new ArrayList<>()));
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
}
