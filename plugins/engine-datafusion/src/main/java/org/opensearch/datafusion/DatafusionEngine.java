/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.datafusion.search.DatafusionContext;
import org.opensearch.datafusion.search.DatafusionQuery;
import org.opensearch.datafusion.search.DatafusionQueryPhaseExecutor;
import org.opensearch.datafusion.search.DatafusionReaderManager;
import org.opensearch.datafusion.search.DatafusionSearcher;
import org.opensearch.index.engine.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineSearcherSupplier;
import org.opensearch.index.engine.ReadEngine;
import org.opensearch.index.engine.SearcherOperations;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QueryPhaseExecutor;
import org.opensearch.vectorized.execution.search.DataFormat;
import org.opensearch.search.query.GenericQueryPhaseSearcher;
import org.opensearch.search.EngineReaderContext;
import org.opensearch.search.ContextEngineSearcher;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.function.Function;

public class DatafusionEngine extends ReadEngine<DatafusionContext, DatafusionSearcher<DatafusionQuery>,
    DatafusionReaderManager, DatafusionQuery, ContextEngineSearcher<DatafusionQuery>> {

    private DataFormat dataFormat;
    private DatafusionReaderManager datafusionReaderManager;

    public DatafusionEngine(DataFormat dataFormat, Collection<FileMetadata> formatCatalogSnapshot) throws IOException {
        this.dataFormat = dataFormat;
        this.datafusionReaderManager = new DatafusionReaderManager("TODO://FigureOutPath", formatCatalogSnapshot);
    }

    @Override
    public GenericQueryPhaseSearcher<DatafusionContext, ContextEngineSearcher<DatafusionQuery>, DatafusionQuery> getQueryPhaseSearcher() {
        return new DatafusionQueryPhaseSearcher();
    }

    @Override
    public QueryPhaseExecutor<DatafusionContext> getQueryPhaseExecutor() {
        return new DatafusionQueryPhaseExecutor();
    }

    @Override
    public DatafusionContext createContext(ReaderContext readerContext, ShardSearchRequest request, SearchShardTask task) throws IOException {
        return null;
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher<DatafusionQuery>> acquireSearcherSupplier(Function<DatafusionSearcher<DatafusionQuery>, DatafusionSearcher<DatafusionQuery>> wrapper) throws EngineException {
        return null;
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher<DatafusionQuery>> acquireSearcherSupplier(Function<DatafusionSearcher<DatafusionQuery>, DatafusionSearcher<DatafusionQuery>> wrapper, Engine.SearcherScope scope) throws EngineException {
        return null;
    }

    @Override
    public DatafusionSearcher<DatafusionQuery> acquireSearcher(String source) throws EngineException {
        return null;
    }

    @Override
    public DatafusionSearcher<DatafusionQuery> acquireSearcher(String source, Engine.SearcherScope scope) throws EngineException {
        return null;
    }

    @Override
    public DatafusionSearcher<DatafusionQuery> acquireSearcher(String source, Engine.SearcherScope scope, Function<DatafusionSearcher<DatafusionQuery>, DatafusionSearcher<DatafusionQuery>> wrapper) throws EngineException {
        return null;
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
