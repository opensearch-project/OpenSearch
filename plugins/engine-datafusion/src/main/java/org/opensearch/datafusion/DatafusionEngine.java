/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.datafusion.search.DatafusionReaderManager;
import org.opensearch.datafusion.search.DatafusionSearcher;
import org.opensearch.index.engine.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineSearcherSupplier;
import org.opensearch.index.engine.SearcherOperations;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.vectorized.execution.search.DataFormat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.function.Function;

public class DatafusionEngine implements SearcherOperations<DatafusionSearcher, DatafusionReaderManager> {

    private DataFormat dataFormat;
    private DatafusionReaderManager datafusionReaderManager;

    public DatafusionEngine(DataFormat dataFormat, Collection<FileMetadata> formatCatalogSnapshot) throws IOException {
        this.dataFormat = dataFormat;
        this.datafusionReaderManager = new DatafusionReaderManager("TODO://FigureOutPath", formatCatalogSnapshot);
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher> acquireSearcherSupplier(Function<DatafusionSearcher, DatafusionSearcher> wrapper) throws EngineException {
        return null;
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher> acquireSearcherSupplier(Function<DatafusionSearcher, DatafusionSearcher> wrapper, Engine.SearcherScope scope) throws EngineException {
        return null;
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source) throws EngineException {
        return null;
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source, Engine.SearcherScope scope) throws EngineException {
        return null;
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source, Engine.SearcherScope scope, Function<DatafusionSearcher, DatafusionSearcher> wrapper) throws EngineException {
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
