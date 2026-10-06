/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DeleteExecutionEngine;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.plugins.Plugin;

import java.util.Set;
import java.util.function.Function;

/**
 * A mock {@link DataFormatPlugin} for testing purposes.
 */
public class MockDataFormatPlugin extends Plugin implements DataFormatPlugin {
    private final MockDataFormat dataFormat;
    private Function<IndexingEngineConfig, IndexingExecutionEngine<?, ?>> indexingEngineFactory;
    private Function<Committer, DeleteExecutionEngine<?>> deleteEngineFactory;

    public MockDataFormatPlugin() {
        this(new MockDataFormat("", 100L, Set.of()));
    }

    protected MockDataFormatPlugin(MockDataFormat mockDataFormat) {
        this.dataFormat = mockDataFormat;
    }

    public static MockDataFormatPlugin of(MockDataFormat dataFormat) {
        return new MockDataFormatPlugin(dataFormat);
    }

    @Override
    public DataFormat getDataFormat() {
        return dataFormat;
    }

    /** Supplies the indexing engine instead of the default mock, so tests need not subclass this plugin. */
    public MockDataFormatPlugin withIndexingEngine(Function<IndexingEngineConfig, IndexingExecutionEngine<?, ?>> factory) {
        this.indexingEngineFactory = factory;
        return this;
    }

    /** Supplies the delete engine instead of the default mock, so tests need not subclass this plugin. */
    public MockDataFormatPlugin withDeleteExecutionEngine(Function<Committer, DeleteExecutionEngine<?>> factory) {
        this.deleteEngineFactory = factory;
        return this;
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings) {
        return indexingEngineFactory == null ? new MockIndexingExecutionEngine(dataFormat) : indexingEngineFactory.apply(settings);
    }

    @Override
    public DeleteExecutionEngine<?> getDeleteExecutionEngine(Committer committer) {
        return deleteEngineFactory == null ? new MockDeleteExecutionEngine(dataFormat) : deleteEngineFactory.apply(committer);
    }
}
