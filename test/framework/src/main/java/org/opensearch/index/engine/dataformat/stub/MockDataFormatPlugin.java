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
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.plugins.Plugin;

import java.util.Set;

/**
 * A mock {@link DataFormatPlugin} for testing purposes.
 */
public class MockDataFormatPlugin extends Plugin implements DataFormatPlugin {
    private final MockDataFormat dataFormat;

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

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings) {
        return new MockIndexingExecutionEngine(dataFormat);
    }
}
