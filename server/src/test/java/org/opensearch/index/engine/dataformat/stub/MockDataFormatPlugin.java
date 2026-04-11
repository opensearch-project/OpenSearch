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

/**
 * A mock {@link DataFormatPlugin} for testing purposes.
 */
public class MockDataFormatPlugin implements DataFormatPlugin {
    private final MockDataFormat dataFormat;

    public MockDataFormatPlugin() {
        this(new MockDataFormat());
    }

    public MockDataFormatPlugin(MockDataFormat dataFormat) {
        this.dataFormat = dataFormat;
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
