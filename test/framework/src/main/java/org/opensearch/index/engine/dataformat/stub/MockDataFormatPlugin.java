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

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * A mock {@link DataFormatPlugin} for testing purposes.
 */
public class MockDataFormatPlugin extends Plugin implements DataFormatPlugin {
    private final MockDataFormat dataFormat;
    private final Collection<DataFormat> auxiliaryDataFormats;

    public MockDataFormatPlugin() {
        this(new MockDataFormat("", 100L, Set.of()));
    }

    protected MockDataFormatPlugin(MockDataFormat mockDataFormat) {
        this(mockDataFormat, List.of());
    }

    protected MockDataFormatPlugin(MockDataFormat mockDataFormat, Collection<DataFormat> auxiliaryDataFormats) {
        this.dataFormat = mockDataFormat;
        this.auxiliaryDataFormats = List.copyOf(auxiliaryDataFormats);
    }

    public static MockDataFormatPlugin of(MockDataFormat dataFormat) {
        return new MockDataFormatPlugin(dataFormat);
    }

    /** Creates a plugin owning {@code dataFormat} plus the given auxiliary (side-table) formats. */
    public static MockDataFormatPlugin of(MockDataFormat dataFormat, DataFormat... auxiliaryDataFormats) {
        return new MockDataFormatPlugin(dataFormat, List.of(auxiliaryDataFormats));
    }

    @Override
    public DataFormat getDataFormat() {
        return dataFormat;
    }

    @Override
    public Collection<DataFormat> getAuxiliaryDataFormats() {
        return auxiliaryDataFormats;
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings) {
        return new MockIndexingExecutionEngine(dataFormat);
    }

    @Override
    public DeleteExecutionEngine<?> getDeleteExecutionEngine(Committer committer) {
        return new MockDeleteExecutionEngine(dataFormat);
    }
}
