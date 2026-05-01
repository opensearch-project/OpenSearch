/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.index.engine.dataformat.AbstractDataFormatAwareEngineTestCase;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockSearchBackEndPlugin;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.util.List;
import java.util.Set;

/**
 * Runs the {@link AbstractDataFormatAwareEngineTestCase} suite with the mock
 * in-memory engine. Validates that the DFAE orchestration layer works correctly
 * with any conforming {@link org.opensearch.index.engine.dataformat.IndexingExecutionEngine}.
 */
public class MockEngineDataFormatAwareEngineTests extends AbstractDataFormatAwareEngineTestCase {

    private static final MockDataFormat MOCK_FORMAT = new MockDataFormat("composite", 100L, Set.of());

    @Override
    protected DataFormatPlugin createDataFormatPlugin() {
        return MockDataFormatPlugin.of(MOCK_FORMAT);
    }

    @Override
    protected SearchBackEndPlugin<?> createSearchBackEndPlugin() {
        return new MockSearchBackEndPlugin(List.of(MOCK_FORMAT.name()));
    }

    @Override
    protected String dataFormatName() {
        return MOCK_FORMAT.name();
    }
}
