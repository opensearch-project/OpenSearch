/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DeleteDataFormatPlugin;
import org.opensearch.index.engine.dataformat.DeleteExecutionEngine;
import org.opensearch.plugins.Plugin;

/**
 * A mock {@link DeleteDataFormatPlugin} for testing purposes.
 */
public class MockDeleteDataFormatPlugin extends Plugin implements DeleteDataFormatPlugin {

    @Override
    public DataFormat getDataFormat() {
        return new MockDataFormat("mock-delete", 0L, java.util.Set.of());
    }

    @Override
    public DeleteExecutionEngine<?> deleteEngine() {
        return new MockDeleteExecutionEngine();
    }
}
