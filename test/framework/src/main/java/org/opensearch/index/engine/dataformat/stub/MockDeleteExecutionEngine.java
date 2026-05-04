/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DeleteExecutionEngine;
import org.opensearch.index.engine.dataformat.Deleter;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A no-op {@link DeleteExecutionEngine} for testing purposes.
 */
public class MockDeleteExecutionEngine implements DeleteExecutionEngine<DataFormat> {

    private final Map<Writer<?>, Deleter> deleters = new ConcurrentHashMap<>();

    @Override
    public Deleter createDeleter(Writer<?> writer) {
        MockDeleter deleter = new MockDeleter();
        deleters.put(writer, deleter);
        return deleter;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) {
        return new RefreshResult(List.of());
    }

    @Override
    public DataFormat getDataFormat() {
        return new MockDataFormat("mock-delete", 0L, java.util.Set.of());
    }

    public Deleter getDeleter(Writer<?> writer) {
        return deleters.get(writer);
    }

    @Override
    public void close() {}
}
