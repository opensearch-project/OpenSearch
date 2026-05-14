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
import org.opensearch.index.engine.dataformat.DeleteInput;
import org.opensearch.index.engine.dataformat.DeleteResult;
import org.opensearch.index.engine.dataformat.Deleter;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.Segment;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A mock {@link DeleteExecutionEngine} for testing purposes.
 */
public class MockDeleteExecutionEngine implements DeleteExecutionEngine<DataFormat> {

    private final DataFormat dataFormat;
    private final Map<Long, Deleter> deleters = new ConcurrentHashMap<>();

    public MockDeleteExecutionEngine(DataFormat dataFormat) {
        this.dataFormat = dataFormat;
    }

    @Override
    public Deleter createDeleter(Writer<?> writer) {
        Deleter deleter = new MockDeleter(writer.generation());
        deleters.put(writer.generation(), deleter);
        return deleter;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        return null;
    }

    @Override
    public DataFormat getDataFormat() {
        return dataFormat;
    }

    @Override
    public DeleteResult deleteDocument(DeleteInput deleteInput) throws IOException {
        Deleter deleter = deleters.get(deleteInput.generation());
        if (deleter != null) {
            return deleter.deleteDoc(deleteInput);
        }
        return new DeleteResult.Success(1L, 1L, 1L);
    }

    @Override
    public Map<Long, long[]> getLiveDocsForSegments(List<Segment> segments) {
        return Map.of();
    }

    @Override
    public void close() throws IOException {
        deleters.clear();
    }

    private static class MockDeleter implements Deleter {
        private final long generation;

        MockDeleter(long generation) {
            this.generation = generation;
        }

        @Override
        public long generation() {
            return generation;
        }

        @Override
        public DeleteResult deleteDoc(DeleteInput deleteInput) throws IOException {
            return new DeleteResult.Success(1L, 1L, 1L);
        }

        @Override
        public void lock() {}

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public void unlock() {}

        @Override
        public void close() throws IOException {}
    }
}
