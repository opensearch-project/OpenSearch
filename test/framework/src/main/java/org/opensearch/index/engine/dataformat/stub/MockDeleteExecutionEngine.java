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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A mock {@link DeleteExecutionEngine} for testing purposes.
 */
public class MockDeleteExecutionEngine implements DeleteExecutionEngine<DataFormat> {

    private final DataFormat dataFormat;
    private final Map<Long, Deleter> deleters = new ConcurrentHashMap<>();
    private final List<String> deletedIds = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> checkedOutGenerations = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean deletesAppliedOnCheckout = false;

    /** If true, {@link #onWriterCheckedOut} reports that buffered deletes were applied. */
    public void setDeletesAppliedOnCheckout(boolean applied) {
        this.deletesAppliedOnCheckout = applied;
    }

    public MockDeleteExecutionEngine(DataFormat dataFormat) {
        this.dataFormat = dataFormat;
    }

    /** Ids passed to {@link #deleteDocument}, in call order; retained after {@link #close()}. */
    public List<String> deletedIds() {
        return deletedIds;
    }

    /** Generations passed to {@link #onWriterCheckedOut}, in call order, including repeats. */
    public List<Long> checkedOutGenerations() {
        return checkedOutGenerations;
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
    public void recordWrite(String id, long generation, long rowId) {

    }

    @Override
    public boolean onWriterCheckedOut(long generation) throws IOException {
        checkedOutGenerations.add(generation);
        return deletesAppliedOnCheckout;
    }

    @Override
    public DeleteResult deleteDocument(DeleteInput deleteInput, Writer<?> writer) throws IOException {
        deletedIds.add(deleteInput.id());
        Deleter deleter = deleters.get(deleteInput.generation());
        if (deleter != null) {
            // Mirror the real engine: the live path buffers the id for the parent writer.
            deleter.recordBufferedDeletes(deleteInput.id());
        }
        return new DeleteResult.Success(1L, 1L, 1L);
    }

    @Override
    public void close() throws IOException {
        deleters.clear();
    }

    private static class MockDeleter implements Deleter {
        private final long generation;
        private final Queue<String> bufferedDeletes = new ConcurrentLinkedQueue<>();
        private volatile boolean active = true;

        MockDeleter(long generation) {
            this.generation = generation;
        }

        @Override
        public long generation() {
            return generation;
        }

        @Override
        public Queue<String> deactivate() {
            active = false;
            Queue<String> drained = new ConcurrentLinkedQueue<>(bufferedDeletes);
            bufferedDeletes.clear();
            return drained;
        }

        @Override
        public boolean recordBufferedDeletes(String id) {
            bufferedDeletes.add(id);
            return true;
        }

        @Override
        public boolean isActive() {
            return active;
        }

        @Override
        public void recordPositionalDelete(long rowId) {
            // No-op: the mock has no writer to forward row-id deletes to.
        }

        @Override
        public void close() throws IOException {}
    }
}
