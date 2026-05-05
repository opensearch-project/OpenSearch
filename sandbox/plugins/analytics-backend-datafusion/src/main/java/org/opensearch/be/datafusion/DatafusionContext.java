/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.SearchExecutionContext;
import org.opensearch.tasks.Task;

import java.io.IOException;

/**
 * DataFusion-specific search execution context.
 * <p>
 * Carries the DataFusion query plan, engine searcher, and the native result
 * stream handle after execution.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionContext implements SearchExecutionContext<DatafusionSearcher> {

    private final DatafusionSearcher engineSearcher;
    private final NativeRuntimeHandle nativeRuntime;
    private DatafusionQuery datafusionQuery;
    private StreamHandle streamHandle;
    private Task task;

    /**
     * Creates a DataFusion execution context
     * @param task the search shard task
     * @param reader the DataFusion reader providing index data
     * @param nativeRuntime handle to the native DataFusion runtime
     */
    public DatafusionContext(Task task, DatafusionReader reader, NativeRuntimeHandle nativeRuntime) {
        this.task = task;
        this.engineSearcher = new DatafusionSearcher(reader.getReaderHandle());
        this.nativeRuntime = nativeRuntime;
    }

    @Override
    public void close() throws IOException {
        try {
            if (streamHandle != null) {
                streamHandle.close();
                streamHandle = null;
            }
        } finally {
            engineSearcher.close();
        }
    }

    /**
     * Returns the native runtime handle.
     */
    public NativeRuntimeHandle getNativeRuntime() {
        return nativeRuntime;
    }

    /** Returns the DataFusion query plan. */
    public DatafusionQuery getDatafusionQuery() {
        return datafusionQuery;
    }

    /**
     * Sets the DataFusion query plan.
     * @param query the DataFusion query to set
     */
    public void setDatafusionQuery(DatafusionQuery query) {
        this.datafusionQuery = query;
    }

    /** Returns the native result stream handle, or {@code null} if execution has not completed. */
    public StreamHandle getStreamHandle() {
        return streamHandle;
    }

    /**
     * Takes ownership of the stream handle, returning it and clearing the reference.
     * After this call, {@link #close()} will not close the stream handle.
     */
    public StreamHandle takeStreamHandle() {
        StreamHandle handle = streamHandle;
        streamHandle = null;
        return handle;
    }

    /**
     * Sets the native result stream handle after query execution.
     *
     * @param streamHandle the native result stream handle
     */
    public void setStreamHandle(StreamHandle streamHandle) {
        this.streamHandle = streamHandle;
    }

    @Override
    public Task task() {
        return task;
    }

    @Override
    public DatafusionSearcher getSearcher() {
        return engineSearcher;
    }
}
