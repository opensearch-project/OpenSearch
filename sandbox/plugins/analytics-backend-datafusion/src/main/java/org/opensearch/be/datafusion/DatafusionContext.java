/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.be.datafusion.jni.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.IndexFilterTree;
import org.opensearch.search.SearchExecutionContext;

import java.io.IOException;

/**
 * DataFusion-specific search execution context.
 * <p>
 * Carries the DataFusion query plan, engine searcher, optional {@link IndexFilterTree},
 * and the native result stream handle after execution.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionContext implements SearchExecutionContext<DatafusionSearcher> {

    private final DatafusionSearcher engineSearcher;
    private final NativeRuntimeHandle nativeRuntime;
    private DatafusionQuery datafusionQuery;
    private IndexFilterTree filterTree;
    private StreamHandle streamHandle;
    private SearchShardTask task;

    /**
     * Creates a DataFusion execution context
     * @param task the search shard task
     * @param reader the DataFusion reader providing index data
     * @param nativeRuntime handle to the native DataFusion runtime
     */
    public DatafusionContext(SearchShardTask task, DatafusionReader reader, NativeRuntimeHandle nativeRuntime) {
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
            try {
                if (filterTree != null) {
                    filterTree.close();
                }
            } finally {
                engineSearcher.close();
            }
        }
    }

    /**
     * Returns the live native runtime pointer for JNI calls.
     */
    public long getRuntimePtr() {
        return nativeRuntime.get();
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

    /** Returns the index filter tree, or {@code null} if not set. */
    public IndexFilterTree getFilterTree() {
        return filterTree;
    }

    /**
     * Sets the index filter tree for indexed query execution.
     * @param filterTree the index filter tree
     */
    public void setFilterTree(IndexFilterTree filterTree) {
        this.filterTree = filterTree;
    }

    /**
     * Returns the native result stream handle, or {@code null} if execution has not completed.
     */
    public StreamHandle getStreamHandle() {
        return streamHandle;
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
    public SearchShardTask task() {
        return task;
    }

    @Override
    public DatafusionSearcher getSearcher() {
        return engineSearcher;
    }
}
