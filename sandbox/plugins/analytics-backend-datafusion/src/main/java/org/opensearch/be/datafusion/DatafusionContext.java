/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.jni.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.IndexFilterTree;
import org.opensearch.search.SearchExecutionContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;

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
public class DatafusionContext implements SearchExecutionContext {

    private final ShardSearchRequest request;
    private final SearchShardTarget shardTarget;
    private final DatafusionSearcher engineSearcher;
    private final NativeRuntimeHandle nativeRuntime;
    private DatafusionQuery datafusionQuery;
    private IndexFilterTree filterTree;
    private StreamHandle streamHandle;

    public DatafusionContext(
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        DatafusionReader reader,
        NativeRuntimeHandle nativeRuntime
    ) throws IOException {
        this.request = request;
        this.shardTarget = shardTarget;
        this.engineSearcher = new DatafusionSearcher(reader.getReaderHandle());
        this.nativeRuntime = nativeRuntime;
    }

    @Override
    public ShardSearchRequest request() {
        return request;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return shardTarget;
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

    // DataFusion-specific

    public DatafusionSearcher getEngineSearcher() {
        return engineSearcher;
    }

    /**
     * Returns the live native runtime pointer for JNI calls.
     */
    public long getRuntimePtr() {
        return nativeRuntime.get();
    }

    public DatafusionQuery getDatafusionQuery() {
        return datafusionQuery;
    }

    public void setDatafusionQuery(DatafusionQuery query) {
        this.datafusionQuery = query;
    }

    public IndexFilterTree getFilterTree() {
        return filterTree;
    }

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
     */
    public void setStreamHandle(StreamHandle streamHandle) {
        this.streamHandle = streamHandle;
    }
}
