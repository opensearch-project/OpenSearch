/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.core.action.ActionListener;
import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.search.SearchExecutionContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Search context that holds a native reader pointer and result stream pointer.
 * Calls {@link NativeBridge} directly for query/fetch execution.
 */
public class DatafusionContext implements SearchExecutionContext {

    private final ShardSearchRequest request;
    private final SearchShardTarget shardTarget;
    private final long readerPtr;
    private final long runtimePtr;
    private final long contextId;

    private byte[] substraitPlan;
    private String tableName = "";
    private long streamPtr;

    public DatafusionContext(ShardSearchRequest request, SearchShardTarget shardTarget, long readerPtr, long runtimePtr, long contextId) {
        this.request = request;
        this.shardTarget = shardTarget;
        this.readerPtr = readerPtr;
        this.runtimePtr = runtimePtr;
        this.contextId = contextId;
    }

    @Override
    public ShardSearchRequest request() {
        return request;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    public void setSubstraitPlan(byte[] plan) {
        this.substraitPlan = plan;
    }

    public byte[] getSubstraitPlan() {
        return substraitPlan;
    }

    public long getReaderPtr() {
        return readerPtr;
    }

    public long getRuntimePtr() {
        return runtimePtr;
    }

    public long getStreamPtr() {
        return streamPtr;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Executes the substrait plan via JNI and stores the result stream pointer.
     */
    public void executeQuery() throws IOException {
        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeQueryPhaseAsync(
            readerPtr, tableName, substraitPlan, false, 1, runtimePtr, contextId,
            new ActionListener<Long>() {
                @Override
                public void onResponse(Long ptr) {
                    future.complete(ptr);
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            }
        );
        try {
            this.streamPtr = future.join();
        } catch (Exception e) {
            throw new IOException("Query execution failed", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (streamPtr != 0) {
            NativeBridge.streamClose(streamPtr);
            streamPtr = 0;
        }
    }
}
