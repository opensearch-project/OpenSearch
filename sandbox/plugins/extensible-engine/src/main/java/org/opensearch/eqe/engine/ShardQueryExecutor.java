/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.engine;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.index.shard.IndexShard;

/**
 * Data-node-side executor that runs a plan fragment using the {@link ExecutionEngine}.
 *
 * <p>Thin delegation wrapper for now. Will grow to acquire IndexSearcher,
 * manage partition routing, and handle resource lifecycle.
 */
public class ShardQueryExecutor {

    private final ExecutionEngine nativeExecutor;
    private final BufferAllocator allocator;

    public ShardQueryExecutor(ExecutionEngine nativeExecutor, BufferAllocator allocator) {
        this.nativeExecutor = nativeExecutor;
        this.allocator = allocator;
    }

    /**
     * Execute a plan fragment against a specific shard and return Arrow results.
     *
     * @param planBytes  serialized plan fragment bytes
     * @param indexShard the index shard to execute against
     * @return query results as Arrow VectorSchemaRoot
     * @throws IllegalStateException if no NativeEngineExecutor is available
     */
    public VectorSchemaRoot execute(byte[] planBytes, IndexShard indexShard) {
        if (nativeExecutor == null) {
            throw new IllegalStateException("No NativeEngineExecutor available");
        }
        return nativeExecutor.execute(planBytes, allocator);
    }

    /** @return true if a NativeEngineExecutor is loaded */
    public boolean isAvailable() {
        return nativeExecutor != null;
    }
}
