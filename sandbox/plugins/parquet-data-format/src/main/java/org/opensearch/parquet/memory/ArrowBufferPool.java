/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.memory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;

import java.io.Closeable;

/**
 * Arrow memory allocator pool for Parquet ingest operations.
 *
 * <p>Uses the "ingest" pool from the unified {@link ArrowNativeAllocator}.
 * Child allocators are created per {@link org.opensearch.parquet.vsr.ManagedVSR} instance,
 * each limited to 1/10th of the pool's configured limit.
 */
public class ArrowBufferPool implements Closeable {

    private static final Logger logger = LogManager.getLogger(ArrowBufferPool.class);

    private final BufferAllocator poolAllocator;
    private final long maxChildAllocation;

    /**
     * Creates a new ArrowBufferPool backed by the unified native allocator's ingest pool.
     */
    public ArrowBufferPool() {
        this.poolAllocator = ArrowNativeAllocator.instance().getPoolAllocator(NativeAllocatorPoolConfig.POOL_INGEST);
        this.maxChildAllocation = poolAllocator.getLimit() / 10;
        logger.debug("ArrowBufferPool using ingest pool: limit={}, maxChildAllocation={}", poolAllocator.getLimit(), maxChildAllocation);
    }

    /**
     * Creates a child allocator with the given name.
     * @param name the allocator name
     * @return a new child buffer allocator
     */
    public BufferAllocator createChildAllocator(String name) {
        return poolAllocator.newChildAllocator(name, 0, maxChildAllocation);
    }

    /** Returns the total bytes currently allocated by the ingest pool. */
    public long getTotalAllocatedBytes() {
        return poolAllocator.getAllocatedMemory();
    }

    @Override
    public void close() {
        // Pool allocator is owned by ArrowNativeAllocator — don't close it here.
    }
}
