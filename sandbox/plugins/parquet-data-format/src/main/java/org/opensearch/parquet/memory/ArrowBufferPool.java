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
import org.opensearch.common.settings.Settings;

import java.io.Closeable;

/**
 * Arrow memory allocator pool for Parquet ingest operations.
 *
 * <p>Uses the "ingest" pool from the unified {@link ArrowNativeAllocator}.
 * Child allocators are created per {@link org.opensearch.parquet.vsr.ManagedVSR} instance,
 * each allowed to use the full pool limit. Memory isolation is provided by the
 * pool-level cap in {@link ArrowNativeAllocator}, and Arrow's own accounting will
 * fail allocations that exceed the pool's remaining capacity.
 */
public class ArrowBufferPool implements Closeable {

    private static final Logger logger = LogManager.getLogger(ArrowBufferPool.class);

    private final BufferAllocator poolAllocator;

    /**
     * Creates a new ArrowBufferPool backed by the unified native allocator's ingest pool.
     *
     * @param settings        node settings (reserved for future use)
     * @param nativeAllocator the framework's unified native allocator, injected by
     *                        {@code ParquetDataFormatPlugin#createComponents}
     */
    public ArrowBufferPool(Settings settings, ArrowNativeAllocator nativeAllocator) {
        this.poolAllocator = nativeAllocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_INGEST);
        logger.debug("ArrowBufferPool: poolLimit={}", poolAllocator.getLimit());
    }

    /**
     * Creates a child allocator with the given name. The child is allowed to use the
     * full pool limit. Concurrent child allocators naturally share the pool — Arrow's
     * accounting will fail allocations that exceed the pool's remaining capacity.
     *
     * @param name the allocator name
     * @return a new child buffer allocator
     */
    public BufferAllocator createChildAllocator(String name) {
        return poolAllocator.newChildAllocator(name, 0, poolAllocator.getLimit());
    }

    /** Returns the total bytes currently allocated by the ingest pool. */
    public long getTotalAllocatedBytes() {
        return poolAllocator.getAllocatedMemory();
    }

    @Override
    public void close() {
        // The framework owns the ingest pool's BufferAllocator; nothing to free here.
        // Child allocators created via createChildAllocator are owned by their callers
        // (VSRPool / ManagedVSR) and closed when those resources release.
    }
}
