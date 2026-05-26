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
import org.opensearch.parquet.ParquetSettings;

import java.io.Closeable;
import java.util.function.IntSupplier;

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
    private final IntSupplier divisorSupplier;

    /**
     * Creates a new ArrowBufferPool backed by the unified native allocator's ingest pool.
     *
     * @param settings        node settings (used by the {@link #ArrowBufferPool(Settings, ArrowNativeAllocator)}
     *                        convenience ctor to derive a static divisor supplier; ignored
     *                        here)
     * @param divisorSupplier supplies the current value of
     *                        {@link ParquetSettings#MAX_PER_VSR_ALLOCATION_DIVISOR}; read on
     *                        every {@link #createChildAllocator(String)} call so dynamic
     *                        cluster-settings updates take effect for new child allocators
     * @param nativeAllocator the framework's unified native allocator, injected by
     *                        {@code ParquetDataFormatPlugin#createComponents}
     */
    public ArrowBufferPool(Settings settings, IntSupplier divisorSupplier, ArrowNativeAllocator nativeAllocator) {
        this.poolAllocator = nativeAllocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_INGEST);
        this.divisorSupplier = divisorSupplier;
        logger.debug("ArrowBufferPool: poolLimit={}", poolAllocator.getLimit());
    }

    /**
     * Test-only convenience constructor that derives the divisor statically from
     * {@code settings}. Production callers go through
     * {@link #ArrowBufferPool(Settings, IntSupplier, ArrowNativeAllocator)} so the divisor
     * follows dynamic cluster-settings updates.
     *
     * @param settings node settings
     * @param nativeAllocator the framework's unified native allocator (typically a fixture)
     */
    public ArrowBufferPool(Settings settings, ArrowNativeAllocator nativeAllocator) {
        this(settings, () -> ParquetSettings.MAX_PER_VSR_ALLOCATION_DIVISOR.get(settings), nativeAllocator);
    }

    /**
     * Creates a child allocator with the given name. The cap is computed lazily from the
     * pool's current limit and the latest divisor, so cluster-settings updates to
     * {@code parquet.max_per_vsr_allocation_divisor} are picked up here.
     *
     * @param name the allocator name
     * @return a new child buffer allocator
     */
    public BufferAllocator createChildAllocator(String name) {
        long limit = poolAllocator.getLimit();
        int divisor = divisorSupplier.getAsInt();
        long maxChildAllocation = limit == Long.MAX_VALUE ? Long.MAX_VALUE : limit / divisor;
        return poolAllocator.newChildAllocator(name, 0, maxChildAllocation);
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
