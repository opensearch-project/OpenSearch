/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.memory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.parquet.ParquetSettings;

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
    private final boolean ownsAllocator;

    /**
     * Creates a new ArrowBufferPool backed by the unified native allocator's ingest pool.
     */
    public ArrowBufferPool() {
        BufferAllocator alloc;
        boolean owns = false;
        try {
            alloc = ArrowNativeAllocator.instance().getPoolAllocator(NativeAllocatorPoolConfig.POOL_INGEST);
        } catch (IllegalStateException e) {
            alloc = new RootAllocator(Long.MAX_VALUE);
            owns = true;
            logger.warn("NativeAllocator not available, using standalone RootAllocator");
        }
        this.poolAllocator = alloc;
        this.ownsAllocator = owns;
        long limit = poolAllocator.getLimit();
        this.maxChildAllocation = limit == Long.MAX_VALUE ? Long.MAX_VALUE : limit / 10;
        logger.debug("ArrowBufferPool: limit={}, maxChildAllocation={}", limit, maxChildAllocation);
    }

    /**
     * Creates a new ArrowBufferPool with a standalone allocator (legacy/test path).
     *
     * @param settings node settings used to derive the maximum native allocation
     */
    public ArrowBufferPool(Settings settings) {
        long maxAllocationInBytes = getMaxAllocationInBytes(settings);
        this.poolAllocator = new RootAllocator(maxAllocationInBytes);
        this.ownsAllocator = true;
        this.maxChildAllocation = maxAllocationInBytes / 10;
        logger.debug("ArrowBufferPool standalone: limit={}, maxChildAllocation={}", maxAllocationInBytes, maxChildAllocation);
    }

    /**
     * Creates a child allocator with the given name.
     *
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
        if (ownsAllocator) {
            poolAllocator.close();
        }
    }

    private static long getMaxAllocationInBytes(Settings settings) {
        long totalAvailableMemory = OsProbe.getInstance().getTotalPhysicalMemorySize() - JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        RatioValue ratio = RatioValue.parseRatioValue(ParquetSettings.MAX_NATIVE_ALLOCATION.get(settings));
        return (long) (totalAvailableMemory * ratio.getAsRatio());
    }
}
