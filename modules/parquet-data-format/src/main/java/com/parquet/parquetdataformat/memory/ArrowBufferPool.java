package com.parquet.parquetdataformat.memory;

import com.parquet.parquetdataformat.ParquetDataFormatPlugin;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;

import java.io.Closeable;

/**
 * Manages BufferAllocator lifecycle with configurable allocation strategies.
 * Provides factory methods for creating allocators with different policies
 * based on OpenSearch settings and memory pressure conditions.
 */
public class ArrowBufferPool implements Closeable {

    private static final Logger logger = LogManager.getLogger(ArrowBufferPool.class);

    private final RootAllocator rootAllocator;
    private final long maxChildAllocation;

    public ArrowBufferPool(Settings settings) {
        long maxAllocationInBytes = 10L * 1024 * 1024 * 1024;

        logger.info("Max native memory allocation for ArrowBufferPool: {} bytes", maxAllocationInBytes);
        this.rootAllocator = new RootAllocator(maxAllocationInBytes);
        this.maxChildAllocation = 1024 * 1024 * 1024;
    }

    /**
     * Creates a new child allocator with the configured strategy and limits.
     *
     * @param name Unique name for the allocator
     * @return BufferAllocator configured with pool settings
     */
    public BufferAllocator createChildAllocator(String name) {
        return createChildAllocator(name, maxChildAllocation);
    }

    /**
     * Creates a new child allocator with custom limits.
     *
     * @param name     Unique name for the allocator
     * @param maxAllocation Maximum allocation limit
     * @return BufferAllocator configured with specified limits
     */
    private BufferAllocator createChildAllocator(String name, long maxAllocation) {
        return rootAllocator.newChildAllocator(name, 0, maxAllocation);
    }

    public long getTotalAllocatedBytes() {
        return rootAllocator.getAllocatedMemory();
    }

    /**
     * Closes all active allocators and cleans up the pool.
     */
    @Override
    public void close() {
        rootAllocator.close();
    }

    private static long getMaxAllocationInBytes(Settings settings) {
        long totalAvailableSystemMemory = OsProbe.getInstance().getTotalPhysicalMemorySize() - JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        RatioValue maxAllocationPercentage = RatioValue.parseRatioValue(settings.get(ParquetDataFormatPlugin.INDEX_MAX_NATIVE_ALLOCATION.getKey(), ParquetDataFormatPlugin.DEFAULT_MAX_NATIVE_ALLOCATION));
        return (long) (totalAvailableSystemMemory * maxAllocationPercentage.getAsRatio());
    }
}
