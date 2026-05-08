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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.parquet.ParquetSettings;

import java.io.Closeable;

/**
 * Arrow memory allocator pool with configurable limits derived from node settings.
 *
 * <p>Wraps an Apache Arrow {@link RootAllocator} whose maximum allocation is derived from the
 * {@link ParquetSettings#MAX_NATIVE_ALLOCATION} setting. Percentages are applied against
 * {@code totalPhysicalMemory - configuredMaxHeap} and clamped by
 * {@link ParquetSettings#MIN_NATIVE_ALLOCATION} / {@link ParquetSettings#MAX_NATIVE_ALLOCATION_CEILING};
 * absolute byte sizes are used as-is.
 *
 * <p>Child allocators are created per {@link org.opensearch.parquet.vsr.ManagedVSR} instance,
 * each capped by {@link ParquetSettings#CHILD_ALLOCATION} to provide memory isolation between
 * batches. The child cap is reduced to the root allocation when the configured value exceeds it.
 */
public class ArrowBufferPool implements Closeable {

    private static final Logger logger = LogManager.getLogger(ArrowBufferPool.class);

    private final RootAllocator rootAllocator;
    private final long maxChildAllocation;

    /**
     * Creates a new ArrowBufferPool.
     *
     * @param settings node settings used to derive the maximum native allocation
     */
    public ArrowBufferPool(Settings settings) {
        long maxAllocationInBytes = resolveMaxAllocationBytes(settings);
        long childAllocation = ParquetSettings.CHILD_ALLOCATION.get(settings).getBytes();
        if (childAllocation > maxAllocationInBytes) {
            logger.warn(
                "[{}] ({}) is larger than [{}] ({}); capping child allocation to root allocation",
                ParquetSettings.CHILD_ALLOCATION.getKey(),
                new ByteSizeValue(childAllocation),
                ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(),
                new ByteSizeValue(maxAllocationInBytes)
            );
            childAllocation = maxAllocationInBytes;
        }
        logger.debug("ArrowBufferPool sized: root={} bytes, perChild={} bytes", maxAllocationInBytes, childAllocation);
        this.rootAllocator = new RootAllocator(maxAllocationInBytes);
        this.maxChildAllocation = childAllocation;
    }

    /**
     * Creates a child allocator with the given name.
     * @param name the allocator name
     * @return a new child buffer allocator
     */
    public BufferAllocator createChildAllocator(String name) {
        return rootAllocator.newChildAllocator(name, 0, maxChildAllocation);
    }

    /** Returns the total bytes currently allocated by the root allocator. */
    public long getTotalAllocatedBytes() {
        return rootAllocator.getAllocatedMemory();
    }

    @Override
    public void close() {
        rootAllocator.close();
    }

    /**
     * Resolves the root allocator budget. Mirrors {@code IndexingMemoryController}'s native
     * indexing buffer logic: percentages apply to {@code totalPhysicalMemory - configuredMaxHeap}
     * and are clamped by {@link ParquetSettings#MIN_NATIVE_ALLOCATION} /
     * {@link ParquetSettings#MAX_NATIVE_ALLOCATION_CEILING}; absolute byte sizes are used as-is.
     */
    static long resolveMaxAllocationBytes(Settings settings) {
        String configured = ParquetSettings.MAX_NATIVE_ALLOCATION.get(settings);

        if (configured.endsWith("%")) {
            long totalAvailableMemory = OsProbe.getInstance().getTotalPhysicalMemorySize() - JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
            ByteSizeValue floor = ParquetSettings.MIN_NATIVE_ALLOCATION.get(settings);
            if (totalAvailableMemory <= 0) {
                logger.warn(
                    "Non-heap memory not measurable; falling back to [{}] = {}",
                    ParquetSettings.MIN_NATIVE_ALLOCATION.getKey(),
                    floor
                );
                return floor.getBytes();
            }

            RatioValue ratio = RatioValue.parseRatioValue(configured);
            long bytes = (long) (totalAvailableMemory * ratio.getAsRatio());

            ByteSizeValue ceiling = ParquetSettings.MAX_NATIVE_ALLOCATION_CEILING.get(settings);
            if (bytes < floor.getBytes()) {
                bytes = floor.getBytes();
            }
            if (ceiling.getBytes() != -1 && bytes > ceiling.getBytes()) {
                bytes = ceiling.getBytes();
            }
            return bytes;
        }

        return ByteSizeValue.parseBytesSizeValue(configured, ParquetSettings.MAX_NATIVE_ALLOCATION.getKey()).getBytes();
    }
}
