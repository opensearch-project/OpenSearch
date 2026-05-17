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
 * {@code totalPhysicalMemory - configuredMaxHeap}; absolute byte sizes are used as-is.
 *
 * <p>Child allocators are created per {@link org.opensearch.parquet.vsr.ManagedVSR} instance,
 * each fixed at one tenth of the root allocation to provide memory isolation between batches.
 *
 * <p>Limits are runtime-mutable: {@link #applyLimits(Settings)} re-resolves the root cap and
 * pushes the new value to the live root allocator and each existing child allocator via
 * {@link BufferAllocator#setLimit(long)}. The per-child cap is recomputed as {@code root / 10}.
 * Lowering a cap below an allocator's currently-reserved memory does not reclaim memory; it
 * only rejects future allocations until usage drains. {@code ParquetDataFormatPlugin} owns the
 * cluster-settings listener that drives this method.
 */
public class ArrowBufferPool implements Closeable {

    private static final Logger logger = LogManager.getLogger(ArrowBufferPool.class);

    /** Per-child allocator cap as a fraction of the root allocation. */
    private static final int CHILD_TO_ROOT_RATIO = 10;

    private final RootAllocator rootAllocator;
    private volatile long maxChildAllocation;

    /**
     * Creates a new ArrowBufferPool.
     *
     * @param settings node settings used to derive the maximum native allocation
     */
    public ArrowBufferPool(Settings settings) {
        long maxAllocationInBytes = resolveMaxAllocationBytes(settings);
        long childAllocation = maxAllocationInBytes / CHILD_TO_ROOT_RATIO;
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

    /** Returns the current root allocator memory limit in bytes. Visible for tests. */
    public long getRootLimit() {
        return rootAllocator.getLimit();
    }

    /** Returns the current per-child allocator cap in bytes. Visible for tests. */
    public long getMaxChildAllocation() {
        return maxChildAllocation;
    }

    /**
     * Re-reads {@link ParquetSettings#MAX_NATIVE_ALLOCATION}, pushes the resolved root limit to
     * the live {@link RootAllocator} and recomputes the per-child cap as {@code root / 10}. New
     * children created after this call also see the new cap via the volatile field.
     */
    public void applyLimits(Settings settings) {
        long newRoot = resolveMaxAllocationBytes(settings);
        long newChild = newRoot / CHILD_TO_ROOT_RATIO;
        long previousRoot = rootAllocator.getLimit();
        long previousChild = this.maxChildAllocation;
        if (newRoot == previousRoot && newChild == previousChild) {
            return;
        }
        rootAllocator.setLimit(newRoot);
        this.maxChildAllocation = newChild;
        // Update live children. setLimit doesn't reclaim memory below current usage; it just
        // rejects future allocations until the child drains.
        for (BufferAllocator child : rootAllocator.getChildAllocators()) {
            child.setLimit(newChild);
        }
        logger.info("ArrowBufferPool limits updated: root {}B -> {}B, perChild {}B -> {}B", previousRoot, newRoot, previousChild, newChild);
    }

    @Override
    public void close() {
        rootAllocator.close();
    }

    /**
     * Resolves the root allocator budget. Percentages apply to
     * {@code totalPhysicalMemory - configuredMaxHeap}; absolute byte sizes are used as-is.
     * <p>
     * Throws {@link IllegalStateException} if a percentage is supplied but non-heap memory is
     * unmeasurable — operators on misconfigured boxes should specify an absolute byte size.
     */
    static long resolveMaxAllocationBytes(Settings settings) {
        String configured = ParquetSettings.MAX_NATIVE_ALLOCATION.get(settings);

        if (configured.endsWith("%")) {
            long totalAvailableMemory = OsProbe.getInstance().getTotalPhysicalMemorySize() - JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
            if (totalAvailableMemory <= 0) {
                throw new IllegalStateException(
                    "Non-heap memory not measurable for setting ["
                        + ParquetSettings.MAX_NATIVE_ALLOCATION.getKey()
                        + "]; configure an absolute byte size (e.g. \"2gb\") instead of a percentage."
                );
            }

            RatioValue ratio = RatioValue.parseRatioValue(configured);
            return (long) (totalAvailableMemory * ratio.getAsRatio());
        }

        return ByteSizeValue.parseBytesSizeValue(configured, ParquetSettings.MAX_NATIVE_ALLOCATION.getKey()).getBytes();
    }
}
