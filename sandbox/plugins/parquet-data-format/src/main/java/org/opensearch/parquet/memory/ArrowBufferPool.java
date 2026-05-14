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
 * each capped by {@link ParquetSettings#CHILD_ALLOCATION} to provide memory isolation between
 * batches. The child cap is reduced to the root allocation when the configured value exceeds it.
 *
 * <p>Limits are runtime-mutable: {@link #applyLimits(Settings)} re-resolves the root and per-child
 * caps and pushes the new values to the live root allocator and each existing child allocator via
 * {@link BufferAllocator#setLimit(long)}. Lowering a cap below an allocator's currently-reserved
 * memory does not reclaim memory; it only rejects future allocations until usage drains.
 * {@code ParquetDataFormatPlugin} owns the cluster-settings listeners that drive this method.
 */
public class ArrowBufferPool implements Closeable {

    private static final Logger logger = LogManager.getLogger(ArrowBufferPool.class);

    private final RootAllocator rootAllocator;
    private volatile long maxChildAllocation;

    /**
     * Creates a new ArrowBufferPool.
     *
     * @param settings node settings used to derive the maximum native allocation
     */
    public ArrowBufferPool(Settings settings) {
        long maxAllocationInBytes = resolveMaxAllocationBytes(settings);
        long childAllocation = resolveChildAllocationBytes(settings, maxAllocationInBytes);
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
     * Re-reads the two allocation settings ({@link ParquetSettings#MAX_NATIVE_ALLOCATION},
     * {@link ParquetSettings#CHILD_ALLOCATION}), pushes the resolved root limit to the live
     * {@link RootAllocator} and pushes the new per-child cap to every live child allocator. New
     * children created after this call also see the new cap via the volatile field.
     */
    public void applyLimits(Settings settings) {
        long newRoot = resolveMaxAllocationBytes(settings);
        long newChild = resolveChildAllocationBytes(settings, newRoot);
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

    /**
     * Resolves the per-child allocator cap, capping at the root allocation when the configured
     * value exceeds it.
     */
    static long resolveChildAllocationBytes(Settings settings, long rootBytes) {
        long childAllocation = ParquetSettings.CHILD_ALLOCATION.get(settings).getBytes();
        if (childAllocation > rootBytes) {
            logger.warn(
                "[{}] ({}) is larger than [{}] ({}); capping child allocation to root allocation",
                ParquetSettings.CHILD_ALLOCATION.getKey(),
                new ByteSizeValue(childAllocation),
                ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(),
                new ByteSizeValue(rootBytes)
            );
            childAllocation = rootBytes;
        }
        return childAllocation;
    }
}
