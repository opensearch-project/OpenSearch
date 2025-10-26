package com.parquet.parquetdataformat.memory;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.common.settings.Settings;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages BufferAllocator lifecycle with configurable allocation strategies.
 * Provides factory methods for creating allocators with different policies
 * based on OpenSearch settings and memory pressure conditions.
 */
public class ArrowBufferPool {

    private final Settings settings;
    private final long maxAllocation;
    private final long initReservation;
    private final AllocationListener allocationListener;
    private final MemoryPressureMonitor memoryMonitor;

    // Track active allocators for monitoring and cleanup
    private final ConcurrentHashMap<String, BufferAllocator> activeAllocators;
    private final AtomicLong totalAllocated;

    public ArrowBufferPool(Settings settings, MemoryPressureMonitor memoryMonitor) {
        this.settings = settings;
        this.memoryMonitor = memoryMonitor;
        this.activeAllocators = new ConcurrentHashMap<>();
        this.totalAllocated = new AtomicLong(0);

        // Configure memory limits - parse size strings manually
        this.maxAllocation = parseByteSize(settings.get("parquet.memory.max_allocation", "1gb"));
        this.initReservation = parseByteSize(settings.get("parquet.memory.init_reservation", "100mb"));

        // Set up allocation listener for monitoring
        this.allocationListener = new PoolAllocationListener();
    }

    /**
     * Creates a new child allocator with the configured strategy and limits.
     *
     * @param name Unique name for the allocator
     * @return BufferAllocator configured with pool settings
     */
    public BufferAllocator createAllocator(String name) {
        return createAllocator(name, initReservation, maxAllocation);
    }

    /**
     * Creates a new child allocator with custom limits.
     *
     * @param name Unique name for the allocator
     * @param reservation Initial reservation amount
     * @param maxBytes Maximum allocation limit
     * @return BufferAllocator configured with specified limits
     */
    public BufferAllocator createAllocator(String name, long reservation, long maxBytes) {
        // Check memory pressure before creating new allocator
        if (memoryMonitor.shouldRejectAllocation(reservation)) {
            throw new OutOfMemoryError(
                "Cannot create allocator '" + name + "': memory pressure too high");
        }

        BufferAllocator rootAllocator = createRootAllocator();
        //BufferAllocator childAllocator = rootAllocator.newChildAllocator(
        //    name, allocationListener, reservation, maxBytes);

        activeAllocators.put(name, rootAllocator);
        totalAllocated.addAndGet(reservation);

        return rootAllocator;
    }

    /**
     * Releases an allocator and cleans up resources.
     *
     * @param name Name of the allocator to release
     */
    public void releaseAllocator(String name) {
        BufferAllocator allocator = activeAllocators.remove(name);
        if (allocator != null) {
            long allocated = allocator.getAllocatedMemory();
            totalAllocated.addAndGet(-allocated);
            allocator.close();
        }
    }

    /**
     * Gets current memory allocation statistics.
     *
     * @return AllocationStats with current usage information
     */
    public AllocationStats getStats() {
        return new AllocationStats(
            totalAllocated.get(),
            maxAllocation,
            activeAllocators.size(),
            memoryMonitor.getCurrentPressure()
        );
    }

    /**
     * Closes all active allocators and cleans up the pool.
     */
    public void close() {
        activeAllocators.values().forEach(BufferAllocator::close);
        activeAllocators.clear();
        totalAllocated.set(0);
    }

    private BufferAllocator createRootAllocator() {
        // Create a simple RootAllocator with basic settings
        return new RootAllocator(maxAllocation);
    }

    /**
     * Simple byte size parser for configuration strings.
     */
    private long parseByteSize(String sizeStr) {
        if (sizeStr == null || sizeStr.trim().isEmpty()) {
            return 0;
        }

        String trimmed = sizeStr.trim().toLowerCase();
        long multiplier = 1;

        if (trimmed.endsWith("kb")) {
            multiplier = 1024;
            trimmed = trimmed.substring(0, trimmed.length() - 2);
        } else if (trimmed.endsWith("mb")) {
            multiplier = 1024 * 1024;
            trimmed = trimmed.substring(0, trimmed.length() - 2);
        } else if (trimmed.endsWith("gb")) {
            multiplier = 1024 * 1024 * 1024;
            trimmed = trimmed.substring(0, trimmed.length() - 2);
        } else if (trimmed.endsWith("b")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }

        try {
            return Long.parseLong(trimmed.trim()) * multiplier;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid byte size format: " + sizeStr, e);
        }
    }

    /**
     * Allocation listener that integrates with memory monitoring.
     */
    private class PoolAllocationListener implements AllocationListener {

        @Override
        public void onPreAllocation(long size) {
            if (memoryMonitor.shouldRejectAllocation(size)) {
                throw new OutOfMemoryError("Memory pressure too high for allocation of " + size + " bytes");
            }
        }

        @Override
        public void onAllocation(long size) {
            memoryMonitor.recordAllocation(size);
        }

        @Override
        public void onRelease(long size) {
            memoryMonitor.recordDeallocation(size);
        }

        @Override
        public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
            memoryMonitor.recordFailedAllocation(size, "FAILED");
            return false; // Don't retry
        }

        @Override
        public void onChildAdded(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
            // Track child allocator creation
        }

        @Override
        public void onChildRemoved(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
            // Track child allocator removal
        }
    }

    /**
     * Allocation statistics for monitoring.
     */
    public static class AllocationStats {
        private final long totalAllocated;
        private final long maxAllocation;
        private final int activeAllocators;
        private final double memoryPressure;

        public AllocationStats(long totalAllocated, long maxAllocation,
                             int activeAllocators, double memoryPressure) {
            this.totalAllocated = totalAllocated;
            this.maxAllocation = maxAllocation;
            this.activeAllocators = activeAllocators;
            this.memoryPressure = memoryPressure;
        }

        public long getTotalAllocated() { return totalAllocated; }
        public long getMaxAllocation() { return maxAllocation; }
        public int getActiveAllocators() { return activeAllocators; }
        public double getMemoryPressure() { return memoryPressure; }
        public double getUtilizationRatio() {
            return maxAllocation > 0 ? (double) totalAllocated / maxAllocation : 0.0;
        }
    }
}
