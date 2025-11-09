package com.parquet.parquetdataformat.memory;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages BufferAllocator lifecycle with configurable allocation strategies.
 * Provides factory methods for creating allocators with different policies
 * based on OpenSearch settings and memory pressure conditions.
 */
public class ArrowBufferPool {

//    private final Settings settings;
    private final long maxAllocation;
    private final long initReservation;
//    private final AllocationListener allocationListener;

    // Track active allocators for monitoring and cleanup
//    private final ConcurrentHashMap<String, BufferAllocator> activeAllocators;
    private final AtomicLong totalAllocated;

    private final RootAllocator rootAllocator;

    public ArrowBufferPool() {
//        this.settings = settings;
//        this.activeAllocators = new ConcurrentHashMap<>();
        this.totalAllocated = new AtomicLong(0);

        // Configure memory limits - parse size strings manually
//        this.maxAllocation = parseByteSize(settings.get("parquet.memory.max_allocation", "1gb"));
//        this.initReservation = parseByteSize(settings.get("parquet.memory.init_reservation", "100mb"));
        // TODO get these from settings?
        this.maxAllocation = 10L * 1024 * 1024 * 1024;
        this.initReservation = 1024 * 1024 * 100;

        // Set up allocation listener for monitoring
//        this.allocationListener = new PoolAllocationListener();

        this.rootAllocator = new RootAllocator(maxAllocation);
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
     * @param name     Unique name for the allocator
     * @param maxAllocation Maximum allocation limit
     * @return BufferAllocator configured with specified limits
     */
    public BufferAllocator createAllocator(String name, long initReservation, long maxAllocation) {
        BufferAllocator childAllocator = rootAllocator.newChildAllocator(name, 0, 1024 * 1024 * 1024);
//        activeAllocators.put(name, childAllocator);
        return childAllocator;
    }

    public long getAllocatedBytes() {
        return rootAllocator.getAllocatedMemory();
    }

    /**
     * Releases an allocator and cleans up resources.
     *
     * @param name Name of the allocator to release
     */
    public void releaseAllocator(String name) {
//        BufferAllocator allocator = activeAllocators.remove(name);
//        if (allocator != null) {
//            long allocated = allocator.getAllocatedMemory();
//            totalAllocated.addAndGet(-allocated);
//            allocator.close();
//        }
    }

    /**
     * Closes all active allocators and cleans up the pool.
     */
    public void close() {
//        activeAllocators.values().forEach(BufferAllocator::close);
//        activeAllocators.clear();
        totalAllocated.set(0);
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
}
