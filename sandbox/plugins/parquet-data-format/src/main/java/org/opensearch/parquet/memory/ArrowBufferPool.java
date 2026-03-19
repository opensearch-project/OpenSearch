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

import java.io.Closeable;

/**
 * Manages Arrow BufferAllocator lifecycle with configurable allocation limits.
 */
public class ArrowBufferPool implements Closeable {

    private static final Logger logger = LogManager.getLogger(ArrowBufferPool.class);

    private final RootAllocator rootAllocator;
    private final long maxChildAllocation;

    public ArrowBufferPool() {
        long maxAllocationInBytes = 10L * 1024 * 1024 * 1024;
        logger.info("Max native memory allocation for ArrowBufferPool: {} bytes", maxAllocationInBytes);
        this.rootAllocator = new RootAllocator(maxAllocationInBytes);
        this.maxChildAllocation = 1024L * 1024 * 1024;
    }

    public BufferAllocator createChildAllocator(String name) {
        return rootAllocator.newChildAllocator(name, 0, maxChildAllocation);
    }

    public long getTotalAllocatedBytes() {
        return rootAllocator.getAllocatedMemory();
    }

    @Override
    public void close() {
        rootAllocator.close();
    }
}
