/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.memory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.Closeable;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Default {@link ArrowAllocatorService} backed by a single {@link RootAllocator}. The
 * underlying {@link org.apache.arrow.memory.AllocationManager} implementation is selected
 * by Arrow based on classpath and system properties; {@code arrow-base} ships
 * {@code arrow-memory-netty}, so by default Netty-backed allocation is used. Switching to
 * {@code arrow-memory-unsafe} is a build-time choice and does not require changes here.
 */
@SuppressWarnings("removal")
public final class DefaultArrowAllocatorService implements ArrowAllocatorService, Closeable {

    private final RootAllocator root;

    /** Creates a new service with an unbounded root; child allocators carry their own limits. */
    public DefaultArrowAllocatorService() {
        this.root = AccessController.doPrivileged((PrivilegedAction<RootAllocator>) () -> new RootAllocator(Long.MAX_VALUE));
    }

    @Override
    public BufferAllocator newChildAllocator(String name, long limit) {
        return root.newChildAllocator(name, 0, limit);
    }

    @Override
    public long getAllocatedMemory() {
        return root.getAllocatedMemory();
    }

    @Override
    public long getPeakMemoryAllocation() {
        return root.getPeakMemoryAllocation();
    }

    @Override
    public void close() {
        root.close();
    }
}
