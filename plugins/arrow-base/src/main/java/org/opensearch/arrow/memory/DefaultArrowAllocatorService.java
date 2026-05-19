/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.memory;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;

import java.io.Closeable;

/**
 * Default {@link ArrowAllocatorService} that delegates to the unified
 * {@link ArrowNativeAllocator}. All child allocators share the same root,
 * ensuring a single memory domain across the node.
 */
public final class DefaultArrowAllocatorService implements ArrowAllocatorService, Closeable {

    private final ArrowNativeAllocator nativeAllocator;

    /**
     * Creates a service backed by the singleton native allocator.
     *
     * @param nativeAllocator the unified native allocator instance
     */
    public DefaultArrowAllocatorService(ArrowNativeAllocator nativeAllocator) {
        this.nativeAllocator = nativeAllocator;
    }

    @Override
    public BufferAllocator newChildAllocator(String name, long limit) {
        return nativeAllocator.getRootAllocator().newChildAllocator(name, 0, limit);
    }

    @Override
    public long getAllocatedMemory() {
        return nativeAllocator.getRootAllocator().getAllocatedMemory();
    }

    @Override
    public long getPeakMemoryAllocation() {
        return nativeAllocator.getRootAllocator().getPeakMemoryAllocation();
    }

    @Override
    public void close() {
        // Root is owned by ArrowNativeAllocator — don't close here.
    }
}
