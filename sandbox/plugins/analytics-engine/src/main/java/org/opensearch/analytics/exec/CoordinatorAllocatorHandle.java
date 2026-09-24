/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;

/**
 * Owns the coordinator-side {@link BufferAllocator} child of the framework's
 * {@code POOL_QUERY} pool. Acts as a thin Guice-bound holder so the analytics plugin
 * can close the allocator deterministically on {@code Plugin.close()} — before the
 * arrow-base plugin closes the root allocator.
 *
 * <p>Without this, the allocator would be created and held by the Guice-managed
 * {@link DefaultPlanExecutor} (which has no close hook), and would still be
 * outstanding when {@code ArrowNativeAllocator.close()} walks the pool tree at
 * shutdown — causing {@code BaseAllocator.close()} to throw
 * {@code IllegalStateException: Allocator[ROOT] closed with outstanding child allocators}.
 *
 * <p>The plugin owns the lifetime; consumers ({@code DefaultPlanExecutor}) only
 * read the underlying allocator via {@link #getAllocator()}.
 *
 * <p>Also carries the node-scoped Arrow import staging allocator so the coordinator-reduce path can
 * reach it. That one is <b>borrowed, not owned</b>: {@code AnalyticsSearchService} creates and closes
 * it, so a node has exactly one whatever the execution path.
 */
public final class CoordinatorAllocatorHandle implements AutoCloseable {

    private final BufferAllocator allocator;
    private final BufferAllocator importStagingAllocator;

    /**
     * Wraps an already-constructed coordinator allocator (typically a child of
     * {@code POOL_QUERY}). The caller (the plugin) retains responsibility for
     * having created the allocator under the right parent.
     *
     * @param importStagingAllocator the node-scoped import staging allocator owned by
     *        {@code AnalyticsSearchService}; borrowed here, never closed by this handle
     */
    public CoordinatorAllocatorHandle(BufferAllocator allocator, BufferAllocator importStagingAllocator) {
        this.allocator = allocator;
        this.importStagingAllocator = importStagingAllocator;
    }

    /** Returns the wrapped allocator. Consumers must not close it directly. */
    public BufferAllocator getAllocator() {
        return allocator;
    }

    /**
     * Returns the node-scoped allocator Arrow C Data imports are staged on — unbounded and parented at
     * the root so an import cannot fail part-way through an array, and long-lived because the Flight
     * transport keeps charging it after the importing stream closes. Owned by
     * {@code AnalyticsSearchService}; consumers must not close it.
     */
    public BufferAllocator getImportStagingAllocator() {
        return importStagingAllocator;
    }

    @Override
    public void close() {
        allocator.close();
    }
}
