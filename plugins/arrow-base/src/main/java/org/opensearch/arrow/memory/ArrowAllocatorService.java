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
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Node-level Arrow allocator service. Plugins that need an Arrow {@link BufferAllocator}
 * obtain a child from this service instead of creating their own {@link RootAllocator},
 * so every Arrow buffer on a node shares the same root. Arrow's
 * {@link org.apache.arrow.memory.AllocationManager} associate check on cross-allocator
 * operations requires {@code source.getRoot() == target.getRoot()}, and this is what
 * makes producer-to-consumer zero-copy handoff work across plugin boundaries.
 *
 * <p>One instance per node, bound into Guice by {@code ArrowBasePlugin}. Consumers inject
 * the interface and do not depend on a concrete implementation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface ArrowAllocatorService {

    /**
     * Creates a named child allocator with its own memory limit. Callers own the returned
     * allocator and must close it.
     *
     * @param name descriptive name for debugging (e.g., "flight", "analytics-search")
     * @param limit maximum bytes this child can allocate; Arrow enforces this on every
     *              {@code buffer()} call and walks the parent chain, throwing
     *              {@link org.apache.arrow.memory.OutOfMemoryException} on breach
     */
    BufferAllocator newChildAllocator(String name, long limit);

    /** Current bytes allocated across all descendants of the root. */
    long getAllocatedMemory();

    /** Peak bytes allocated since this service started. */
    long getPeakMemoryAllocation();
}
