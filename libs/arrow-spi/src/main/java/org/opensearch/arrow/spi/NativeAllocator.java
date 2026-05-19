/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

import java.io.Closeable;

/**
 * Arrow-agnostic interface for a hierarchical native memory allocator.
 *
 * <p>The implementation (backed by Arrow's {@code RootAllocator}) is provided by
 * a plugin. The SPI allows other subsystems to interact with the allocator
 * without depending on Arrow classes.
 *
 * <p>Plugins that need Arrow allocators obtain the implementation via
 * service lookup or plugin extension and call {@link #getOrCreatePool} to
 * register their pool.
 *
 * @opensearch.api
 */
public interface NativeAllocator extends Closeable {

    /**
     * Returns the named pool, creating it on first access with the given limit.
     * Subsequent calls with the same name return the same pool (first-call limit wins).
     *
     * @param poolName logical pool name (e.g., "query", "flight")
     * @param limit maximum bytes this pool can allocate in aggregate
     * @return an opaque pool handle
     */
    PoolHandle getOrCreatePool(String poolName, long limit);

    /**
     * Updates the limit of an existing pool.
     *
     * @param poolName logical pool name
     * @param newLimit new maximum bytes for the pool
     */
    void setPoolLimit(String poolName, long newLimit);

    /**
     * Sets the root-level memory limit for the entire allocator.
     *
     * @param limit new maximum bytes for the root allocator
     */
    void setRootLimit(long limit);

    /**
     * Collects a point-in-time stats snapshot across all pools.
     */
    NativeAllocatorPoolStats stats();

    /**
     * Opaque handle to a memory pool. Plugins downcast to the concrete type
     * (e.g., Arrow's {@code BufferAllocator}) in the implementation layer.
     */
    interface PoolHandle {

        /**
         * Creates a named child allocation within this pool.
         *
         * @param childName name for debugging
         * @param childLimit maximum bytes for the child
         * @return an opaque child handle (downcast to BufferAllocator in Arrow impl)
         */
        PoolHandle newChild(String childName, long childLimit);

        /**
         * Returns the current allocated bytes for this pool/child.
         */
        long allocatedBytes();

        /**
         * Returns the peak memory allocation.
         */
        long peakBytes();

        /**
         * Returns the configured limit.
         */
        long limit();

        /**
         * Releases this allocation handle.
         */
        void close();
    }
}
