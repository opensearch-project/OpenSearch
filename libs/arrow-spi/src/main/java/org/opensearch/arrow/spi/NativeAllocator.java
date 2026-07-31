/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

import java.io.Closeable;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Unified native memory allocator interface.
 *
 * <p>Manages memory pools under a shared budget. Each pool has a minimum
 * guaranteed allocation and a maximum burst limit. Implementations may
 * redistribute unused capacity across pools.
 *
 * @opensearch.api
 */
public interface NativeAllocator extends Closeable {

    /**
     * Returns the named pool, creating it on first access.
     * Subsequent calls with the same name return the existing pool (first-call config wins).
     *
     * @param poolName logical pool name
     * @param min minimum guaranteed bytes
     * @param max maximum bytes this pool can allocate
     * @param group the group this pool belongs to for aggregated stats, or null
     * @return an opaque pool handle
     */
    PoolHandle getOrCreatePool(String poolName, long min, long max, PoolGroup group);

    /**
     * Updates the effective limit of an existing pool.
     *
     * @param poolName logical pool name
     * @param newLimit new maximum bytes for the pool
     */
    void setPoolLimit(String poolName, long newLimit);

    /**
     * Registers a virtual pool with initial min/max and a callback
     * invoked when the pool's limit changes.
     *
     * @param poolName logical pool name
     * @param min minimum guaranteed bytes
     * @param max initial maximum bytes (the pool's starting limit)
     * @param group the group this pool belongs to for aggregated stats
     * @param limitSetter callback invoked when the pool limit changes
     * @return a handle to update stats from the native layer
     */
    VirtualPoolHandle registerVirtualPool(String poolName, long min, long max, PoolGroup group, Consumer<Long> limitSetter);

    /**
     * Updates the minimum guaranteed bytes for a pool.
     *
     * @param poolName logical pool name
     * @param newMin new minimum bytes
     */
    void setPoolMin(String poolName, long newMin);

    /**
     * Returns all registered pool names.
     */
    Set<String> getAllPoolNames();

    /**
     * Adds a callback invoked before stats collection to refresh pool usage data.
     *
     * @param refresher runnable that updates pool stats
     */
    void addStatsRefresher(Runnable refresher);

    /**
     * Sets the supplier for process-wide native memory stats.
     *
     * @param supplier returns [allocatedBytes, residentBytes]
     */
    void setNativeMemoryStatsSupplier(Supplier<long[]> supplier);

    /**
     * Registers a listener invoked when the effective limit of a pool group changes.
     * The consumer receives the new total effective limit (sum of all pools in the group).
     *
     * @param group the pool group to listen on
     * @param listener consumer that receives the new grouped limit in bytes
     */
    void addPoolGroupLimitListener(PoolGroup group, Consumer<Long> listener);

    /**
     * Handle for a virtual pool. Plugins update stats via this handle.
     */
    interface VirtualPoolHandle {
        /**
         * Update the current usage stats.
         *
         * @param allocatedBytes current allocated bytes
         * @param peakBytes peak allocated bytes
         */
        void updateStats(long allocatedBytes, long peakBytes);

        /** Returns current allocated bytes. */
        long allocatedBytes();

        /** Returns peak allocated bytes. */
        long peakBytes();

        /** Returns current limit. */
        long limit();
    }

    /**
     * Opaque handle to a memory pool.
     */
    interface PoolHandle {

        /**
         * Creates a named child allocation within this pool.
         *
         * @param childName name for debugging
         * @param childLimit maximum bytes for the child
         * @return a child handle
         */
        PoolHandle newChild(String childName, long childLimit);

        /** Returns the current allocated bytes. */
        long allocatedBytes();

        /** Returns the peak memory allocation. */
        long peakBytes();

        /** Returns the configured limit. */
        long limit();

        /** Releases this allocation handle. */
        void close();
    }
}
