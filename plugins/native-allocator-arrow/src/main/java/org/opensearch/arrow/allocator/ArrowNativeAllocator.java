/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.arrow.spi.NativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Arrow-backed implementation of {@link NativeAllocator}.
 *
 * <p>Owns a single {@link RootAllocator} for the node. All plugins that need
 * Arrow buffers obtain pool handles from this class via the SPI interface.
 *
 * <h2>Elastic rebalancing</h2>
 * <p>A background task periodically redistributes unused capacity across pools.
 * Each pool has a <em>guaranteed</em> limit (configured via settings). When other
 * pools are idle, an active pool can temporarily grow beyond its guarantee up to
 * the root limit. When contention rises, pools shrink back toward their guarantee.
 * This prevents idle capacity from being wasted while maintaining isolation under load.
 *
 * <p>The singleton instance is accessible via {@link #instance()} after the
 * plugin creates it. This allows child plugins (in the same classloader) to
 * locate the allocator without explicit injection.
 */
public class ArrowNativeAllocator implements NativeAllocator {

    private static volatile ArrowNativeAllocator INSTANCE;

    /**
     * Returns the singleton instance, or throws if the plugin hasn't started.
     */
    public static ArrowNativeAllocator instance() {
        ArrowNativeAllocator inst = INSTANCE;
        if (inst == null) {
            throw new IllegalStateException(
                "ArrowNativeAllocator not initialized. " + "Ensure native-allocator-arrow plugin is installed and started."
            );
        }
        return inst;
    }

    private final RootAllocator root;
    private final ConcurrentMap<String, ArrowPoolHandle> pools = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> poolMins = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> poolMaxes = new ConcurrentHashMap<>();
    private final ScheduledExecutorService rebalancer;
    private volatile ScheduledFuture<?> rebalanceTask;

    /**
     * Creates a new allocator with a fresh RootAllocator.
     *
     * @param rootLimit maximum bytes for the root allocator
     */
    public ArrowNativeAllocator(long rootLimit) {
        this.root = new RootAllocator(rootLimit);
        org.opensearch.threadpool.Scheduler.SafeScheduledThreadPoolExecutor executor =
            new org.opensearch.threadpool.Scheduler.SafeScheduledThreadPoolExecutor(1, r -> {
                Thread t = new Thread(r, "native-allocator-rebalancer");
                t.setDaemon(true);
                return t;
            });
        executor.setRemoveOnCancelPolicy(true);
        this.rebalancer = executor;
        INSTANCE = this;
    }

    /**
     * Schedules (or reschedules) the rebalancer at the given interval.
     * A value of 0 disables rebalancing.
     *
     * @param intervalSeconds rebalance period in seconds, or 0 to disable
     */
    public void setRebalanceInterval(long intervalSeconds) {
        ScheduledFuture<?> existing = rebalanceTask;
        if (existing != null) {
            org.opensearch.common.util.concurrent.FutureUtils.cancel(existing);
            rebalanceTask = null;
        }
        if (intervalSeconds > 0) {
            rebalanceTask = rebalancer.scheduleAtFixedRate(this::rebalance, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        }
    }

    @Override
    public PoolHandle getOrCreatePool(String poolName, long limit) {
        return getOrCreatePool(poolName, limit, limit);
    }

    /**
     * Creates or returns a pool with min/max limits.
     *
     * @param poolName logical pool name
     * @param min guaranteed minimum bytes (always available)
     * @param max maximum bytes the pool can burst to
     * @return the pool handle
     */
    public PoolHandle getOrCreatePool(String poolName, long min, long max) {
        poolMins.putIfAbsent(poolName, min);
        poolMaxes.putIfAbsent(poolName, max);
        return pools.computeIfAbsent(poolName, name -> {
            BufferAllocator child = root.newChildAllocator(name, 0, min);
            return new ArrowPoolHandle(child);
        });
    }

    @Override
    public void setPoolLimit(String poolName, long newLimit) {
        ArrowPoolHandle handle = pools.get(poolName);
        if (handle == null) {
            throw new IllegalStateException("Pool '" + poolName + "' does not exist");
        }
        poolMaxes.put(poolName, newLimit);
        handle.allocator.setLimit(newLimit);
    }

    /**
     * Updates the minimum guaranteed bytes for a pool.
     *
     * @param poolName the pool name
     * @param newMin new minimum bytes
     */
    public void setPoolMin(String poolName, long newMin) {
        if (!pools.containsKey(poolName)) {
            throw new IllegalStateException("Pool '" + poolName + "' does not exist");
        }
        poolMins.put(poolName, newMin);
    }

    @Override
    public void setRootLimit(long limit) {
        root.setLimit(limit);
    }

    @Override
    public NativeAllocatorPoolStats stats() {
        List<NativeAllocatorPoolStats.PoolStats> poolStats = new ArrayList<>();
        for (var entry : pools.entrySet()) {
            BufferAllocator alloc = entry.getValue().allocator;
            poolStats.add(
                new NativeAllocatorPoolStats.PoolStats(
                    entry.getKey(),
                    alloc.getAllocatedMemory(),
                    alloc.getPeakMemoryAllocation(),
                    alloc.getLimit(),
                    alloc.getChildAllocators().size()
                )
            );
        }
        return new NativeAllocatorPoolStats(root.getAllocatedMemory(), root.getPeakMemoryAllocation(), root.getLimit(), poolStats);
    }

    @Override
    public void close() {
        rebalancer.shutdownNow();
        pools.forEach((name, handle) -> {
            try {
                handle.allocator.close();
            } catch (Exception e) {
                // best-effort
            }
        });
        pools.clear();
        root.close();
        INSTANCE = null;
    }

    /**
     * Redistributes unused capacity across pools based on min/max guarantees.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Every pool is guaranteed at least its configured min</li>
     *   <li>Compute headroom = rootLimit - sum(all pool current allocations)</li>
     *   <li>Identify active pools (allocated > 0)</li>
     *   <li>Distribute headroom equally among active pools, capped at each pool's max</li>
     *   <li>Inactive pools are set to their min</li>
     *   <li>No pool's limit ever drops below its current allocation or its min</li>
     * </ol>
     */
    void rebalance() {
        if (pools.isEmpty()) return;

        long rootLimit = root.getLimit();
        long totalAllocated = 0;
        List<String> activePoolNames = new ArrayList<>();

        for (Map.Entry<String, ArrowPoolHandle> entry : pools.entrySet()) {
            long allocated = entry.getValue().allocator.getAllocatedMemory();
            totalAllocated += allocated;

            if (allocated > 0) {
                activePoolNames.add(entry.getKey());
            }
        }

        long headroom = Math.max(0, rootLimit - totalAllocated);
        int activeCount = activePoolNames.size();
        long bonusPerActive = activeCount > 0 ? headroom / activeCount : 0;

        for (Map.Entry<String, ArrowPoolHandle> entry : pools.entrySet()) {
            String name = entry.getKey();
            BufferAllocator alloc = entry.getValue().allocator;
            long min = poolMins.getOrDefault(name, 0L);
            long max = poolMaxes.getOrDefault(name, Long.MAX_VALUE);
            long currentAllocation = alloc.getAllocatedMemory();

            long effectiveLimit;
            if (activeCount > 0 && activePoolNames.contains(name)) {
                effectiveLimit = min + bonusPerActive;
            } else {
                effectiveLimit = min;
            }

            // Cap at pool's max
            effectiveLimit = Math.min(effectiveLimit, max);
            // Never drop below current allocation or min
            effectiveLimit = Math.max(effectiveLimit, currentAllocation);
            effectiveLimit = Math.max(effectiveLimit, min);
            // Never exceed root
            effectiveLimit = Math.min(effectiveLimit, rootLimit);

            alloc.setLimit(effectiveLimit);
        }
    }

    /**
     * Returns the underlying Arrow allocator for a pool.
     *
     * @param poolName name of the pool to look up
     */
    public BufferAllocator getPoolAllocator(String poolName) {
        ArrowPoolHandle handle = pools.get(poolName);
        if (handle == null) {
            throw new IllegalStateException("Pool '" + poolName + "' does not exist");
        }
        return handle.allocator;
    }

    /**
     * Returns the root Arrow allocator.
     */
    public BufferAllocator getRootAllocator() {
        return root;
    }

    /**
     * Returns all registered pool names.
     */
    public Set<String> getPoolNames() {
        return Collections.unmodifiableSet(pools.keySet());
    }

    /**
     * Returns the guaranteed limit for a pool.
     *
     * @param poolName name of the pool
     */
    /**
     * Returns the minimum guaranteed bytes for a pool.
     *
     * @param poolName name of the pool
     */
    public long getPoolMin(String poolName) {
        return poolMins.getOrDefault(poolName, 0L);
    }

    /**
     * Returns the maximum burst limit for a pool.
     *
     * @param poolName name of the pool
     */
    public long getPoolMax(String poolName) {
        return poolMaxes.getOrDefault(poolName, Long.MAX_VALUE);
    }

    /**
     * Arrow-backed pool handle.
     */
    static class ArrowPoolHandle implements PoolHandle {

        final BufferAllocator allocator;

        ArrowPoolHandle(BufferAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public PoolHandle newChild(String childName, long childLimit) {
            return new ArrowPoolHandle(allocator.newChildAllocator(childName, 0, childLimit));
        }

        @Override
        public long allocatedBytes() {
            return allocator.getAllocatedMemory();
        }

        @Override
        public long peakBytes() {
            return allocator.getPeakMemoryAllocation();
        }

        @Override
        public long limit() {
            return allocator.getLimit();
        }

        @Override
        public void close() {
            allocator.close();
        }

        public BufferAllocator getAllocator() {
            return allocator;
        }
    }
}
