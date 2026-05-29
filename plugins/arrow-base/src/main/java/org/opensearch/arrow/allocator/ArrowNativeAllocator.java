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
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;

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
 * <p>Constructed once by {@link ArrowBasePlugin#createComponents} and exposed to
 * downstream plugins via Guice and {@code PluginComponentRegistry} so consumers
 * receive the instance through explicit dependency injection rather than a static
 * singleton.
 */
public class ArrowNativeAllocator implements NativeAllocator {

    private final RootAllocator root;
    private final ConcurrentMap<String, ArrowPoolHandle> pools = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> poolMins = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> poolMaxes = new ConcurrentHashMap<>();
    private final ScheduledExecutorService rebalancer;
    private volatile ScheduledFuture<?> rebalanceTask;
    /**
     * True iff the rebalancer is configured to run periodically. Used by
     * {@link #getOrCreatePool} to decide each pool's initial child-allocator
     * limit: when rebalancing is enabled, pools start at {@code min} and grow
     * via the next rebalance tick (preserving the original PR's
     * "guarantee + burst" semantics); when rebalancing is disabled, pools
     * start at {@code max} so consumers can allocate immediately without
     * waiting for a tick that never comes.
     */
    private volatile boolean rebalancerEnabled = false;

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
        rebalancerEnabled = intervalSeconds > 0;
        if (rebalancerEnabled) {
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
            // Pick an initial limit that's safe for both rebalancer-on and rebalancer-off
            // deployments. When rebalancing is enabled, start at min (the original PR's
            // "guarantee + burst" semantics): the next rebalance tick will distribute
            // headroom up to each pool's max. When rebalancing is disabled (the default),
            // pools with min=0 would otherwise reject every allocation until a tick that
            // never comes — start at max so consumers can allocate immediately.
            long initial = rebalancerEnabled ? min : max;
            BufferAllocator child = root.newChildAllocator(name, 0, initial);
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
     * Updates the minimum guaranteed bytes for a pool. The new min is recorded for the
     * rebalancer (which honors it as a floor on the next tick) and also pushed to the
     * live {@link BufferAllocator} so the change takes effect immediately even when
     * the rebalancer is disabled — the alternative was a Dynamic setting that returned
     * HTTP 200 but had no observable effect.
     *
     * <p>Live propagation rules:
     * <ul>
     *   <li>If {@code newMin} exceeds the pool's current limit, the limit is raised to
     *       {@code newMin} (capped at the configured pool max). Children of the pool
     *       allocator inherit the change automatically via Arrow's parent-cap check at
     *       allocation time, so dynamic resizes reach in-flight workloads without an
     *       explicit notification SPI.
     *   <li>If {@code newMin} is below the current limit, the limit is left alone —
     *       the rebalancer is the only path that shrinks live limits, so a min change
     *       on its own never reduces capacity in flight.
     * </ul>
     *
     * @param poolName the pool name
     * @param newMin new minimum bytes
     */
    public void setPoolMin(String poolName, long newMin) {
        ArrowPoolHandle handle = pools.get(poolName);
        if (handle == null) {
            throw new IllegalStateException("Pool '" + poolName + "' does not exist");
        }
        poolMins.put(poolName, newMin);
        long max = poolMaxes.getOrDefault(poolName, Long.MAX_VALUE);
        long current = handle.allocator.getLimit();
        long target = Math.min(newMin, max);
        if (target > current) {
            handle.allocator.setLimit(target);
        }
    }

    @Override
    public void setRootLimit(long limit) {
        root.setLimit(limit);
    }

    /**
     * Returns a point-in-time stats snapshot across all pools. Used by the
     * {@code NativeAllocatorStatsRegistry} component published from
     * {@code ArrowBasePlugin.createComponents()} and wired into {@code NodeService} to
     * render allocator state under {@code _nodes/stats[/native_allocator]}.
     */
    public NativeAllocatorPoolStats stats() {
        List<NativeAllocatorPoolStats.PoolStats> poolStats = new ArrayList<>();
        for (var entry : pools.entrySet()) {
            BufferAllocator alloc = entry.getValue().allocator;
            poolStats.add(
                new NativeAllocatorPoolStats.PoolStats(
                    entry.getKey(),
                    alloc.getAllocatedMemory(),
                    alloc.getPeakMemoryAllocation(),
                    alloc.getLimit()
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
        // Close any remaining child allocators (e.g., ad-hoc children created via ArrowAllocatorService)
        for (BufferAllocator child : new ArrayList<>(root.getChildAllocators())) {
            try {
                child.close();
            } catch (Exception e) {
                // best-effort — log but don't block shutdown
            }
        }
        root.close();
    }

    /**
     * Redistributes unused capacity across pools based on min/max guarantees.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Every pool is guaranteed at least its configured min</li>
     *   <li>Compute headroom = rootLimit - sum(all pool current allocations)</li>
     *   <li>Distribute headroom equally across all pools (not just active ones), capped
     *       at each pool's max. Distributing to all pools — including those with zero
     *       current allocation — avoids the dead-pool corner case where a pool with
     *       min = 0 starts at limit = 0, can never make its first allocation, and so
     *       never becomes "active" enough to receive a bonus. Pools that don't need the
     *       headroom stay at min naturally because their max caps the bonus.</li>
     *   <li>No pool's limit ever drops below its current allocation or its min</li>
     * </ol>
     */
    void rebalance() {
        if (pools.isEmpty()) return;

        long rootLimit = root.getLimit();
        long totalAllocated = 0;

        for (Map.Entry<String, ArrowPoolHandle> entry : pools.entrySet()) {
            totalAllocated += entry.getValue().allocator.getAllocatedMemory();
        }

        long headroom = Math.max(0, rootLimit - totalAllocated);
        int poolCount = pools.size();
        long bonusPerPool = poolCount > 0 ? headroom / poolCount : 0;

        for (Map.Entry<String, ArrowPoolHandle> entry : pools.entrySet()) {
            String name = entry.getKey();
            BufferAllocator alloc = entry.getValue().allocator;
            long min = poolMins.getOrDefault(name, 0L);
            long max = poolMaxes.getOrDefault(name, Long.MAX_VALUE);
            long currentAllocation = alloc.getAllocatedMemory();

            long effectiveLimit = min + bonusPerPool;

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
