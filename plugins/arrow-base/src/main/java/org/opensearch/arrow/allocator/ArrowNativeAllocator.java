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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.arrow.spi.NativeAllocator;
import org.opensearch.arrow.spi.PoolGroup;
import org.opensearch.common.SetOnce;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.Supplier;

import static org.opensearch.arrow.allocator.ArrowBasePlugin.DEFAULT_OVERCOMMIT_ENABLED;
import static org.opensearch.arrow.allocator.ArrowBasePlugin.DEFAULT_OVERCOMMIT_PRESSURE_THRESHOLD;

/**
 * Arrow-backed implementation of {@link NativeAllocator}.
 *
 * <p>Owns a single {@link RootAllocator} (set to {@code Long.MAX_VALUE} — per-pool
 * limits are the real enforcement). Manages both Arrow-backed pools and virtual pools.
 */
public class ArrowNativeAllocator implements NativeAllocator {

    private static final Logger logger = LogManager.getLogger(ArrowNativeAllocator.class);

    private final RootAllocator root;
    private final ConcurrentMap<String, ArrowPoolHandle> pools = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, VirtualPoolHandleImpl> virtualPools = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, PoolConfig> poolConfigs = new ConcurrentHashMap<>();
    /** Pools excluded from budget validation, the rebalancer, and pool-group limit sums. */
    private final Set<String> unmanagedPools = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<PoolGroup, List<Consumer<Long>>> poolGroupLimitListeners = new ConcurrentHashMap<>();
    private final List<Runnable> statsRefreshers = new CopyOnWriteArrayList<>();
    private volatile Supplier<long[]> nativeMemoryStatsSupplier;
    private volatile long budget = Long.MAX_VALUE;

    // ─── Over-commit admission (allocator-owned decision) ────────────────────────
    /** Node-level native memory utilization % (0–100), or negative when unavailable. Injected by the node. */
    private volatile DoubleSupplier nativeMemoryPressureSupplier;
    /** Feature gate. When false, {@link #tryOverCommit()} always rejects (status-quo behavior). */
    private volatile boolean overCommitEnabled = DEFAULT_OVERCOMMIT_ENABLED;
    /** Node native pressure % at/above which over-commit is refused. */
    private volatile double overCommitPressureThreshold = DEFAULT_OVERCOMMIT_PRESSURE_THRESHOLD;
    /** Permit gate bounding the number of concurrently over-committing operations (sized once at startup). */
    private final SetOnce<Semaphore> overCommitPermits = new SetOnce<>();
    /** Outstanding over-commit leases keyed by their token, so a token from the native side can be released. */
    private final ConcurrentMap<Long, OverCommitLease> outstandingOverCommits = new ConcurrentHashMap<>();
    /** Monotonic source of over-commit tokens. Starts at 0, which is reserved for "no grant". */
    private final AtomicLong overCommitTokenSeq = new AtomicLong();

    /**
     * Creates a new allocator with a fresh RootAllocator.
     */
    public ArrowNativeAllocator() {
        this.root = new RootAllocator(Long.MAX_VALUE);
    }

    /**
     * Sets the total native memory budget for validation.
     *
     * @param budget node.native_memory.limit in bytes
     */
    public void setBudget(long budget) {
        this.budget = budget;
    }

    // ─── Over-commit admission API (called by pool-full paths across all pool types) ─────────────

    /**
     * Installs the node-level native-memory pressure supplier (percent 0–100, or negative if unavailable).
     *
     * @param supplier supplies the current node native-memory utilization percentage
     */
    public void setNativeMemoryPressureSupplier(DoubleSupplier supplier) {
        this.nativeMemoryPressureSupplier = supplier;
    }

    /**
     * Enables/disables the over-commit fallback (feature gate).
     *
     * @param enabled whether the over-commit fallback is enabled
     */
    public void setOverCommitEnabled(boolean enabled) {
        this.overCommitEnabled = enabled;
    }

    /**
     * Sets the node native-pressure % at/above which over-commit is refused.
     *
     * @param thresholdPercent the native-memory pressure percentage threshold
     */
    public void setOverCommitPressureThreshold(double thresholdPercent) {
        this.overCommitPressureThreshold = thresholdPercent;
    }

    /**
     * Sets the maximum number of concurrently over-committing operations. Applied once at startup.
     *
     * @param max the maximum number of concurrent over-commits
     */
    public void setMaxConcurrentOverCommits(int max) {
        this.overCommitPermits.set(new Semaphore(Math.max(ArrowBasePlugin.OVERCOMMIT_MAX_CONCURRENT_MIN, max)));
    }

    /** Current node-level native memory pressure %, or -1 when unavailable (signal missing / not ready). */
    double currentNativePressurePercent() {
        DoubleSupplier s = this.nativeMemoryPressureSupplier;
        if (s == null) {
            return -1.0;
        }
        try {
            return s.getAsDouble();
        } catch (RuntimeException e) {
            return -1.0;
        }
    }

    /**
     * Allocator-owned decision: may a full pool over-commit right now? Grants only when the feature is
     * enabled, node-level native memory pressure is below the threshold, and a concurrency permit is
     * available. On a grant this returns a {@link OverCommitLease} that the caller MUST
     * {@link OverCommitLease#close() close} exactly once when the over-committing operation completes
     * (the permit is held until then); on rejection it returns {@link Optional#empty()}.
     *
     * <p>This is the safe, in-JVM entry point: the only way to release a permit is to close a lease
     * that this method minted, so a permit can never be released without first being acquired.
     *
     * <p>Generic across pool types (Arrow-backed pools via allocation-failure hooks, virtual/native
     * pools via the FFM upcall through {@link #tryOverCommitToken()}). Never throws.
     *
     * @return a lease on grant, or empty on rejection
     */
    public Optional<OverCommitLease> tryOverCommit() {
        try {
            if (overCommitEnabled == false) {
                return Optional.empty();
            }
            double pressure = currentNativePressurePercent();
            if (pressure < 0) {
                logger.debug("Over-commit unavailable: native memory pressure signal not ready");
                return Optional.empty();
            }
            if (pressure >= overCommitPressureThreshold) {
                logger.debug("Over-commit refused: native memory pressure {}% >= threshold {}%", pressure, overCommitPressureThreshold);
                return Optional.empty();
            }
            logger.debug(
                "Native memory pressure {}% within threshold {}%; attempting over-commit permit acquire",
                pressure,
                overCommitPressureThreshold
            );
            // Permit gate: at most max_concurrent over-commits in flight. The permit is held
            // until the lease is closed when the over-committing operation completes.
            Semaphore permits = overCommitPermits.get();
            if (permits == null) {
                logger.warn("Over-commit enabled but permit pool is not initialized; refusing over-commit");
                return Optional.empty();
            }
            if (permits.tryAcquire()) {
                long token = overCommitTokenSeq.incrementAndGet();
                OverCommitLease lease = new OverCommitLease(token);
                outstandingOverCommits.put(token, lease);
                logger.debug(
                    "Over-commit granted; acquired permit (token {}, {} permits now available)",
                    token,
                    permits.availablePermits()
                );
                return Optional.of(lease);
            }
            logger.warn("Over-commit refused: concurrency limit reached (all max_concurrent permits in use)");
            return Optional.empty();
        } catch (Throwable t) {
            return Optional.empty();
        }
    }

    /**
     * FFM upcall entry point for native pools: performs the same decision as {@link #tryOverCommit()}
     * but returns an opaque token instead of a lease object (which cannot cross the native boundary).
     * The native side stores the token and passes it back to {@link #releaseOverCommitToken(long)} on
     * release. Never throws — returns {@code 0} on any rejection or error.
     *
     * @return a nonzero grant token, or {@code 0} if the over-commit was rejected
     */
    public long tryOverCommitToken() {
        return tryOverCommit().map(OverCommitLease::id).orElse(0L);
    }

    /**
     * FFM upcall entry point for native pools: releases the over-commit permit previously granted
     * under {@code token}. An unknown, stale, or already-released token is a no-op, so a spurious
     * native release can never over-release the permit gate.
     *
     * @param token the grant token returned by {@link #tryOverCommitToken()}
     */
    public void releaseOverCommitToken(long token) {
        OverCommitLease lease = outstandingOverCommits.get(token);
        if (lease != null) {
            lease.close();
        }
    }

    /**
     * A capability handle for a single granted over-commit. Minted only by
     * {@link ArrowNativeAllocator#tryOverCommit()}; closing it releases the underlying permit exactly
     * once (idempotent), so double-close and unpaired release are both harmless.
     */
    public final class OverCommitLease implements AutoCloseable {
        private final long id;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private OverCommitLease(long id) {
            this.id = id;
        }

        /** The opaque token identifying this grant (used to release across the native boundary). */
        public long id() {
            return id;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                outstandingOverCommits.remove(id);
                Semaphore permits = overCommitPermits.get();
                if (permits != null) {
                    permits.release();
                    logger.debug("Released over-commit permit (token {}, {} permits now available)", id, permits.availablePermits());
                }
            }
        }
    }

    // ─── Public / SPI methods ───────────────────────────────────────────────────

    @Override
    public PoolHandle getOrCreatePool(String poolName, long min, long max, PoolGroup group) {
        validateSumMaxesWithinBudget(poolName, max);
        poolConfigs.putIfAbsent(poolName, new PoolConfig(min, max, group));
        return pools.computeIfAbsent(poolName, name -> {
            BufferAllocator child = root.newChildAllocator(name, 0, max);
            return new ArrowPoolHandle(child);
        });
    }

    /**
     * Registers an unbounded pool excluded from budget validation, the rebalancer, and pool-group
     * limit sums. Intended for POOL_QUERY: its bytes are zero-copy foreign wraps of pre-existing
     * native memory, so limiting it would leak imported batches. Real enforcement lives Rust-side.
     *
     * @param poolName name of the pool
     * @param group    pool group (for stats/reporting only; excluded from group limit sums)
     * @throws IllegalStateException if a managed pool with this name already exists
     */
    public PoolHandle registerUnmanagedPool(String poolName, PoolGroup group) {
        if (pools.containsKey(poolName)) {
            throw new IllegalStateException(
                "Pool ["
                    + poolName
                    + "] already exists as a managed pool and cannot be re-registered as unmanaged; "
                    + "its child allocator has a bounded limit. Register it unmanaged from the start."
            );
        }
        unmanagedPools.add(poolName);
        poolConfigs.putIfAbsent(poolName, new PoolConfig(0, Long.MAX_VALUE, group));
        return pools.computeIfAbsent(poolName, name -> {
            BufferAllocator child = root.newChildAllocator(name, 0, Long.MAX_VALUE);
            return new ArrowPoolHandle(child);
        });
    }

    /** Whether a pool is a special/unmanaged unbounded pool (excluded from sizing math). */
    public boolean isUnmanagedPool(String poolName) {
        return unmanagedPools.contains(poolName);
    }

    /** Pool names the rebalancer should manage — all pools minus the unmanaged/special ones. */
    public Set<String> getManagedPoolNames() {
        Set<String> managed = new HashSet<>(getAllPoolNames());
        managed.removeAll(unmanagedPools);
        return Collections.unmodifiableSet(managed);
    }

    @Override
    public void setPoolLimit(String poolName, long newLimit) {
        PoolConfig config = poolConfigs.get(poolName);
        if (config != null) {
            config.max = newLimit;
        }
        ArrowPoolHandle arrowHandle = pools.get(poolName);
        if (arrowHandle != null) {
            arrowHandle.allocator.setLimit(newLimit);
            return;
        }
        VirtualPoolHandleImpl vp = virtualPools.get(poolName);
        if (vp != null) {
            vp.setLimit(newLimit);
            return;
        }
        throw new IllegalStateException("Pool '" + poolName + "' does not exist");
    }

    @Override
    public VirtualPoolHandle registerVirtualPool(String poolName, long min, long max, PoolGroup group, Consumer<Long> limitSetter) {
        if (min > max) {
            throw new IllegalArgumentException("Pool '" + poolName + "' min (" + min + ") exceeds max (" + max + ")");
        }
        validateSumMaxesWithinBudget(poolName, max);
        VirtualPoolHandleImpl handle = new VirtualPoolHandleImpl(poolName, max, limitSetter);
        VirtualPoolHandleImpl existing = virtualPools.putIfAbsent(poolName, handle);
        if (existing != null || pools.containsKey(poolName)) {
            virtualPools.remove(poolName, handle);
            throw new IllegalStateException("Pool '" + poolName + "' already registered");
        }
        poolConfigs.put(poolName, new PoolConfig(min, max, group));
        limitSetter.accept(max);
        return handle;
    }

    @Override
    public void setPoolMin(String poolName, long newMin) {
        PoolConfig config = poolConfigs.get(poolName);
        if (config != null) {
            config.min = newMin;
        }
        // Raise live limit if newMin exceeds current effective limit
        ArrowPoolHandle arrowHandle = pools.get(poolName);
        if (arrowHandle != null) {
            long max = config != null ? config.max : Long.MAX_VALUE;
            long current = arrowHandle.allocator.getLimit();
            long target = Math.min(newMin, max);
            if (target > current) {
                arrowHandle.allocator.setLimit(target);
            }
            return;
        }
        VirtualPoolHandleImpl vp = virtualPools.get(poolName);
        if (vp != null) {
            long max = config != null ? config.max : Long.MAX_VALUE;
            long current = vp.limit();
            long target = Math.min(newMin, max);
            if (target > current) {
                vp.setLimit(target);
            }
        }
    }

    @Override
    public Set<String> getAllPoolNames() {
        Set<String> all = new HashSet<>(pools.keySet());
        all.addAll(virtualPools.keySet());
        return Collections.unmodifiableSet(all);
    }

    @Override
    public void addStatsRefresher(Runnable refresher) {
        statsRefreshers.add(refresher);
    }

    @Override
    public void setNativeMemoryStatsSupplier(Supplier<long[]> supplier) {
        this.nativeMemoryStatsSupplier = supplier;
    }

    /**
     * Sets the effective (live) limit for a pool without updating the configured max.
     * Used by the rebalancer to adjust pool limits dynamically.
     *
     * @param poolName name of the pool
     * @param newLimit new effective limit in bytes
     */
    public void setPoolEffectiveLimit(String poolName, long newLimit) {
        ArrowPoolHandle arrowHandle = pools.get(poolName);
        if (arrowHandle != null) {
            arrowHandle.allocator.setLimit(newLimit);
            return;
        }
        VirtualPoolHandleImpl vp = virtualPools.get(poolName);
        if (vp != null) {
            vp.setLimit(newLimit);
            return;
        }
        throw new IllegalStateException("Pool '" + poolName + "' does not exist");
    }

    /**
     * Resets all pools to their configured max. Called when the rebalancer is disabled.
     * Logs a warning for any pool that was bursting above its max.
     */
    public void resetAllPoolsToMax() {
        for (String name : getAllPoolNames()) {
            if (unmanagedPools.contains(name)) {
                continue; // special unbounded pool — nothing to reset
            }
            PoolConfig config = poolConfigs.get(name);
            long max = config != null ? config.max : Long.MAX_VALUE;
            long current = getEffectiveLimit(name);
            if (current > max) {
                logger.warn(
                    "Pool [{}] effective limit {} exceeds max {}, resetting to max. In-flight allocations may be rejected.",
                    name,
                    current,
                    max
                );
            }
            setPoolEffectiveLimit(name, max);
        }
    }

    /**
     * Convenience method for plugins that have Setting objects. Registers the virtual pool
     * and auto-wires dynamic setting listeners for min/max changes.
     *
     * @param poolName name of the virtual pool
     * @param minSetting setting for minimum bytes
     * @param maxSetting setting for maximum bytes
     * @param settings current node settings
     * @param clusterSettings cluster settings for dynamic updates
     * @param group pool group assignment
     * @param limitSetter callback invoked when the pool limit changes
     */
    public VirtualPoolHandle registerVirtualPool(
        String poolName,
        Setting<Long> minSetting,
        Setting<Long> maxSetting,
        Settings settings,
        ClusterSettings clusterSettings,
        PoolGroup group,
        Consumer<Long> limitSetter
    ) {
        long min = minSetting.get(settings);
        long max = maxSetting.get(settings);
        VirtualPoolHandle handle = registerVirtualPool(poolName, min, max, group, limitSetter);

        clusterSettings.addSettingsUpdateConsumer(maxSetting, newMax -> setPoolLimit(poolName, newMax));
        clusterSettings.addSettingsUpdateConsumer(minSetting, newMin -> setPoolMin(poolName, newMin));

        return handle;
    }

    /**
     * Returns a point-in-time stats snapshot across all pools.
     */
    public NativeAllocatorPoolStats stats() {
        refreshStats();

        long nativeAllocated = -1;
        long nativeResident = -1;
        Supplier<long[]> supplier = this.nativeMemoryStatsSupplier;
        if (supplier != null) {
            try {
                long[] stats = supplier.get();
                if (stats != null && stats.length >= 2) {
                    nativeAllocated = stats[0];
                    nativeResident = stats[1];
                }
            } catch (Exception e) {
                // best-effort
            }
        }

        List<NativeAllocatorPoolStats.PoolStats> poolStats = new ArrayList<>();
        for (var entry : pools.entrySet()) {
            BufferAllocator alloc = entry.getValue().allocator;
            PoolConfig config = poolConfigs.get(entry.getKey());
            poolStats.add(
                new NativeAllocatorPoolStats.PoolStats(
                    entry.getKey(),
                    alloc.getAllocatedMemory(),
                    alloc.getPeakMemoryAllocation(),
                    alloc.getLimit(),
                    config != null && config.group != null ? config.group.getName() : null,
                    config != null ? config.min : 0L
                )
            );
        }
        for (var entry : virtualPools.entrySet()) {
            VirtualPoolHandleImpl vp = entry.getValue();
            PoolConfig config = poolConfigs.get(entry.getKey());
            poolStats.add(
                new NativeAllocatorPoolStats.PoolStats(
                    entry.getKey(),
                    vp.allocatedBytes(),
                    vp.peakBytes(),
                    vp.limit(),
                    config != null && config.group != null ? config.group.getName() : null,
                    config != null ? config.min : 0L
                )
            );
        }

        return new NativeAllocatorPoolStats(nativeAllocated, nativeResident, poolStats);
    }

    /**
     * Runs all registered stats refreshers.
     */
    public void refreshStats() {
        for (Runnable refresher : statsRefreshers) {
            try {
                refresher.run();
            } catch (Exception e) {
                // best-effort
            }
        }
    }

    @Override
    public void close() {
        pools.forEach((name, handle) -> {
            try {
                handle.allocator.close();
            } catch (Exception e) {
                // best-effort
            }
        });
        pools.clear();
        virtualPools.clear();
        for (BufferAllocator child : new ArrayList<>(root.getChildAllocators())) {
            try {
                child.close();
            } catch (Exception e) {
                // best-effort
            }
        }
        root.close();
    }

    // ─── Package-private accessors (used by rebalancer and tests) ────────────────

    /**
     * Returns the underlying Arrow allocator for a pool.
     *
     * @param poolName name of the pool
     */
    public BufferAllocator getPoolAllocator(String poolName) {
        ArrowPoolHandle handle = pools.get(poolName);
        if (handle == null) {
            throw new IllegalStateException("Pool '" + poolName + "' does not exist");
        }
        return handle.allocator;
    }

    /** Returns the root Arrow allocator. */
    public BufferAllocator getRootAllocator() {
        return root;
    }

    /** Returns all registered pool names (Arrow pools only). */
    public Set<String> getPoolNames() {
        return Collections.unmodifiableSet(pools.keySet());
    }

    /**
     * Returns the minimum guaranteed bytes for a pool.
     *
     * @param poolName name of the pool
     */
    public long getPoolMin(String poolName) {
        PoolConfig config = poolConfigs.get(poolName);
        return config != null ? config.min : 0L;
    }

    /**
     * Returns the maximum burst limit for a pool.
     *
     * @param poolName name of the pool
     */
    public long getPoolMax(String poolName) {
        PoolConfig config = poolConfigs.get(poolName);
        return config != null ? config.max : Long.MAX_VALUE;
    }

    /**
     * Returns the group for a pool, or null if not assigned.
     *
     * @param poolName name of the pool
     */
    public PoolGroup getPoolGroup(String poolName) {
        PoolConfig config = poolConfigs.get(poolName);
        return config != null ? config.group : null;
    }

    /**
     * Returns the allocated bytes for a virtual pool.
     *
     * @param poolName name of the virtual pool
     */
    public long getVirtualPoolAllocated(String poolName) {
        VirtualPoolHandleImpl vp = virtualPools.get(poolName);
        return vp != null ? vp.allocatedBytes() : 0;
    }

    /**
     * Returns the current limit for a virtual pool.
     *
     * @param poolName name of the virtual pool
     */
    public long getVirtualPoolLimit(String poolName) {
        VirtualPoolHandleImpl vp = virtualPools.get(poolName);
        return vp != null ? vp.limit() : 0;
    }

    /**
     * Returns the effective limit for any pool (Arrow or virtual).
     *
     * @param poolName name of the pool
     */
    public long getEffectiveLimit(String poolName) {
        ArrowPoolHandle arrowHandle = pools.get(poolName);
        if (arrowHandle != null) {
            return arrowHandle.allocator.getLimit();
        }
        VirtualPoolHandleImpl vp = virtualPools.get(poolName);
        if (vp != null) {
            return vp.limit();
        }
        return 0;
    }

    /**
     * Returns the allocated bytes for any pool (Arrow or virtual).
     *
     * @param poolName name of the pool
     */
    public long getAllocated(String poolName) {
        ArrowPoolHandle arrowHandle = pools.get(poolName);
        if (arrowHandle != null) {
            return arrowHandle.allocator.getAllocatedMemory();
        }
        VirtualPoolHandleImpl vp = virtualPools.get(poolName);
        if (vp != null) {
            return vp.allocatedBytes();
        }
        return 0;
    }

    /** Returns the native memory stats supplier. */
    public Supplier<long[]> getNativeMemoryStatsSupplier() {
        return nativeMemoryStatsSupplier;
    }

    /**
     * Registers a listener invoked when the grouped effective limit for the given pool group changes.
     * The consumer receives the new total effective limit (sum of all pools in the group).
     *
     * @param group the pool group to listen on
     * @param listener consumer that receives the new grouped limit in bytes
     */
    public void addPoolGroupLimitListener(PoolGroup group, Consumer<Long> listener) {
        poolGroupLimitListeners.computeIfAbsent(group, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    /**
     * Fires listeners for the specified pool group with the current grouped effective limit sum.
     * Called by the rebalancer after all pool limits for a tick have been updated.
     *
     * @param group the pool group whose listeners should be notified
     */
    public void firePoolGroupListeners(PoolGroup group) {
        List<Consumer<Long>> listeners = poolGroupLimitListeners.get(group);
        if (listeners == null || listeners.isEmpty()) return;

        long groupSum = 0;
        for (var entry : poolConfigs.entrySet()) {
            // Skip unmanaged/special pools: their Long.MAX_VALUE effective limit would swamp the
            // grouped total that group listeners (e.g. the DataFusion pool sizer) consume.
            if (entry.getValue().group == group && unmanagedPools.contains(entry.getKey()) == false) {
                groupSum += getEffectiveLimit(entry.getKey());
            }
        }
        long finalSum = groupSum;
        for (Consumer<Long> listener : listeners) {
            try {
                listener.accept(finalSum);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("Pool group limit listener failed for group [{}]", group), e);
            }
        }
    }

    // ─── Private helpers ─────────────────────────────────────────────────────────

    private void validateSumMaxesWithinBudget(String newPoolName, long newPoolMax) {
        if (budget == Long.MAX_VALUE || budget <= 0) {
            return;
        }
        // Unmanaged/special pools (e.g. POOL_QUERY) are unbounded by design; their Long.MAX_VALUE
        // "max" is not real budget consumption and must not be summed against the node budget.
        if (unmanagedPools.contains(newPoolName)) {
            return;
        }
        long sumMaxes = newPoolMax;
        for (var entry : poolConfigs.entrySet()) {
            if (entry.getKey().equals(newPoolName) == false && unmanagedPools.contains(entry.getKey()) == false) {
                sumMaxes += entry.getValue().max;
            }
        }
        if (sumMaxes > budget) {
            throw new IllegalArgumentException(
                "Sum of pool max limits ("
                    + sumMaxes
                    + " bytes) exceeds native memory budget ("
                    + budget
                    + " bytes). Reduce pool max settings or increase the budget."
            );
        }
    }

    // ─── Inner classes ───────────────────────────────────────────────────────────

    /**
     * Mutable configuration for a pool: min, max, and group.
     */
    static class PoolConfig {
        volatile long min;
        volatile long max;
        final PoolGroup group;

        PoolConfig(long min, long max, PoolGroup group) {
            this.min = min;
            this.max = max;
            this.group = group;
        }
    }

    /**
     * Virtual pool handle implementation. Tracks stats reported from native layer
     * and delegates limit changes to the registered callback.
     */
    public static class VirtualPoolHandleImpl implements VirtualPoolHandle {
        private final String name;
        private volatile long limit;
        private volatile long allocatedBytes;
        private volatile long peakBytes;
        private final Consumer<Long> limitSetter;

        VirtualPoolHandleImpl(String name, long limit, Consumer<Long> limitSetter) {
            this.name = name;
            this.limit = limit;
            this.limitSetter = limitSetter;
        }

        @Override
        public void updateStats(long allocated, long peak) {
            this.allocatedBytes = allocated;
            this.peakBytes = peak;
        }

        void setLimit(long newLimit) {
            this.limit = newLimit;
            if (limitSetter != null) {
                limitSetter.accept(newLimit);
            }
        }

        @Override
        public long allocatedBytes() {
            return allocatedBytes;
        }

        @Override
        public long peakBytes() {
            return peakBytes;
        }

        @Override
        public long limit() {
            return limit;
        }
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
