/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.arrow.spi.PoolGroup;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Periodic rebalancer that redistributes native memory across pools.
 *
 * <p>Algorithm:
 * <ul>
 *   <li>Pools start at their configured max on registration</li>
 *   <li>If no pool is under pressure, return early (no-op)</li>
 *   <li>Idle pools (utilization &lt; idle_threshold) are shrunk, never below min</li>
 *   <li>Pressured pools (utilization &gt; pressure_threshold) receive freed capacity, can exceed max</li>
 *   <li>Excess freed capacity is returned to idle pools proportionally</li>
 *   <li>Invariant: sum(effective_limits) &lt;= budget at all times</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class NativeMemoryRebalancer implements Runnable {

    private static final Logger logger = LogManager.getLogger(NativeMemoryRebalancer.class);

    private final ArrowNativeAllocator allocator;
    private final Supplier<Long> budgetSupplier;

    private volatile double pressureThreshold;
    private volatile double idleThreshold;
    private volatile double shrinkFactor;

    /**
     * Creates a new rebalancer.
     *
     * @param allocator the allocator managing all pools
     * @param budgetSupplier supplies the current budget value
     * @param pressureThreshold utilization above this triggers growth (default 0.75)
     * @param idleThreshold utilization below this means pool can give back capacity (default 0.50)
     * @param shrinkFactor factor to shrink idle pools by — new limit = limit * (1 - shrinkFactor) (default 0.10)
     */
    public NativeMemoryRebalancer(
        ArrowNativeAllocator allocator,
        Supplier<Long> budgetSupplier,
        double pressureThreshold,
        double idleThreshold,
        double shrinkFactor
    ) {
        this.allocator = allocator;
        this.budgetSupplier = budgetSupplier;
        this.pressureThreshold = pressureThreshold;
        this.idleThreshold = idleThreshold;
        this.shrinkFactor = shrinkFactor;
    }

    /**
     * Updates the pressure threshold dynamically.
     *
     * @param value new threshold (0.0 to 1.0)
     */
    public void setPressureThreshold(double value) {
        this.pressureThreshold = value;
    }

    /**
     * Updates the idle threshold dynamically.
     *
     * @param value new threshold (0.0 to 1.0)
     */
    public void setIdleThreshold(double value) {
        this.idleThreshold = value;
    }

    /**
     * Updates the shrink factor dynamically.
     *
     * @param value new factor (0.0 to 1.0)
     */
    public void setShrinkFactor(double value) {
        this.shrinkFactor = value;
    }

    /** The pressure threshold currently in effect. Exposed for tests. */
    double getPressureThreshold() {
        return pressureThreshold;
    }

    /** The idle threshold currently in effect. Exposed for tests. */
    double getIdleThreshold() {
        return idleThreshold;
    }

    /** The shrink factor currently in effect. Exposed for tests. */
    double getShrinkFactor() {
        return shrinkFactor;
    }

    @Override
    public void run() {
        try {
            rebalance();
        } catch (Exception e) {
            logger.warn("Rebalancer tick failed", e);
        }
    }

    void rebalance() {
        Set<String> allPools = allocator.getAllPoolNames();
        if (allPools.isEmpty()) return;

        long budget = budgetSupplier.get();
        if (budget <= 0 || budget == Long.MAX_VALUE) return;

        // Refresh stats from native layers
        allocator.refreshStats();

        // Snapshot per-pool state
        Map<String, PoolSnapshot> snapshots = new HashMap<>();
        for (String name : allPools) {
            long allocated = allocator.getAllocated(name);
            long effectiveLimit = allocator.getEffectiveLimit(name);
            long min = allocator.getPoolMin(name);
            long max = allocator.getPoolMax(name);
            double utilization = effectiveLimit > 0 ? (double) allocated / effectiveLimit : 0;
            snapshots.put(name, new PoolSnapshot(allocated, effectiveLimit, min, max, utilization));
        }

        // Identify pressured pools — if none, nothing to do
        Map<String, Long> desires = new HashMap<>();
        long totalDesired = 0;
        for (var entry : snapshots.entrySet()) {
            PoolSnapshot s = entry.getValue();
            if (s.utilization > pressureThreshold) {
                long desired = Math.max(1, (long) (s.allocated * 0.25));
                desires.put(entry.getKey(), desired);
                totalDesired += desired;
            }
        }
        if (totalDesired == 0) {
            logger.debug("Rebalancer: no pools under pressure, skipping");
            return;
        }

        // Shrink idle pools, floor at min
        long freedCapacity = 0;
        for (var entry : snapshots.entrySet()) {
            PoolSnapshot s = entry.getValue();
            if (s.utilization < idleThreshold) {
                long newLimit = Math.max((long) (s.effectiveLimit * (1.0 - shrinkFactor)), s.min);
                newLimit = Math.max(newLimit, s.allocated);
                if (newLimit < s.effectiveLimit) {
                    freedCapacity += s.effectiveLimit - newLimit;
                    allocator.setPoolEffectiveLimit(entry.getKey(), newLimit);
                    s.effectiveLimit = newLimit;
                }
            }
        }

        if (freedCapacity == 0) {
            logger.debug("Rebalancer: no capacity freed from idle pools");
            return;
        }

        // Distribute freed capacity to pressured pools (can exceed max)
        long totalGranted = 0;
        long grantCap = Math.min(freedCapacity, totalDesired);
        for (var entry : desires.entrySet()) {
            String name = entry.getKey();
            long desired = entry.getValue();
            PoolSnapshot s = snapshots.get(name);
            long grant = (long) ((double) grantCap * desired / totalDesired);
            grant = Math.min(grant, grantCap - totalGranted);
            if (grant > 0) {
                try {
                    long newLimit = s.effectiveLimit + grant;
                    allocator.setPoolEffectiveLimit(name, newLimit);
                    totalGranted += grant;
                    logger.debug("Rebalancer: grew pool [{}] by {} bytes to {} (max={})", name, grant, newLimit, s.max);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("Rebalancer: failed to grow pool [{}]", name), e);
                }
            }
        }

        // Return any excess freed capacity back to idle pools
        long excess = freedCapacity - totalGranted;
        if (excess > 0) {
            returnToIdlePools(snapshots, excess);
        }

        // Notify pool group listeners after all limit changes are complete
        for (PoolGroup group : PoolGroup.values()) {
            allocator.firePoolGroupListeners(group);
        }
    }

    // ─── Private helpers ─────────────────────────────────────────────────────────

    private void returnToIdlePools(Map<String, PoolSnapshot> snapshots, long capacity) {
        long totalIdleSize = 0;
        for (PoolSnapshot s : snapshots.values()) {
            if (s.utilization < idleThreshold) {
                totalIdleSize += s.effectiveLimit;
            }
        }
        if (totalIdleSize == 0) return;

        long totalReturned = 0;
        for (var entry : snapshots.entrySet()) {
            PoolSnapshot s = entry.getValue();
            if (s.utilization < idleThreshold) {
                long share = (long) ((double) capacity * s.effectiveLimit / totalIdleSize);
                share = Math.min(share, capacity - totalReturned);
                if (share > 0) {
                    long newLimit = s.effectiveLimit + share;
                    allocator.setPoolEffectiveLimit(entry.getKey(), newLimit);
                    s.effectiveLimit = newLimit;
                    totalReturned += share;
                }
            }
        }
    }

    /**
     * Point-in-time snapshot of a pool's state during one rebalance tick.
     */
    static class PoolSnapshot {
        final long allocated;
        long effectiveLimit;
        final long min;
        final long max;
        final double utilization;

        PoolSnapshot(long allocated, long effectiveLimit, long min, long max, double utilization) {
            this.allocated = allocated;
            this.effectiveLimit = effectiveLimit;
            this.min = min;
            this.max = max;
            this.utilization = utilization;
        }
    }
}
