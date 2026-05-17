/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.resource.tracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.threadpool.ThreadPool;

import java.lang.management.ManagementFactory;
import java.util.function.LongSupplier;

/**
 * AverageNativeMemoryUsageTracker reports this OpenSearch process's off-heap native memory
 * utilization as a percentage of a configured node-level budget, averaged over a rolling window.
 *
 * <p>On Linux, each polling cycle computes
 * {@code usage = max(0, RssAnon - HeapCommitted)} where {@code RssAnon} comes from
 * {@link OsProbe#getProcessRssAnon()} and {@code HeapCommitted} comes from the JVM memory MX
 * bean. The denominator is the effective native memory budget:
 * {@code effective = limit - (limit * bufferPercent / 100)}, resolved per poll from the
 * {@code volatile} fields in {@link ResourceTrackerSettings} that are updated by the owning
 * {@link NodeResourceUsageTracker} via cluster-settings update consumers registered against
 * {@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_LIMIT_SETTING} and
 * {@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_BUFFER_PERCENT_SETTING}. When the limit is
 * absent or non-positive, or the buffer fully consumes the limit, {@code getUsage()} returns
 * {@code 0} without dividing.
 *
 * <p>Activation is already gated to Linux in {@link NodeResourceUsageTracker}; on non-Linux
 * platforms the polling loop is not started and {@link OsProbe#getProcessRssAnon()} returns
 * {@code -1} without touching the filesystem.
 */
public class AverageNativeMemoryUsageTracker extends AbstractAverageUsageTracker {

    private static final Logger LOGGER = LogManager.getLogger(AverageNativeMemoryUsageTracker.class);

    private final LongSupplier rssAnonSupplier;
    private final LongSupplier heapCommittedSupplier;
    private final LongSupplier effectiveNativeMemorySupplier;

    /**
     * Production constructor. Wires the RSS reader to {@link OsProbe#getProcessRssAnon()}, the
     * heap-committed supplier to the JVM memory MX bean, and the effective-native-memory supplier
     * to {@link #computeEffectiveNativeMemory(ResourceTrackerSettings)} so dynamic updates to
     * {@link ResourceTrackerSettings#getNativeMemoryLimitBytes()} and
     * {@link ResourceTrackerSettings#getNativeMemoryBufferPercent()} are observed on the next
     * polling cycle.
     */
    public AverageNativeMemoryUsageTracker(
        ThreadPool threadPool,
        TimeValue pollingInterval,
        TimeValue windowDuration,
        ResourceTrackerSettings resourceTrackerSettings
    ) {
        super(threadPool, pollingInterval, windowDuration);
        this.rssAnonSupplier = () -> OsProbe.getInstance().getProcessRssAnon();
        this.heapCommittedSupplier = () -> ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getCommitted();
        this.effectiveNativeMemorySupplier = () -> computeEffectiveNativeMemory(resourceTrackerSettings);
    }

    /**
     * Package-private test constructor. Accepts three {@link LongSupplier}s in place of the
     * production defaults so tests can drive {@link #getUsage()} deterministically without
     * reading {@code /proc/self/status}, the JVM memory MX bean, or node settings. The third
     * supplier provides the effective native memory (limit minus buffer) directly.
     */
    AverageNativeMemoryUsageTracker(
        ThreadPool threadPool,
        TimeValue pollingInterval,
        TimeValue windowDuration,
        LongSupplier rssAnonSupplier,
        LongSupplier heapCommittedSupplier,
        LongSupplier effectiveNativeMemorySupplier
    ) {
        super(threadPool, pollingInterval, windowDuration);
        this.rssAnonSupplier = rssAnonSupplier;
        this.heapCommittedSupplier = heapCommittedSupplier;
        this.effectiveNativeMemorySupplier = effectiveNativeMemorySupplier;
    }

    @Override
    public long getUsage() {

        long rssAnon = rssAnonSupplier.getAsLong();
        if (rssAnon < 0L) {
            LOGGER.warn("Native memory poll skipped: RssAnon unavailable from /proc/self/status");
            return 0L;
        }

        long heapCommitted = heapCommittedSupplier.getAsLong();
        long nativeUsed = Math.max(0L, rssAnon - heapCommitted);

        long effectiveNativeMemory = effectiveNativeMemorySupplier.getAsLong();
        if (effectiveNativeMemory <= 0L) {
            LOGGER.warn(
                "Native memory poll: rssAnon={} heapCommitted={} nativeUsed={} effectiveNativeMemory=0 -> 0%",
                rssAnon,
                heapCommitted,
                nativeUsed
            );
            return 0L;
        }

        double utilization = (double) nativeUsed / effectiveNativeMemory * 100.0;
        long percent = (long) utilization;
        if (percent > 100L) {
            percent = 100L;
        }
        if (percent < 0L) {
            percent = 0L;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Native memory poll: rssAnon={} heapCommitted={} nativeUsed={} effectiveNativeMemory={} pct={}",
                rssAnon,
                heapCommitted,
                nativeUsed,
                effectiveNativeMemory,
                percent
            );
        }

        return percent;
    }

    /**
     * Resolves the current native-memory limit and buffer percentage from {@link ResourceTrackerSettings}
     * and returns the effective native-memory budget in bytes: {@code limit - (limit * buffer / 100)}.
     * Returns {@code 0L} when the limit is absent or non-positive, or when the buffer fully
     * consumes the limit, so {@link #getUsage()} can short-circuit without dividing.
     */
    long computeEffectiveNativeMemory(ResourceTrackerSettings resourceTrackerSettings) {
        long limit = resourceTrackerSettings.getNativeMemoryLimitBytes();
        if (limit <= 0L) {
            return 0L;
        }
        int bufferPercent = resourceTrackerSettings.getNativeMemoryBufferPercent();
        long buffer = limit * bufferPercent / 100L;
        long effective = Math.max(0L, limit - buffer);
        return effective;
    }
}
