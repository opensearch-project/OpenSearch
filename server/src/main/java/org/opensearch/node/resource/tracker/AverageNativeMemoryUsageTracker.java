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
import java.lang.management.MemoryMXBean;
import java.util.function.LongSupplier;

/**
 * AverageNativeMemoryUsageTracker reports this OpenSearch process's off-heap native memory
 * utilization as a percentage of a node-level budget, averaged over a rolling window.
 *
 * <p>On Linux, each polling cycle computes
 * {@code usage = max(0, RssAnon - HeapCommitted - NonHeapCommitted)} where {@code RssAnon}
 * comes from {@link OsProbe#getProcessRssAnon()}, {@code HeapCommitted} and
 * {@code NonHeapCommitted} (metaspace + code cache) come from the JVM memory MX bean.
 * Subtracting non-heap removes fixed JVM overhead so the metric reflects actual native
 * allocations (Arrow buffers, jemalloc/DataFusion, direct byte buffers).
 *
 * <p>The denominator is the effective native memory budget:
 * {@code effective = limit - (limit * bufferPercent / 100)}. The limit is resolved from
 * {@link ResourceTrackerSettings#NODE_NATIVE_MEMORY_LIMIT_SETTING}. When not explicitly
 * configured, the limit is auto-derived as {@code totalPhysicalMemory - jvmMaxHeap}.
 *
 * <p>Activation is already gated to Linux in {@link NodeResourceUsageTracker}; on non-Linux
 * platforms the polling loop is not started and {@link OsProbe#getProcessRssAnon()} returns
 * {@code -1} without touching the filesystem.
 */
public class AverageNativeMemoryUsageTracker extends AbstractAverageUsageTracker {

    private static final Logger LOGGER = LogManager.getLogger(AverageNativeMemoryUsageTracker.class);

    private final LongSupplier rssAnonSupplier;
    private final LongSupplier heapCommittedSupplier;
    private final LongSupplier nonHeapCommittedSupplier;
    private final LongSupplier effectiveNativeMemorySupplier;

    /**
     * Production constructor. Wires the RSS reader to {@link OsProbe#getProcessRssAnon()}, the
     * heap-committed supplier to the JVM memory MX bean, and the effective-native-memory supplier
     * to {@link #computeEffectiveNativeMemory(ResourceTrackerSettings)} so dynamic updates to
     * {@link ResourceTrackerSettings#getNativeMemoryLimitBytes()} and
     * {@link ResourceTrackerSettings#getNativeMemoryBufferPercent()} are observed on the next
     * polling cycle. When the limit is not explicitly configured, the effective budget is
     * auto-derived as {@code totalPhysicalMemory - jvmMaxHeap}.
     */
    public AverageNativeMemoryUsageTracker(
        ThreadPool threadPool,
        TimeValue pollingInterval,
        TimeValue windowDuration,
        ResourceTrackerSettings resourceTrackerSettings
    ) {
        super(threadPool, pollingInterval, windowDuration);
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        this.rssAnonSupplier = () -> OsProbe.getInstance().getProcessRssAnon();
        this.heapCommittedSupplier = () -> memoryMXBean.getHeapMemoryUsage().getCommitted();
        this.nonHeapCommittedSupplier = () -> memoryMXBean.getNonHeapMemoryUsage().getCommitted();
        this.effectiveNativeMemorySupplier = () -> computeEffectiveNativeMemory(resourceTrackerSettings);
    }

    /**
     * Package-private test constructor. Accepts suppliers in place of the production defaults
     * so tests can drive {@link #getUsage()} deterministically without reading
     * {@code /proc/self/status}, the JVM memory MX bean, or node settings.
     */
    AverageNativeMemoryUsageTracker(
        ThreadPool threadPool,
        TimeValue pollingInterval,
        TimeValue windowDuration,
        LongSupplier rssAnonSupplier,
        LongSupplier heapCommittedSupplier,
        LongSupplier nonHeapCommittedSupplier,
        LongSupplier effectiveNativeMemorySupplier
    ) {
        super(threadPool, pollingInterval, windowDuration);
        this.rssAnonSupplier = rssAnonSupplier;
        this.heapCommittedSupplier = heapCommittedSupplier;
        this.nonHeapCommittedSupplier = nonHeapCommittedSupplier;
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
        long nonHeapCommitted = nonHeapCommittedSupplier.getAsLong();
        long nativeUsed = Math.max(0L, rssAnon - heapCommitted - nonHeapCommitted);

        long effectiveNativeMemory = effectiveNativeMemorySupplier.getAsLong();
        if (effectiveNativeMemory <= 0L) {
            LOGGER.debug("Native memory tracking inactive: node.native_memory.limit not configured and auto-detection unavailable");
            return 0L;
        }

        double utilization = (double) nativeUsed / effectiveNativeMemory * 100.0;
        long percent = Math.min(100L, Math.max(0L, (long) utilization));

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Native memory poll: rssAnon={} heapCommitted={} nonHeapCommitted={} nativeUsed={} effectiveNativeMemory={} pct={}",
                rssAnon,
                heapCommitted,
                nonHeapCommitted,
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
     * <p>
     * When the limit is not explicitly configured (zero), auto-derives the budget as
     * {@code totalPhysicalMemory - jvmMaxHeap}. Returns {@code 0L} only when neither explicit
     * configuration nor auto-detection yields a usable budget.
     */
    long computeEffectiveNativeMemory(ResourceTrackerSettings resourceTrackerSettings) {
        long limit = resourceTrackerSettings.getNativeMemoryLimitBytes();
        if (limit <= 0L) {
            long totalPhysicalMemory = OsProbe.getInstance().getTotalPhysicalMemorySize();
            long jvmMaxHeap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
            if (totalPhysicalMemory > 0L && jvmMaxHeap > 0L && totalPhysicalMemory > jvmMaxHeap) {
                limit = totalPhysicalMemory - jvmMaxHeap;
            } else {
                return 0L;
            }
        }
        int bufferPercent = resourceTrackerSettings.getNativeMemoryBufferPercent();
        long buffer = limit * bufferPercent / 100L;
        return Math.max(0L, limit - buffer);
    }
}
