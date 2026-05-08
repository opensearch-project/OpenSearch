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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.threadpool.ThreadPool;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * AverageNativeMemoryUsageTracker reports this OpenSearch process's off-heap native memory
 * utilization as a percentage of the budget configured for the native analytics engines,
 * averaged over a rolling window.
 *
 * <p>On Linux, each polling cycle computes
 * {@code usage = max(0, RssAnon - HeapCommitted)} where {@code RssAnon} comes from
 * {@link OsProbe#getProcessRssAnon()} and {@code HeapCommitted} comes from the JVM memory MX
 * bean. The denominator is a 20% padded sum of the two plugin budgets:
 * {@code cap = 1.2 * (datafusion.memory_pool_limit_bytes + resolved(parquet.max_native_allocation))}.
 * When either plugin setting is absent, zero, negative, or unparseable, its contribution is 0;
 * when both contribute 0 the cap is 0 and {@code getUsage()} returns 0 without dividing.
 *
 * <p>Activation is already gated to Linux in {@link NodeResourceUsageTracker}; on non-Linux
 * platforms the polling loop is not started and {@link OsProbe#getProcessRssAnon()} returns
 * {@code -1} without touching the filesystem.
 */
public class AverageNativeMemoryUsageTracker extends AbstractAverageUsageTracker {

    private static final Logger LOGGER = LogManager.getLogger(AverageNativeMemoryUsageTracker.class);

    static final String DATAFUSION_MEMORY_POOL_LIMIT_KEY = "datafusion.memory_pool_limit_bytes";
    static final String PARQUET_MAX_NATIVE_ALLOCATION_KEY = "parquet.max_native_allocation";
    static final double HEADROOM_FACTOR = 1.2;

    private final LongSupplier rssAnonSupplier;
    private final LongSupplier heapCommittedSupplier;
    private final LongSupplier nativeMemoryCapSupplier;

    /**
     * Production constructor. Wires the RSS reader to {@link OsProbe#getProcessRssAnon()}, the
     * heap-committed supplier to the JVM memory MX bean, and the native-memory cap supplier to
     * {@link #computeNativeMemoryCap(Settings)} so dynamic updates to
     * {@code datafusion.memory_pool_limit_bytes} are observed on the next polling cycle.
     */
    public AverageNativeMemoryUsageTracker(ThreadPool threadPool, TimeValue pollingInterval, TimeValue windowDuration, Settings settings) {
        super(threadPool, pollingInterval, windowDuration);
        this.rssAnonSupplier = () -> OsProbe.getInstance().getProcessRssAnon();
        this.heapCommittedSupplier = () -> ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getCommitted();
        this.nativeMemoryCapSupplier = () -> computeNativeMemoryCap(settings);
    }

    /**
     * Package-private test constructor. Accepts three {@link LongSupplier}s in place of the
     * production defaults so tests can drive {@link #getUsage()} deterministically without
     * reading {@code /proc/self/status}, the JVM memory MX bean, or plugin settings.
     */
    AverageNativeMemoryUsageTracker(
        ThreadPool threadPool,
        TimeValue pollingInterval,
        TimeValue windowDuration,
        LongSupplier rssAnonSupplier,
        LongSupplier heapCommittedSupplier,
        LongSupplier nativeMemoryCapSupplier
    ) {
        super(threadPool, pollingInterval, windowDuration);
        this.rssAnonSupplier = rssAnonSupplier;
        this.heapCommittedSupplier = heapCommittedSupplier;
        this.nativeMemoryCapSupplier = nativeMemoryCapSupplier;
    }

    @Override
    public long getUsage() {

        long rssAnon = rssAnonSupplier.getAsLong();
        if (rssAnon < 0L) {
            LOGGER.debug("Native memory poll skipped: RssAnon unavailable from /proc/self/status");
            return 0L;
        }

        long heapCommitted = heapCommittedSupplier.getAsLong();
        long usage = Math.max(0L, rssAnon - heapCommitted);

        long cap = nativeMemoryCapSupplier.getAsLong();
        if (cap <= 0L) {
            LOGGER.debug("Native memory poll: rssAnon={} heapCommitted={} usage={} cap=0 -> 0%", rssAnon, heapCommitted, usage);
            return 0L;
        }

        long percent = (usage * 100L) / cap;
        if (percent > 100L) {
            percent = 100L;
        }
        if (percent < 0L) {
            percent = 0L;
        }

        LOGGER.debug("Native memory poll: rssAnon={} heapCommitted={} usage={} cap={} pct={}", rssAnon, heapCommitted, usage, cap, percent);
        return percent;
    }


    /**
     * Computes {@code Non_Heap_Base = max(0, totalPhysicalMemory - Runtime.maxMemory())}, the
     * reference value against which {@code parquet.max_native_allocation} percentages resolve.
     */
    long computeNonHeapBase() {
        return Math.max(0L, OsProbe.getInstance().getTotalPhysicalMemorySize() - Runtime.getRuntime().maxMemory());
    }

    /**
     * Resolves the DataFusion native-pool contribution. Absent, zero, negative, or unparseable
     * values all collapse to {@code 0L}.
     */
    long resolveDataFusionContribution(Settings settings) {
        try {
            long value = settings.getAsLong(DATAFUSION_MEMORY_POOL_LIMIT_KEY, 0L);
            return value > 0L ? value : 0L;
        } catch (IllegalArgumentException unparseable) {
            return 0L;
        }
    }

    /**
     * Resolves the Parquet native-allocation contribution from the percentage-string setting.
     * Absent, empty, missing trailing {@code %}, unparseable, NaN, or non-positive values all
     * collapse to {@code 0L}. Percentages above 100 are defensively clamped to 100.
     */
    long resolveParquetContribution(Settings settings, long nonHeapBase) {
        if (nonHeapBase <= 0L) {
            return 0L;
        }
        String raw = settings.get(PARQUET_MAX_NATIVE_ALLOCATION_KEY);
        if (raw == null) {
            return 0L;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty() || trimmed.endsWith("%") == false) {
            return 0L;
        }
        String pctStr = trimmed.substring(0, trimmed.length() - 1).trim();
        double pct;
        try {
            pct = Double.parseDouble(pctStr);
        } catch (NumberFormatException nfe) {
            return 0L;
        }
        if (Double.isNaN(pct) || pct <= 0.0) {
            return 0L;
        }
        if (pct > 100.0) {
            pct = 100.0;
        }
        return (long) Math.floor(nonHeapBase * pct / 100.0);
    }

    /**
     * Resolves both plugin settings to byte values and returns the 20% headroom-padded sum.
     * Returns {@code 0L} when neither plugin setting contributes a positive value, so
     * {@link #getUsage()} can short-circuit without dividing.
     */
    long computeNativeMemoryCap(Settings settings) {
        long df = resolveDataFusionContribution(settings);
        long base = computeNonHeapBase();
        long pq = resolveParquetContribution(settings, base);
        long sum = df + pq;
        if (sum <= 0L) {
            return 0L;
        }
        return (long) Math.floor(HEADROOM_FACTOR * (double) sum);
    }
}
