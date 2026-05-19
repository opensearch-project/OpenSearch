/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.monitor.process.ProcessProbe;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Registers pull-based gauges for node-level runtime metrics covering JVM memory
 * (heap, non-heap, per-pool), GC collectors, buffer pools, threads (including
 * per-state counts), class loading, uptime, and CPU usage.
 *
 * <p>All JVM gauge suppliers read through {@link JvmService#stats()}, which caches
 * the {@link JvmStats} snapshot with a 1-second TTL. A single collection sweep
 * reuses one snapshot across all gauges with no redundant MXBean calls.
 *
 * <p>Memory pool, GC collector, and buffer pool gauges are discovered dynamically
 * from the initial {@link JvmStats} snapshot and tagged by name, so they work
 * identically across G1, Parallel, CMS, ZGC, and other collector implementations.
 *
 * @opensearch.internal
 */
public class NodeRuntimeMetrics implements Closeable {

    private static final Logger logger = LogManager.getLogger(NodeRuntimeMetrics.class);

    // Units
    static final String UNIT_BYTES = "bytes";
    static final String UNIT_SECONDS = "s";
    static final String UNIT_1 = "1";

    // Tag keys
    static final String TAG_TYPE = "type";
    static final String TAG_POOL = "pool";
    static final String TAG_GC = "gc";
    static final String TAG_STATE = "state";

    // Memory
    static final String JVM_MEMORY_USED = "jvm.memory.used";
    static final String JVM_MEMORY_COMMITTED = "jvm.memory.committed";
    static final String JVM_MEMORY_LIMIT = "jvm.memory.limit";
    static final String JVM_MEMORY_USED_AFTER_LAST_GC = "jvm.memory.used_after_last_gc";

    // GC
    static final String JVM_GC_DURATION = "jvm.gc.duration";
    static final String JVM_GC_COUNT = "jvm.gc.count";

    // Buffer pools
    static final String JVM_BUFFER_MEMORY_USED = "jvm.buffer.memory.used";
    static final String JVM_BUFFER_MEMORY_LIMIT = "jvm.buffer.memory.limit";
    static final String JVM_BUFFER_COUNT = "jvm.buffer.count";

    // Threads
    static final String JVM_THREAD_COUNT = "jvm.thread.count";

    // Classes
    static final String JVM_CLASS_COUNT = "jvm.class.count";
    static final String JVM_CLASS_LOADED = "jvm.class.loaded";
    static final String JVM_CLASS_UNLOADED = "jvm.class.unloaded";

    // CPU
    static final String JVM_CPU_RECENT_UTILIZATION = "jvm.cpu.recent_utilization";
    static final String JVM_SYSTEM_CPU_UTILIZATION = "jvm.system.cpu.utilization";

    // Uptime
    static final String JVM_UPTIME = "jvm.uptime";

    private final JvmService jvmService;
    private final List<Closeable> gaugeHandles = new ArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    // Thread state snapshot cache — shared across all per-state gauges
    private final ThreadMXBean threadMXBean;
    private long[] threadStateCounts = new long[Thread.State.values().length];
    private long threadStateTimestamp;
    private static final long CACHE_TTL_MS = 1000;

    public NodeRuntimeMetrics(MetricsRegistry registry, JvmService jvmService, ProcessProbe processProbe, OsProbe osProbe) {
        this(registry, jvmService, processProbe, osProbe, ManagementFactory.getThreadMXBean());
    }

    NodeRuntimeMetrics(
        MetricsRegistry registry,
        JvmService jvmService,
        ProcessProbe processProbe,
        OsProbe osProbe,
        ThreadMXBean threadMXBean
    ) {
        this.jvmService = Objects.requireNonNull(jvmService, "jvmService");
        this.threadMXBean = Objects.requireNonNull(threadMXBean, "threadMXBean");
        Objects.requireNonNull(registry, "registry");
        Objects.requireNonNull(processProbe, "processProbe");
        Objects.requireNonNull(osProbe, "osProbe");

        try {
            registerMemoryGauges(registry);
            registerGcGauges(registry);
            registerBufferPoolGauges(registry);
            registerThreadGauges(registry);
            registerClassGauges(registry);
            registerUptimeGauge(registry);
            registerCpuGauges(registry, processProbe, osProbe);
        } catch (Exception e) {
            closeQuietly();
            throw e;
        }

        logger.debug("Registered {} node runtime metric gauges", gaugeHandles.size());
    }

    // ---- Memory (heap aggregate, non-heap aggregate, per-pool) ----

    private void registerMemoryGauges(MetricsRegistry registry) {
        Tags heapTags = Tags.of(TAG_TYPE, "heap");
        Tags nonHeapTags = Tags.of(TAG_TYPE, "non_heap");

        gaugeHandles.add(
            registry.createGauge(
                JVM_MEMORY_USED,
                "JVM heap memory used",
                UNIT_BYTES,
                () -> (double) jvmService.stats().getMem().getHeapUsed().getBytes(),
                heapTags
            )
        );
        gaugeHandles.add(
            registry.createGauge(
                JVM_MEMORY_COMMITTED,
                "JVM heap memory committed",
                UNIT_BYTES,
                () -> (double) jvmService.stats().getMem().getHeapCommitted().getBytes(),
                heapTags
            )
        );
        gaugeHandles.add(
            registry.createGauge(
                JVM_MEMORY_LIMIT,
                "JVM heap memory max",
                UNIT_BYTES,
                () -> (double) jvmService.stats().getMem().getHeapMax().getBytes(),
                heapTags
            )
        );
        gaugeHandles.add(
            registry.createGauge(
                JVM_MEMORY_USED,
                "JVM non-heap memory used",
                UNIT_BYTES,
                () -> (double) jvmService.stats().getMem().getNonHeapUsed().getBytes(),
                nonHeapTags
            )
        );
        gaugeHandles.add(
            registry.createGauge(
                JVM_MEMORY_COMMITTED,
                "JVM non-heap memory committed",
                UNIT_BYTES,
                () -> (double) jvmService.stats().getMem().getNonHeapCommitted().getBytes(),
                nonHeapTags
            )
        );

        // Per-pool gauges
        JvmStats stats = jvmService.stats();
        for (JvmStats.MemoryPool pool : stats.getMem()) {
            String poolName = pool.getName();
            Tags poolTags = Tags.of(TAG_POOL, poolName);
            gaugeHandles.add(registry.createGauge(JVM_MEMORY_USED, "JVM memory pool used", UNIT_BYTES, () -> {
                JvmStats.MemoryPool p = getPoolByName(poolName);
                return p == null ? 0.0 : (double) p.getUsed().getBytes();
            }, poolTags));
            gaugeHandles.add(registry.createGauge(JVM_MEMORY_LIMIT, "JVM memory pool max", UNIT_BYTES, () -> {
                JvmStats.MemoryPool p = getPoolByName(poolName);
                if (p == null) return 0.0;
                long bytes = p.getMax().getBytes();
                return bytes < 0 ? 0.0 : (double) bytes;
            }, poolTags));
            gaugeHandles.add(registry.createGauge(JVM_MEMORY_USED_AFTER_LAST_GC, "JVM memory pool used after last GC", UNIT_BYTES, () -> {
                JvmStats.MemoryPool p = getPoolByName(poolName);
                return p == null ? 0.0 : (double) p.getLastGcStats().getUsed().getBytes();
            }, poolTags));
        }
    }

    private JvmStats.MemoryPool getPoolByName(String poolName) {
        for (JvmStats.MemoryPool pool : jvmService.stats().getMem()) {
            if (pool.getName().equals(poolName)) {
                return pool;
            }
        }
        return null;
    }

    // ---- GC (cumulative gauges) ----
    // TODO: Add event-driven GC pause duration histograms via GarbageCollectorMXBean
    // notification listeners. Cumulative gauges give rate-of-GC-time; histograms would
    // enable percentile analysis of individual pause durations (p50, p99).

    private void registerGcGauges(MetricsRegistry registry) {
        JvmStats stats = jvmService.stats();
        for (JvmStats.GarbageCollector gc : stats.getGc()) {
            String collectorName = gc.getName();
            Tags gcTags = Tags.of(TAG_GC, collectorName);
            gaugeHandles.add(registry.createGauge(JVM_GC_DURATION, "GC cumulative collection time", UNIT_SECONDS, () -> {
                JvmStats.GarbageCollector c = getCollectorByName(collectorName);
                return c == null ? 0.0 : c.getCollectionTime().getMillis() / 1000.0;
            }, gcTags));
            gaugeHandles.add(registry.createGauge(JVM_GC_COUNT, "GC collection count", UNIT_1, () -> {
                JvmStats.GarbageCollector c = getCollectorByName(collectorName);
                return c == null ? 0.0 : (double) c.getCollectionCount();
            }, gcTags));
        }
    }

    private JvmStats.GarbageCollector getCollectorByName(String collectorName) {
        for (JvmStats.GarbageCollector gc : jvmService.stats().getGc()) {
            if (gc.getName().equals(collectorName)) {
                return gc;
            }
        }
        return null;
    }

    // ---- Buffer pools ----

    private void registerBufferPoolGauges(MetricsRegistry registry) {
        JvmStats stats = jvmService.stats();
        for (JvmStats.BufferPool bp : stats.getBufferPools()) {
            String bpName = bp.getName();
            Tags bpTags = Tags.of(TAG_POOL, bpName);
            gaugeHandles.add(registry.createGauge(JVM_BUFFER_MEMORY_USED, "Buffer pool memory used", UNIT_BYTES, () -> {
                JvmStats.BufferPool b = getBufferPoolByName(bpName);
                return b == null ? 0.0 : (double) b.getUsed().getBytes();
            }, bpTags));
            gaugeHandles.add(registry.createGauge(JVM_BUFFER_MEMORY_LIMIT, "Buffer pool total capacity", UNIT_BYTES, () -> {
                JvmStats.BufferPool b = getBufferPoolByName(bpName);
                return b == null ? 0.0 : (double) b.getTotalCapacity().getBytes();
            }, bpTags));
            gaugeHandles.add(registry.createGauge(JVM_BUFFER_COUNT, "Buffer pool buffer count", UNIT_1, () -> {
                JvmStats.BufferPool b = getBufferPoolByName(bpName);
                return b == null ? 0.0 : (double) b.getCount();
            }, bpTags));
        }
    }

    private JvmStats.BufferPool getBufferPoolByName(String bpName) {
        for (JvmStats.BufferPool bp : jvmService.stats().getBufferPools()) {
            if (bp.getName().equals(bpName)) {
                return bp;
            }
        }
        return null;
    }

    // ---- Threads ----

    private void registerThreadGauges(MetricsRegistry registry) {
        gaugeHandles.add(
            registry.createGauge(
                JVM_THREAD_COUNT,
                "JVM thread count",
                UNIT_1,
                () -> (double) jvmService.stats().getThreads().getCount(),
                Tags.EMPTY
            )
        );

        for (Thread.State state : Thread.State.values()) {
            String stateName = state.name().toLowerCase(Locale.ROOT);
            Tags stateTags = Tags.of(TAG_STATE, stateName);
            gaugeHandles.add(
                registry.createGauge(
                    JVM_THREAD_COUNT,
                    "JVM threads in this state",
                    UNIT_1,
                    () -> (double) getThreadStateCount(state),
                    stateTags
                )
            );
        }
    }

    /**
     * Returns the count of threads in the given state, using a cached snapshot
     * that is refreshed at most once per second. This ensures that during a
     * single collection sweep all per-state gauges share one getThreadInfo() call.
     */
    private synchronized long getThreadStateCount(Thread.State state) {
        long now = System.currentTimeMillis();
        if (now - threadStateTimestamp > CACHE_TTL_MS) {
            refreshThreadStateCounts();
            threadStateTimestamp = now;
        }
        return threadStateCounts[state.ordinal()];
    }

    private void refreshThreadStateCounts() {
        long[] counts = new long[Thread.State.values().length];
        try {
            long[] threadIds = threadMXBean.getAllThreadIds();
            ThreadInfo[] infos = threadMXBean.getThreadInfo(threadIds);
            for (ThreadInfo info : infos) {
                if (info != null) {
                    counts[info.getThreadState().ordinal()]++;
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to collect thread state counts", e);
        }
        threadStateCounts = counts;
    }

    // ---- Classes ----

    private void registerClassGauges(MetricsRegistry registry) {
        gaugeHandles.add(
            registry.createGauge(
                JVM_CLASS_COUNT,
                "Currently loaded class count",
                UNIT_1,
                () -> (double) jvmService.stats().getClasses().getLoadedClassCount(),
                Tags.EMPTY
            )
        );
        gaugeHandles.add(
            registry.createGauge(
                JVM_CLASS_LOADED,
                "Total loaded class count since JVM start",
                UNIT_1,
                () -> (double) jvmService.stats().getClasses().getTotalLoadedClassCount(),
                Tags.EMPTY
            )
        );
        gaugeHandles.add(
            registry.createGauge(
                JVM_CLASS_UNLOADED,
                "Total unloaded class count since JVM start",
                UNIT_1,
                () -> (double) jvmService.stats().getClasses().getUnloadedClassCount(),
                Tags.EMPTY
            )
        );
    }

    // ---- Uptime ----

    private void registerUptimeGauge(MetricsRegistry registry) {
        gaugeHandles.add(
            registry.createGauge(
                JVM_UPTIME,
                "JVM uptime",
                UNIT_SECONDS,
                () -> jvmService.stats().getUptime().getMillis() / 1000.0,
                Tags.EMPTY
            )
        );
    }

    // ---- CPU ----

    private void registerCpuGauges(MetricsRegistry registry, ProcessProbe processProbe, OsProbe osProbe) {
        gaugeHandles.add(
            registry.createGauge(
                JVM_CPU_RECENT_UTILIZATION,
                "Recent JVM CPU utilization",
                UNIT_1,
                () -> clampCpuPercent(processProbe.getProcessCpuPercent()),
                Tags.EMPTY
            )
        );
        gaugeHandles.add(
            registry.createGauge(
                JVM_SYSTEM_CPU_UTILIZATION,
                "System CPU utilization",
                UNIT_1,
                () -> clampCpuPercent(osProbe.getSystemCpuPercent()),
                Tags.EMPTY
            )
        );
    }

    private static double clampCpuPercent(short pct) {
        return pct < 0 ? 0.0 : pct / 100.0;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        closeQuietly();
    }

    private void closeQuietly() {
        for (Closeable handle : gaugeHandles) {
            try {
                handle.close();
            } catch (IOException e) {
                logger.debug("Failed to close gauge handle", e);
            }
        }
        gaugeHandles.clear();
    }
}
