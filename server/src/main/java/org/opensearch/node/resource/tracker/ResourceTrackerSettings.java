/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.resource.tracker;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.monitor.os.OsProbe;

/**
 * Settings related to resource usage trackers such as polling interval, window duration etc
 */
public class ResourceTrackerSettings {

    private static class Defaults {
        /**
         * This is the default polling interval of usage trackers to get the resource utilization data
         */
        private static final long POLLING_INTERVAL_IN_MILLIS = 500;
        /**
         * This is the default window duration on which the average resource utilization values will be calculated
         */
        private static final long WINDOW_DURATION_IN_SECONDS = 30;
        /**
         * This is the default polling interval for IO usage tracker
         */
        private static final long IO_POLLING_INTERVAL_IN_MILLIS = 5000;
        /**
         * This is the default window duration for IO usage tracker on which the average resource utilization values will be calculated
         */
        private static final long IO_WINDOW_DURATION_IN_SECONDS = 120;
    }

    public static final Setting<TimeValue> GLOBAL_CPU_USAGE_AC_POLLING_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "node.resource.tracker.global_cpu_usage.polling_interval",
        TimeValue.timeValueMillis(Defaults.POLLING_INTERVAL_IN_MILLIS),
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING = Setting.positiveTimeSetting(
        "node.resource.tracker.global_cpu_usage.window_duration",
        TimeValue.timeValueSeconds(Defaults.WINDOW_DURATION_IN_SECONDS),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> GLOBAL_IO_USAGE_AC_POLLING_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "node.resource.tracker.global_io_usage.polling_interval",
        TimeValue.timeValueMillis(Defaults.IO_POLLING_INTERVAL_IN_MILLIS),
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING = Setting.positiveTimeSetting(
        "node.resource.tracker.global_io_usage.window_duration",
        TimeValue.timeValueSeconds(Defaults.IO_WINDOW_DURATION_IN_SECONDS),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> GLOBAL_JVM_USAGE_AC_POLLING_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "node.resource.tracker.global_jvmmp.polling_interval",
        TimeValue.timeValueMillis(Defaults.POLLING_INTERVAL_IN_MILLIS),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING = Setting.positiveTimeSetting(
        "node.resource.tracker.global_jvmmp.window_duration",
        TimeValue.timeValueSeconds(Defaults.WINDOW_DURATION_IN_SECONDS),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> GLOBAL_NATIVE_MEMORY_USAGE_AC_POLLING_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "node.resource.tracker.global_native_memory_usage.polling_interval",
        TimeValue.timeValueSeconds(2),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> GLOBAL_NATIVE_MEMORY_USAGE_AC_WINDOW_DURATION_SETTING = Setting.positiveTimeSetting(
        "node.resource.tracker.global_native_memory_usage.window_duration",
        TimeValue.timeValueSeconds(Defaults.WINDOW_DURATION_IN_SECONDS),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Absolute native-memory budget for this node, in bytes.
     *
     * <p><strong>Default: 79% of {@code RAM - JVM heap}.</strong> The JVM-aware
     * {@link OsProbe#getTotalPhysicalMemorySize()} returns the cgroup-limited RAM in
     * containers and total physical RAM on bare metal; subtracting {@link Runtime#maxMemory()}
     * (the configured {@code -Xmx}) gives the off-heap budget the operator implicitly has
     * available, and 79% of that is allocated to AC's tracker. The remaining ~21% is left
     * unmanaged for Lucene mmap, Liquid cache, OS page cache, and other native consumers
     * not bounded by the framework. Operators with predominantly analytics workloads can
     * raise this toward 100% of off-heap; search-heavy workloads needing more page cache
     * should lower it explicitly.
     *
     * <p>Falls back to {@link ByteSizeValue#ZERO} (unconfigured) when either probe is unreliable
     * — RAM probe returns 0 in some restricted containers, or {@code maxMemory()} is somehow
     * larger than RAM. With ZERO, the tracker reports {@code 0%} and admission control treats the
     * budget as unset; downstream framework defaults (root limit, pool maxes, DataFusion pool)
     * fall back to {@link Long#MAX_VALUE} unbounded mode, preserving pre-AC behaviour.
     */
    public static final Setting<ByteSizeValue> NODE_NATIVE_MEMORY_LIMIT_SETTING = Setting.byteSizeSetting(
        "node.native_memory.limit",
        s -> deriveNativeMemoryLimitDefault(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Computes the default for {@link #NODE_NATIVE_MEMORY_LIMIT_SETTING} as {@code RAM - JVM heap}.
     * Returns the bytes-as-string representation expected by the {@link Setting} parser.
     *
     * <p>Returns {@code "0b"} (unset, AC unconfigured) when:
     * <ul>
     *   <li>{@link OsProbe#getTotalPhysicalMemorySize()} returns {@code 0} (e.g. restricted
     *       containers where {@code /proc/meminfo} cgroup files are not readable)</li>
     *   <li>{@code -Xmx} (heap) is somehow {@code >=} reported physical RAM (degenerate config
     *       or container-aware probe disagreeing with the JVM heap)</li>
     * </ul>
     * Both fall-back paths preserve the pre-default opt-in semantics: AC stays unconfigured,
     * downstream framework caps stay unbounded.
     */
    static String deriveNativeMemoryLimitDefault() {
        long ram = OsProbe.getInstance().getTotalPhysicalMemorySize();
        long heap = Runtime.getRuntime().maxMemory();
        return deriveNativeMemoryLimitDefault(ram, heap);
    }

    /**
     * Pure overload of {@link #deriveNativeMemoryLimitDefault()} with the OS- and
     * JVM-derived inputs lifted to parameters so unit tests can exercise the fallback
     * branches deterministically without mocking {@link OsProbe} or {@link Runtime}.
     */
    static String deriveNativeMemoryLimitDefault(long ram, long heap) {
        if (ram <= 0 || heap <= 0 || heap >= ram) {
            return "0b";
        }
        // 79% of off-heap (RAM - heap) leaves ~21% headroom for unmanaged native consumers:
        // Lucene mmap, Liquid cache, OS page cache, sidecar processes. Matches the partitioning
        // model in PR #21732. Operators with predominantly analytics workloads can raise this
        // toward 100%; search-heavy workloads needing more page cache should lower it.
        long offHeap = ram - heap;
        return Long.toString(offHeap * 79 / 100) + "b";
    }

    /**
     * Percentage of the native-memory limit that is reserved as buffer (not usable). The effective
     * native memory the tracker divides against is {@code limit - (limit * bufferPercent / 100)}.
     */
    public static final Setting<Integer> NODE_NATIVE_MEMORY_BUFFER_PERCENT_SETTING = Setting.intSetting(
        "node.native_memory.buffer_percent",
        0,
        0,
        100,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile TimeValue cpuWindowDuration;
    private volatile TimeValue cpuPollingInterval;
    private volatile TimeValue memoryWindowDuration;
    private volatile TimeValue memoryPollingInterval;
    private volatile TimeValue ioWindowDuration;
    private volatile TimeValue ioPollingInterval;
    private volatile TimeValue nativeMemoryWindowDuration;
    private volatile TimeValue nativeMemoryPollingInterval;
    private volatile long nativeMemoryLimitBytes;
    private volatile int nativeMemoryBufferPercent;

    public ResourceTrackerSettings(Settings settings) {
        this.cpuPollingInterval = GLOBAL_CPU_USAGE_AC_POLLING_INTERVAL_SETTING.get(settings);
        this.cpuWindowDuration = GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.get(settings);
        this.memoryPollingInterval = GLOBAL_JVM_USAGE_AC_POLLING_INTERVAL_SETTING.get(settings);
        this.memoryWindowDuration = GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.get(settings);
        this.ioPollingInterval = GLOBAL_IO_USAGE_AC_POLLING_INTERVAL_SETTING.get(settings);
        this.ioWindowDuration = GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING.get(settings);
        this.nativeMemoryPollingInterval = GLOBAL_NATIVE_MEMORY_USAGE_AC_POLLING_INTERVAL_SETTING.get(settings);
        this.nativeMemoryWindowDuration = GLOBAL_NATIVE_MEMORY_USAGE_AC_WINDOW_DURATION_SETTING.get(settings);
        this.nativeMemoryLimitBytes = NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings).getBytes();
        this.nativeMemoryBufferPercent = NODE_NATIVE_MEMORY_BUFFER_PERCENT_SETTING.get(settings);
    }

    public TimeValue getCpuWindowDuration() {
        return this.cpuWindowDuration;
    }

    public TimeValue getCpuPollingInterval() {
        return cpuPollingInterval;
    }

    public TimeValue getMemoryPollingInterval() {
        return memoryPollingInterval;
    }

    public TimeValue getMemoryWindowDuration() {
        return memoryWindowDuration;
    }

    public TimeValue getIoPollingInterval() {
        return ioPollingInterval;
    }

    public TimeValue getIoWindowDuration() {
        return ioWindowDuration;
    }

    public void setCpuWindowDuration(TimeValue cpuWindowDuration) {
        this.cpuWindowDuration = cpuWindowDuration;
    }

    public void setMemoryWindowDuration(TimeValue memoryWindowDuration) {
        this.memoryWindowDuration = memoryWindowDuration;
    }

    public void setIoWindowDuration(TimeValue ioWindowDuration) {
        this.ioWindowDuration = ioWindowDuration;
    }

    public TimeValue getNativeMemoryPollingInterval() {
        return nativeMemoryPollingInterval;
    }

    public TimeValue getNativeMemoryWindowDuration() {
        return nativeMemoryWindowDuration;
    }

    public void setNativeMemoryWindowDuration(TimeValue nativeMemoryWindowDuration) {
        this.nativeMemoryWindowDuration = nativeMemoryWindowDuration;
    }

    public long getNativeMemoryLimitBytes() {
        return nativeMemoryLimitBytes;
    }

    public void setNativeMemoryLimitBytes(long nativeMemoryLimitBytes) {
        this.nativeMemoryLimitBytes = nativeMemoryLimitBytes;
    }

    public int getNativeMemoryBufferPercent() {
        return nativeMemoryBufferPercent;
    }

    public void setNativeMemoryBufferPercent(int nativeMemoryBufferPercent) {
        this.nativeMemoryBufferPercent = nativeMemoryBufferPercent;
    }
}
