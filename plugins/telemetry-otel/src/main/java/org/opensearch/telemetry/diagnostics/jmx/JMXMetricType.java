/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics.jmx;

import com.sun.management.ThreadMXBean;
import java.util.function.BiFunction;

/**
 * Enum for all JMX metrics
 */
public enum JMXMetricType {

    CPU_TIME("cpu_time", (threadMXBean, t) -> threadMXBean.getThreadCpuTime(t.getId())),
    ALLOCATED_BYTES("allocated_bytes", (threadMXBean, t) -> threadMXBean.getThreadAllocatedBytes(t.getId())),
    BLOCKED_COUNT("blocked_count", (threadMXBean, t) -> threadMXBean.getThreadInfo(t.getId()).getBlockedCount()),
    BLOCKED_TIME("blocked_time", (threadMXBean, t) -> threadMXBean.getThreadInfo(t.getId()).getBlockedTime()),
    WAITED_COUNT("waited_count", (threadMXBean, t) -> threadMXBean.getThreadInfo(t.getId()).getWaitedCount()),
    WAITED_TIME("waited_time", (threadMXBean, t) -> threadMXBean.getThreadInfo(t.getId()).getWaitedTime());

    private final String name;
    private final BiFunction<ThreadMXBean, Thread, Long> valueFunction;

    JMXMetricType(String name, BiFunction<ThreadMXBean, Thread, Long> valueFunction) {
        this.name = name;
        this.valueFunction = valueFunction;
    }

    public String getName() {
        return name;
    }

    public long getValue(ThreadMXBean threadMXBean, Thread t) {
        return valueFunction.apply(threadMXBean, t);
    }
}


