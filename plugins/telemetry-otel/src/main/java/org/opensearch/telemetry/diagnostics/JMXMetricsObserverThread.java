/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics;

import com.sun.management.ThreadMXBean;
import org.opensearch.telemetry.diagnostics.metrics.Measurement;
import org.opensearch.telemetry.diagnostics.metrics.DiagnosticMetric;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

public class JMXMetricsObserverThread implements ThreadResourceObserver {
    private static final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    @Override
    public DiagnosticMetric observe(Thread t) {
        long measurementTime = System.currentTimeMillis();
        Map<String, Measurement<?>> measurements = new HashMap<>();
        measurements.put("cpu_time", new Measurement<>("cpu_time", threadMXBean.getThreadCpuTime(t.getId())));
        measurements.put("allocated_bytes", new Measurement<>("allocated_bytes", threadMXBean.getThreadAllocatedBytes(t.getId())));

        //if (threadMXBean.isThreadContentionMonitoringEnabled()) {
        measurements.put("blocked_count", new Measurement<>("blocked_count", threadMXBean.getThreadInfo(t.getId()).getBlockedCount()));
        measurements.put("blocked_time", new Measurement<>("blocked_time", threadMXBean.getThreadInfo(t.getId()).getBlockedTime()));
        measurements.put("waited_count", new Measurement<>("waited_count", threadMXBean.getThreadInfo(t.getId()).getWaitedCount()));
        measurements.put("waited_time", new Measurement<>("waited_time", threadMXBean.getThreadInfo(t.getId()).getWaitedTime()));
        //}
        return new DiagnosticMetric(measurements, null, measurementTime);
    }
}
