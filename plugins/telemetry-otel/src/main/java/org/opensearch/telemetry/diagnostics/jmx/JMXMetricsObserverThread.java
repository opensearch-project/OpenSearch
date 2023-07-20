/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics.jmx;

import com.sun.management.ThreadMXBean;
import org.opensearch.telemetry.diagnostics.ThreadResourceObserver;
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
        Map<String, Measurement<Number>> measurements = new HashMap<>();
        for (JMXMetricType measurement : JMXMetricType.values()) {
            measurements.put(measurement.getName(),
                new Measurement<>(measurement.getName(), measurement.getValue(threadMXBean, t)));
        }
        return new DiagnosticMetric(measurements, null, measurementTime);
    }
}
