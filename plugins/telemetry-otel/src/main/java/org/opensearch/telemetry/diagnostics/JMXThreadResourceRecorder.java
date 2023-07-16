/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics;

import org.opensearch.telemetry.diagnostics.metrics.Measurement;
import org.opensearch.telemetry.diagnostics.metrics.DiagnosticMetric;

import java.util.HashMap;
import java.util.Map;

public class JMXThreadResourceRecorder extends ThreadResourceRecorder<JMXMetricsObserverThread> {

    public JMXThreadResourceRecorder(JMXMetricsObserverThread observer) {
        super(observer);
    }

    @Override
    protected DiagnosticMetric computeDiff(DiagnosticMetric startMetric, DiagnosticMetric endMetric) {
            long duration = endMetric.getObservationTime() - startMetric.getObservationTime();
            Map<String, Measurement<?>> measurements = new HashMap<>();
            measurements.put("cpu_time", new Measurement<>("cpu_time",
                (long) endMetric.getMeasurement("cpu_time").getValue() - (long) startMetric.getMeasurement("cpu_time").getValue()));

            measurements.put("allocated_bytes", new Measurement<>("allocated_bytes",
                (long) endMetric.getMeasurement("allocated_bytes").getValue() - (long) startMetric.getMeasurement("allocated_bytes").getValue()));

            measurements.put("blocked_count", new Measurement<>("blocked_count",
                (long) endMetric.getMeasurement("blocked_count").getValue() - (long) startMetric.getMeasurement("blocked_count").getValue()));

            measurements.put("blocked_time", new Measurement<>("blocked_time",
                (long) endMetric.getMeasurement("blocked_time").getValue() - (long) startMetric.getMeasurement("blocked_time").getValue()));

            measurements.put("waited_count", new Measurement<>("waited_count",
                (long) endMetric.getMeasurement("waited_count").getValue() - (long) startMetric.getMeasurement("waited_count").getValue()));

            measurements.put("waited_time", new Measurement<>("waited_time",
                (long) endMetric.getMeasurement("waited_time").getValue() - (long) startMetric.getMeasurement("waited_time").getValue()));

            // measurements.put("cpu_utilization", new Measurement<>("cpu_utilization",
            //    (((long)endMetric.getMeasurement("cpu_time").getValue() - (long)startMetric.getMeasurement("cpu_time").getValue())/ duration)*100));

            measurements.put("elapsed_time", new Measurement<>("elapsed_time", duration));

            return new DiagnosticMetric(measurements, null, endMetric.getObservationTime());
    }
}
