/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics.jmx;

import org.opensearch.telemetry.diagnostics.ThreadResourceRecorder;
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
        Map<String, Measurement<Number>> measurements = new HashMap<>();
        for (String measurementName : endMetric.getMeasurements().keySet()) {
            measurements.put(measurementName, new Measurement<>(measurementName,
                endMetric.getMeasurement(measurementName).getValue().longValue() -
                    startMetric.getMeasurement(measurementName).getValue().longValue()));
        }
        measurements.put("elapsed_time", new Measurement<>("elapsed_time", duration));
        return new DiagnosticMetric(measurements, null, endMetric.getObservationTime());
    }
}

