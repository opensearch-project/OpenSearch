/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics.jmx;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import org.opensearch.telemetry.diagnostics.metrics.DiagnosticMetric;
import org.opensearch.telemetry.diagnostics.metrics.MetricEmitter;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

public class JMXOTelMetricEmitter implements MetricEmitter {
    private static JMXOTelMetricEmitter INSTANCE;
    public static Map<String, LongHistogram> histograms = new HashMap<>();
    private static Meter meter;
    private JMXOTelMetricEmitter(OpenTelemetry telemetry) {
        JMXOTelMetricEmitter.meter = telemetry.getMeter(JMXOTelMetricEmitter.class.getName());
        for (JMXMetricType metricType : JMXMetricType.values()) {
            LongHistogram histogram = AccessController.doPrivileged((PrivilegedAction<LongHistogram>) () ->
                meter.histogramBuilder(metricType.getName()).ofLongs().build());
            histograms.put(metricType.getName(), histogram);
        }
        histograms.put("elapsed_time", meter.histogramBuilder("elapsed_time").ofLongs().build());
    }

    synchronized public static JMXOTelMetricEmitter getInstance(OpenTelemetry telemetry) {
        if (INSTANCE == null) {
            INSTANCE = new JMXOTelMetricEmitter(telemetry);
        }
        return INSTANCE;
    }

    @Override
    public void emitMetric(DiagnosticMetric metric) {
        AttributesBuilder attributesBuilder = Attributes.builder();
        if (metric.getAttributes() != null) {
            metric.getAttributes().forEach((k,v) -> attributesBuilder.put(k,String.valueOf(v)));
        }
        Attributes oTelAttributes = attributesBuilder.build();
        for (String measurementName : metric.getMeasurements().keySet()) {
            LongHistogram histogram = JMXOTelMetricEmitter.histograms.get(measurementName);
            long value = metric.getMeasurement(measurementName).getValue().longValue();
            histogram.record(value, oTelAttributes);
        }
        resetHistogram();
    }

    private static void resetHistogram() {
        JMXOTelMetricEmitter.histograms.values().forEach(histogram -> histogram.record(0));
    }
}
