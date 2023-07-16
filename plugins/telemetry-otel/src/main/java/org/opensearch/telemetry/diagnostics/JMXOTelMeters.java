/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import org.opensearch.telemetry.diagnostics.metrics.DiagnosticMetric;
import org.opensearch.telemetry.diagnostics.metrics.MetricEmitter;

public class JMXOTelMeters implements MetricEmitter {

    private static JMXOTelMeters INSTANCE;

    private JMXOTelMeters(OpenTelemetry telemetry) {
        JMXOTelOperationMeters.meter = telemetry.getMeter(JMXOTelMeters.class.getName());
    }

    synchronized public static JMXOTelMeters getInstance(OpenTelemetry telemetry) {
        if (INSTANCE == null) {
            INSTANCE = new JMXOTelMeters(telemetry);
        }
        return INSTANCE;
    }

    public static class JMXOTelOperationMeters {
        public static LongHistogram cpuTime;
        public static DoubleHistogram cpuUtilization;
        public static LongHistogram heapAllocatedBytes;
        public static LongHistogram blockedCount;
        public static LongHistogram waitedCount;
        public static LongHistogram blockedTime;
        public static LongHistogram waitedTime;
        public static LongHistogram elapsedTime;
        protected static Meter meter;

        static  {
            cpuTime = meter.histogramBuilder("CPUTime").ofLongs().build();
            cpuUtilization = meter.histogramBuilder("CPUUtilization").build();
            heapAllocatedBytes = meter.histogramBuilder("HeapAllocatedBytes").ofLongs().build();
            blockedCount = meter.histogramBuilder("BlockedCount").ofLongs().build();
            waitedCount = meter.histogramBuilder("WaitedCount").ofLongs().build();
            blockedTime = meter.histogramBuilder("BlockedTime").ofLongs().build();
            waitedTime = meter.histogramBuilder("WaitedTime").ofLongs().build();
            elapsedTime = meter.histogramBuilder("ElapsedTime").ofLongs().build();
        }
    }

    @Override
    public void emitMetric(DiagnosticMetric metric) {
        AttributesBuilder attributesBuilder = Attributes.builder();
        metric.getAttributes().forEach(attributesBuilder::put);
        Attributes oTelAttributes = attributesBuilder.build();
        JMXOTelOperationMeters.cpuTime.record((long)metric.getMeasurement("cpu_time").getValue(), oTelAttributes);
        JMXOTelOperationMeters.heapAllocatedBytes.record((long)metric.getMeasurement("allocated_bytes").getValue(), oTelAttributes);
        JMXOTelOperationMeters.blockedCount.record((long)metric.getMeasurement("blocked_count").getValue(), oTelAttributes);
        JMXOTelOperationMeters.waitedCount.record((long)metric.getMeasurement("blocked_time").getValue(), oTelAttributes);
        JMXOTelOperationMeters.blockedTime.record((long)metric.getMeasurement("waited_count").getValue(), oTelAttributes);
        JMXOTelOperationMeters.waitedTime.record((long)metric.getMeasurement("waited_time").getValue(), oTelAttributes);
        // JMXOTelOperationMeters.cpuUtilization.record((long)metric.getMeasurement("cpu_utilization").getValue(), oTelAttributes);
        JMXOTelOperationMeters.elapsedTime.record((long)metric.getMeasurement("elapsed_time").getValue(), oTelAttributes);
        resetHistogram();
    }

    private static void resetHistogram() {
        JMXOTelOperationMeters.cpuTime.record(0);
        JMXOTelOperationMeters.heapAllocatedBytes.record(0);
        JMXOTelOperationMeters.blockedTime.record(0);
        JMXOTelOperationMeters.blockedCount.record(0);
        JMXOTelOperationMeters.waitedTime.record(0);
        JMXOTelOperationMeters.waitedCount.record(0);
        JMXOTelOperationMeters.cpuUtilization.record(0);
        JMXOTelOperationMeters.elapsedTime.record(0);
    }
}
