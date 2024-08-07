/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics.exporter;

import io.opentelemetry.exporter.logging.otlp.OtlpJsonLoggingMetricExporter;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.OTelTelemetrySettings;
import org.opensearch.test.OpenSearchTestCase;

import io.opentelemetry.exporter.logging.OtlpJsonLoggingMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;

public class OTelMetricsExporterFactoryTests extends OpenSearchTestCase {

    public void testMetricsExporterDefault() {
        Settings settings = Settings.builder().build();
        MetricExporter metricExporter = OTelMetricsExporterFactory.create(settings);
        assertTrue(metricExporter instanceof OtlpJsonLoggingMetricExporter);
    }

    public void testMetricsExporterLogging() {
        Settings settings = Settings.builder()
            .put(
                OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(),
                "io.opentelemetry.exporter.logging.OtlpJsonLoggingMetricExporter"
            )
            .build();
        MetricExporter metricExporter = OTelMetricsExporterFactory.create(settings);
        assertTrue(metricExporter instanceof OtlpJsonLoggingMetricExporter);
    }

    public void testMetricExporterInvalid() {
        Settings settings = Settings.builder().put(OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(), "abc").build();
        assertThrows(IllegalArgumentException.class, () -> OTelMetricsExporterFactory.create(settings));
    }

    public void testMetricExporterNoCreateFactoryMethod() {
        Settings settings = Settings.builder()
            .put(
                OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(),
                "org.opensearch.telemetry.metrics.exporter.DummyMetricExporter"
            )
            .build();
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> OTelMetricsExporterFactory.create(settings));
        assertEquals(
            "MetricExporter instantiation failed for class [org.opensearch.telemetry.metrics.exporter.DummyMetricExporter]",
            exception.getMessage()
        );
    }

    public void testMetricExporterNonMetricExporterClass() {
        Settings settings = Settings.builder()
            .put(OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(), "java.lang.String")
            .build();
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> OTelMetricsExporterFactory.create(settings));
        assertEquals("MetricExporter instantiation failed for class [java.lang.String]", exception.getMessage());
        assertTrue(exception.getCause() instanceof NoSuchMethodError);

    }

    public void testMetricExporterGetDefaultMethod() {
        Settings settings = Settings.builder()
            .put(
                OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(),
                "io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter"
            )
            .build();

        assertTrue(OTelMetricsExporterFactory.create(settings) instanceof OtlpGrpcMetricExporter);
    }

}
