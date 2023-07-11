/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.exporter;

import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.OtelTelemetrySettings;
import org.opensearch.test.OpenSearchTestCase;

public class SpanExporterFactoryTests extends OpenSearchTestCase {

    private final SpanExporterFactory spanExporterFactory = new SpanExporterFactory();

    public void testSpanExporterDefault() {
        Settings settings = Settings.builder().build();
        SpanExporter spanExporter = spanExporterFactory.create(settings);
        assertTrue(spanExporter instanceof LoggingSpanExporter);
    }

    public void testSpanExporterLogging() {
        Settings settings = Settings.builder()
            .put(
                OtelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.getKey(),
                "io.opentelemetry.exporter.logging.LoggingSpanExporter"
            )
            .build();
        SpanExporter spanExporter = spanExporterFactory.create(settings);
        assertTrue(spanExporter instanceof LoggingSpanExporter);
    }

    public void testSpanExporterInvalid() {
        Settings settings = Settings.builder().put(OtelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.getKey(), "abc").build();
        assertThrows(IllegalArgumentException.class, () -> spanExporterFactory.create(settings));
    }

    public void testSpanExporterNoCreateFactoryMethod() {
        Settings settings = Settings.builder()
            .put(
                OtelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.getKey(),
                "org.opensearch.telemetry.tracing.exporter.DummySpanExporter"
            )
            .build();
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> spanExporterFactory.create(settings));
        assertEquals(
            "SpanExporter instantiation failed for class [org.opensearch.telemetry.tracing.exporter.DummySpanExporter]",
            exception.getMessage()
        );
    }

    public void testSpanExporterNonSpanExporterClass() {
        Settings settings = Settings.builder()
            .put(OtelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.getKey(), "java.lang.String")
            .build();
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> spanExporterFactory.create(settings));
        assertEquals("SpanExporter instantiation failed for class [java.lang.String]", exception.getMessage());
        assertTrue(exception.getCause() instanceof NoSuchMethodError);

    }

}
