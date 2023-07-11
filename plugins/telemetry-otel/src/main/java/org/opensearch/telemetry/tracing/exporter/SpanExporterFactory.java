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
import java.lang.reflect.Method;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Factory class to create the {@link SpanExporter} instance.
 */
public class SpanExporterFactory {

    private static final Logger logger = LogManager.getLogger(SpanExporterFactory.class);

    /**
     * Span Exporter type setting.
     */
    @SuppressWarnings("unchecked")
    public static final Setting<Class<SpanExporter>> OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING = new Setting<>(
        "telemetry.otel.tracer.span.exporter.class",
        LoggingSpanExporter.class.getName(),
        className -> {
            try {
                return (Class<SpanExporter>) Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /**
     * Base constructor.
     */
    public SpanExporterFactory() {

    }

    /**
     * Creates the {@link SpanExporter} instances based on the OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING value.
     * As of now, it expects the SpanExporter implemetations to have create factory method to instantiate the
     * SpanExporter.
     * @param settings settings.
     * @return SpanExporter instance.
     */
    public SpanExporter create(Settings settings) {
        Class<SpanExporter> spanExporterProviderClass = OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.get(settings);
        SpanExporter spanExporter = instantiateSpanExporter(spanExporterProviderClass);
        logger.info("Successfully instantiated the SpanExporter class {}", spanExporterProviderClass);
        return spanExporter;
    }

    private SpanExporter instantiateSpanExporter(Class<SpanExporter> spanExporterProviderClass) {
        try {
            Method m = spanExporterProviderClass.getMethod("create");
            return (SpanExporter) m.invoke(null);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("No create factory method exist in [" + spanExporterProviderClass.getName() + "]");
        } catch (Exception e) {
            throw new IllegalStateException("SpanExporter instantiation failed for class [" + spanExporterProviderClass.getName() + "]");
        }
    }
}
