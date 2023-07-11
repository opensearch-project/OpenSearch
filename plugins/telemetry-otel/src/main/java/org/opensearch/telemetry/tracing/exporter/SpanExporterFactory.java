/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.exporter;

import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.lang.reflect.Constructor;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Factory class to create the {@link SpanExporter} instance.
 */
public class SpanExporterFactory {

    /**
     * Span Exporter type setting.
     */
    @SuppressWarnings("unchecked")
    public static final Setting<Class<SpanExporterProvider>> OTEL_TRACER_SPAN_EXPORTER_PROVIDE_CLASS_SETTING = new Setting<>(
        "telemetry.otel.tracer.span.exporter.provider.class",
        LoggingSpanExporterProvider.class.getName(),
        className -> {
            try {
                return (Class<SpanExporterProvider>) Class.forName(className);
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
     * Creates the {@link SpanExporter} instances based on the OTEL_TRACER_SPAN_EXPORTER_PROVIDE_CLASS_SETTING value.
     * @param settings settings.
     * @return SpanExporter instance.
     */
    public SpanExporter create(Settings settings) {
        Class<SpanExporterProvider> spanExporterProviderClass = OTEL_TRACER_SPAN_EXPORTER_PROVIDE_CLASS_SETTING.get(settings);
        SpanExporterProvider spanExporterProvider = instantiateProvider(spanExporterProviderClass);
        return spanExporterProvider.create(settings);
    }

    private SpanExporterProvider instantiateProvider(Class<SpanExporterProvider> spanExporterProviderClass) {
        final Constructor<?>[] constructors = spanExporterProviderClass.getConstructors();
        if (constructors.length == 0) {
            throw new IllegalStateException("no public constructor for [" + spanExporterProviderClass.getName() + "]");
        }

        if (constructors.length > 1) {
            throw new IllegalStateException("no unique public constructor for [" + spanExporterProviderClass.getName() + "]");
        }
        final Constructor<?> constructor = constructors[0];
        if (constructor.getParameterCount() > 0) {
            throw new IllegalStateException("no no-arg public constructor for [" + spanExporterProviderClass.getName() + "]");
        }
        try {
            return (SpanExporterProvider) constructor.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("failed to load span exporter provider class [" + spanExporterProviderClass.getName() + "]", e);
        }
    }
}
