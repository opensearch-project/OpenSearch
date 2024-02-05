/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.SpecialPermission;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.telemetry.metrics.exporter.OTelMetricsExporterFactory;
import org.opensearch.telemetry.tracing.exporter.OTelSpanExporterFactory;
import org.opensearch.telemetry.tracing.sampler.ProbabilisticSampler;
import org.opensearch.telemetry.tracing.sampler.ProbabilisticTransportActionSampler;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.function.Function;

import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;

/**
 * OTel specific telemetry settings.
 */
public final class OTelTelemetrySettings {

    /**
     * Base Constructor.
     */
    private OTelTelemetrySettings() {}

    /**
     * span exporter batch size
     */
    public static final Setting<Integer> TRACER_EXPORTER_BATCH_SIZE_SETTING = Setting.intSetting(
        "telemetry.otel.tracer.exporter.batch_size",
        512,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );
    /**
     * span exporter max queue size
     */
    public static final Setting<Integer> TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING = Setting.intSetting(
        "telemetry.otel.tracer.exporter.max_queue_size",
        2048,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );
    /**
     * span exporter delay in seconds
     */
    public static final Setting<TimeValue> TRACER_EXPORTER_DELAY_SETTING = Setting.timeSetting(
        "telemetry.otel.tracer.exporter.delay",
        TimeValue.timeValueSeconds(2),
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /**
     * Span Exporter type setting.
     */
    @SuppressWarnings({ "unchecked", "removal" })
    public static final Setting<Class<SpanExporter>> OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING = new Setting<>(
        "telemetry.otel.tracer.span.exporter.class",
        LoggingSpanExporter.class.getName(),
        className -> {
            // Check we ourselves are not being called by unprivileged code.
            SpecialPermission.check();

            try {
                return AccessController.doPrivileged((PrivilegedExceptionAction<Class<SpanExporter>>) () -> {
                    final ClassLoader loader = OTelSpanExporterFactory.class.getClassLoader();
                    return (Class<SpanExporter>) loader.loadClass(className);
                });
            } catch (PrivilegedActionException ex) {
                throw new IllegalStateException("Unable to load span exporter class:" + className, ex.getCause());
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /**
     * Metrics Exporter type setting.
     */
    @SuppressWarnings({ "unchecked", "removal" })
    public static final Setting<Class<MetricExporter>> OTEL_METRICS_EXPORTER_CLASS_SETTING = new Setting<>(
        "telemetry.otel.metrics.exporter.class",
        LoggingMetricExporter.class.getName(),
        className -> {
            // Check we ourselves are not being called by unprivileged code.
            SpecialPermission.check();

            try {
                return AccessController.doPrivileged((PrivilegedExceptionAction<Class<MetricExporter>>) () -> {
                    final ClassLoader loader = OTelMetricsExporterFactory.class.getClassLoader();
                    return (Class<MetricExporter>) loader.loadClass(className);
                });
            } catch (PrivilegedActionException ex) {
                throw new IllegalStateException("Unable to load span exporter class:" + className, ex.getCause());
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /**
     * Samplers orders setting.
     */
    @SuppressWarnings("unchecked")
    public static final Setting<List<String>> OTEL_TRACER_SPAN_SAMPLER_CLASS_SETTINGS = Setting.listSetting(
        "telemetry.otel.tracer.span.sampler.classes",
        List.of(ProbabilisticTransportActionSampler.class.getName(), ProbabilisticSampler.class.getName()),
        Function.identity(),
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

}
