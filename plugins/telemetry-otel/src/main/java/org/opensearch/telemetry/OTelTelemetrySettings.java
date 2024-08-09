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
import org.opensearch.telemetry.tracing.sampler.OTelSamplerFactory;
import org.opensearch.telemetry.tracing.sampler.ProbabilisticSampler;
import org.opensearch.telemetry.tracing.sampler.ProbabilisticTransportActionSampler;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;

import io.opentelemetry.exporter.logging.otlp.OtlpJsonLoggingMetricExporter;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;

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
        OtlpJsonLoggingMetricExporter.class.getName(),
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
    public static final Setting<List<Class<Sampler>>> OTEL_TRACER_SPAN_SAMPLER_CLASS_SETTINGS = Setting.listSetting(
        "telemetry.otel.tracer.span.sampler.classes",
        Arrays.asList(ProbabilisticTransportActionSampler.class.getName(), ProbabilisticSampler.class.getName()),
        sampler -> {
            // Check we ourselves are not being called by unprivileged code.
            SpecialPermission.check();
            try {
                return AccessController.doPrivileged((PrivilegedExceptionAction<Class<Sampler>>) () -> {
                    final ClassLoader loader = OTelSamplerFactory.class.getClassLoader();
                    return (Class<Sampler>) loader.loadClass(sampler);
                });
            } catch (PrivilegedActionException ex) {
                throw new IllegalStateException("Unable to load sampler class: " + sampler, ex.getCause());
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /**
     * Probability of action based sampler
     */
    public static final Setting<Double> TRACER_SAMPLER_ACTION_PROBABILITY = Setting.doubleSetting(
        "telemetry.tracer.action.sampler.probability",
        0.001d,
        0.000d,
        1.00d,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

}
