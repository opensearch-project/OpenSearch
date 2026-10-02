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
import org.opensearch.core.common.Strings;
import org.opensearch.secure_sm.AccessController;
import org.opensearch.telemetry.metrics.exporter.OTelMetricsExporterFactory;
import org.opensearch.telemetry.tracing.exporter.OTelSpanExporterFactory;
import org.opensearch.telemetry.tracing.sampler.OTelSamplerFactory;
import org.opensearch.telemetry.tracing.sampler.ProbabilisticSampler;
import org.opensearch.telemetry.tracing.sampler.ProbabilisticTransportActionSampler;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.metrics.Aggregation;
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
    @SuppressWarnings("unchecked")
    public static final Setting<Class<SpanExporter>> OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING = new Setting<>(
        "telemetry.otel.tracer.span.exporter.class",
        LoggingSpanExporter.class.getName(),
        className -> {
            // Check we ourselves are not being called by unprivileged code.
            SpecialPermission.check();

            try {
                return AccessController.doPrivilegedChecked(() -> {
                    final ClassLoader loader = OTelSpanExporterFactory.class.getClassLoader();
                    return (Class<SpanExporter>) loader.loadClass(className);
                });
            } catch (ClassNotFoundException ex) {
                throw new IllegalStateException("Unable to load span exporter class:" + className, ex);
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /**
     * Metrics Exporter type setting.
     */
    @SuppressWarnings("unchecked")
    public static final Setting<Class<MetricExporter>> OTEL_METRICS_EXPORTER_CLASS_SETTING = new Setting<>(
        "telemetry.otel.metrics.exporter.class",
        LoggingMetricExporter.class.getName(),
        className -> {
            // Check we ourselves are not being called by unprivileged code.
            SpecialPermission.check();

            try {
                return AccessController.doPrivilegedChecked(() -> {
                    final ClassLoader loader = OTelMetricsExporterFactory.class.getClassLoader();
                    return (Class<MetricExporter>) loader.loadClass(className);
                });
            } catch (ClassNotFoundException ex) {
                throw new IllegalStateException("Unable to load span exporter class:" + className, ex);
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    private static final String OTEL_METRICS_HISTOGRAM_AGGREGATION_SETTING_KEY = "telemetry.otel.metrics.histogram.aggregation";

    /**
     * Aggregation applied to histogram instruments.
     */
    public enum HistogramAggregation {
        /**
         * Explicit bucket histogram using the SDK default bucket boundaries.
         */
        EXPLICIT_BUCKET_HISTOGRAM("explicit_bucket_histogram"),
        /**
         * Base-2 exponential bucket histogram.
         */
        BASE2_EXPONENTIAL_BUCKET_HISTOGRAM("base2_exponential_bucket_histogram");

        private final String value;

        HistogramAggregation(String value) {
            this.value = value;
        }

        /**
         * Returns the configuration value of this aggregation.
         * @return the value accepted by the setting
         */
        public String getValue() {
            return value;
        }

        /**
         * Parses a configuration value, ignoring case.
         * @param value the configured value
         * @return the matching aggregation
         * @throws IllegalArgumentException if the value is not supported
         */
        public static HistogramAggregation parse(String value) {
            String normalized = value.toLowerCase(Locale.ROOT);
            for (HistogramAggregation aggregation : values()) {
                if (aggregation.value.equals(normalized)) {
                    return aggregation;
                }
            }
            throw new IllegalArgumentException(
                "Invalid value ["
                    + value
                    + "] for setting ["
                    + OTEL_METRICS_HISTOGRAM_AGGREGATION_SETTING_KEY
                    + "], allowed values are ["
                    + EXPLICIT_BUCKET_HISTOGRAM.value
                    + ", "
                    + BASE2_EXPONENTIAL_BUCKET_HISTOGRAM.value
                    + "]"
            );
        }

        /**
         * Returns the OpenTelemetry SDK aggregation for this configuration.
         * @return the SDK aggregation
         */
        public Aggregation toAggregation() {
            return this == EXPLICIT_BUCKET_HISTOGRAM
                ? Aggregation.explicitBucketHistogram()
                : Aggregation.base2ExponentialBucketHistogram();
        }
    }

    /**
     * Histogram aggregation setting.
     */
    public static final Setting<HistogramAggregation> OTEL_METRICS_HISTOGRAM_AGGREGATION_SETTING = new Setting<>(
        OTEL_METRICS_HISTOGRAM_AGGREGATION_SETTING_KEY,
        HistogramAggregation.BASE2_EXPONENTIAL_BUCKET_HISTOGRAM.getValue(),
        HistogramAggregation::parse,
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
                return AccessController.doPrivilegedChecked(() -> {
                    final ClassLoader loader = OTelSamplerFactory.class.getClassLoader();
                    return (Class<Sampler>) loader.loadClass(sampler);
                });
            } catch (ClassNotFoundException ex) {
                throw new IllegalStateException("Unable to load sampler class: " + sampler, ex);
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    /**
     * OTel resource service.name
     */
    public static final Setting<String> OTEL_SERVICE_NAME_SETTING = Setting.simpleString(
        "telemetry.otel.service.name",
        "OpenSearch",
        value -> {
            if (Strings.hasText(value) == false) {
                throw new IllegalArgumentException("telemetry.otel.service.name must be a non-empty string");
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
