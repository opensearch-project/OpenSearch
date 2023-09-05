/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import org.opensearch.SpecialPermission;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.telemetry.tracing.exporter.OTelSpanExporterFactory;

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
}
