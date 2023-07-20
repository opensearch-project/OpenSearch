/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryPlugin;
import org.opensearch.telemetry.diagnostics.jmx.JMXMetricsObserverThread;
import org.opensearch.telemetry.diagnostics.jmx.JMXOTelMetricEmitter;
import org.opensearch.telemetry.diagnostics.jmx.JMXThreadResourceRecorder;
import org.opensearch.telemetry.listeners.TraceEventListener;
import org.opensearch.telemetry.metrics.OTelMetricsTelemetry;
import org.opensearch.telemetry.tracing.OTelResourceProvider;
import org.opensearch.telemetry.tracing.OTelTelemetry;
import org.opensearch.telemetry.tracing.OTelTracingTelemetry;
import org.opensearch.telemetry.diagnostics.DiagnosticsEventListener;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Telemetry plugin based on Otel
 */
public class OTelTelemetryPlugin extends Plugin implements TelemetryPlugin {

    static final String OTEL_TRACER_NAME = "otel";

    private final Settings settings;

    /**
     * Creates Otel plugin
     * @param settings cluster settings
     */
    public OTelTelemetryPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            OTelTelemetrySettings.TRACER_EXPORTER_BATCH_SIZE_SETTING,
            OTelTelemetrySettings.TRACER_EXPORTER_DELAY_SETTING,
            OTelTelemetrySettings.TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING,
            OTelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING
        );
    }

    @Override
    public Optional<Telemetry> getTelemetry(TelemetrySettings settings) {
        return Optional.of(telemetry());
    }

    @Override
    public Map<String, TraceEventListener> getTraceEventListeners(Telemetry telemetry) {
        if (!ensureOpenTelemetry(telemetry)) {
            return Collections.emptyMap();
        }
        OpenTelemetry openTelemetry = ((OTelMetricsTelemetry)telemetry.getMetricsTelemetry()).getTelemetry();
        return Map.of("ThreadDiagnosticsTraceEventListener", new DiagnosticsEventListener(
            new JMXThreadResourceRecorder(new JMXMetricsObserverThread()),
            JMXOTelMetricEmitter.getInstance(openTelemetry)
        ));
    }

    @Override
    public String getName() {
        return OTEL_TRACER_NAME;
    }

    private Telemetry telemetry() {
        OpenTelemetry openTelemetry = OTelResourceProvider.get(settings);
        return new OTelTelemetry(new OTelTracingTelemetry(openTelemetry),
            new OTelMetricsTelemetry(openTelemetry));
    }

    private boolean ensureOpenTelemetry(Telemetry telemetry) {
       return (telemetry != null && telemetry.getMetricsTelemetry() != null
            && telemetry instanceof OTelTelemetry && telemetry.getMetricsTelemetry() instanceof OTelMetricsTelemetry);
    }

}
