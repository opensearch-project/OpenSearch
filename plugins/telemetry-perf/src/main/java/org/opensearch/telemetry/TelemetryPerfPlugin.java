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
import org.opensearch.telemetry.tracing.listeners.TraceEventListener;
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
 * Telemetry perf plugin
 */
public class TelemetryPerfPlugin extends Plugin {

    private final Settings settings;

    public OTelTelemetryPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
        );
    }

    @Override
    public String getName() {
        return OTEL_TRACER_NAME;
    }

}
