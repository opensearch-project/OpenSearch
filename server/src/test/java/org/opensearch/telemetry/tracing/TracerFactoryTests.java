/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.junit.After;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.telemetry.tracing.noop.NoopTracer;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TracerFactoryTests extends OpenSearchTestCase {

    private TracerFactory tracerFactory;

    @After
    public void close() {
        tracerFactory.close();
    }

    public void testGetTracerWithUnavailableTracingTelemetryReturnsNoopTracer() {
        Settings settings = Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), false).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        Telemetry mockTelemetry = mock(Telemetry.class);
        when(mockTelemetry.getTracingTelemetry()).thenReturn(mock(TracingTelemetry.class));
        tracerFactory = new TracerFactory(telemetrySettings, Optional.empty(), new ThreadContext(Settings.EMPTY));

        Tracer tracer = tracerFactory.getTracer();

        assertTrue(tracer instanceof NoopTracer);
        assertTrue(tracer.startSpan("foo") == SpanScope.NO_OP);
    }

    public void testGetTracerWithAvailableTracingTelemetryReturnsWrappedTracer() {
        Settings settings = Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), true).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        Telemetry mockTelemetry = mock(Telemetry.class);
        when(mockTelemetry.getTracingTelemetry()).thenReturn(mock(TracingTelemetry.class));
        tracerFactory = new TracerFactory(telemetrySettings, Optional.of(mockTelemetry), new ThreadContext(Settings.EMPTY));

        Tracer tracer = tracerFactory.getTracer();
        assertTrue(tracer instanceof WrappedTracer);

    }

    private Set<Setting<?>> getClusterSettings() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TELEMETRY)).stream().forEach((allTracerSettings::add));
        return allTracerSettings;
    }
}
