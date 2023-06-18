/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.junit.After;
import org.junit.Before;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.telemetry.Telemetry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.telemetry.tracing.noop.NoopTracer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TracerManagerTests extends OpenSearchTestCase {

    @Before
    public void setup() {
        TracerManager.clear();
    }

    @After
    public void close() {
        TracerManager.closeTracer();
    }

    public void testGetTracerWithUninitializedTracerFactory() {
        Tracer tracer = TracerManager.getTracer();
        assertTrue(tracer instanceof NoopTracer);
    }

    public void testGetTracerWithTracingDisabledReturnsNoopTracer() {
        Settings settings = Settings.builder().put(TracerSettings.TRACER_ENABLED_SETTING.getKey(), false).build();
        TracerSettings tracerSettings = new TracerSettings(settings, new ClusterSettings(settings, getClusterSettings()));
        TracerManager.initTracerManager(tracerSettings, null, mock(ThreadPool.class));

        Tracer tracer = TracerManager.getTracer();
        assertTrue(tracer instanceof NoopTracer);
    }

    public void testGetTracerWithTracingEnabledReturnsDefaultTracer() {
        Settings settings = Settings.builder().put(TracerSettings.TRACER_ENABLED_SETTING.getKey(), true).build();
        TracerSettings tracerSettings = new TracerSettings(settings, new ClusterSettings(settings, getClusterSettings()));
        Telemetry mockTelemetry = mock(Telemetry.class);
        when(mockTelemetry.getTracingTelemetry()).thenReturn(mock(TracingTelemetry.class));
        TracerManager.initTracerManager(tracerSettings, () -> mockTelemetry, mock(ThreadPool.class));

        Tracer tracer = TracerManager.getTracer();
        assertTrue(tracer instanceof DefaultTracer);

    }

    private Set<Setting<?>> getClusterSettings() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TRACER)).stream().forEach((allTracerSettings::add));
        return allTracerSettings;
    }
}
