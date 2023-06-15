/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.telemetry.tracing.*;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.telemetry.tracing.noop.NoopSpan;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpanFactoryTests extends OpenSearchTestCase {

    public void testCreateSpanLevelDisabledReturnsNoopSpan() {
        Settings settings = Settings.builder().put(TracerSettings.TRACER_LEVEL_SETTING.getKey(), Level.ROOT).build();
        TracerSettings tracerSettings = new TracerSettings(settings, new ClusterSettings(settings, getClusterSettings()));

        SpanFactory spanFactory = new SpanFactory(tracerSettings, null);

        assertTrue(spanFactory.createSpan("spanName", null, Level.INFO) instanceof NoopSpan);
    }

    public void testCreateSpanLevelEnabledReturnsDefaultSpan() {
        Settings settings = Settings.builder().put(TracerSettings.TRACER_LEVEL_SETTING.getKey(), Level.INFO).build();
        TracerSettings tracerSettings = new TracerSettings(settings, new ClusterSettings(settings, getClusterSettings()));

        TracingTelemetry mockTracingTelemetry = mock(TracingTelemetry.class);
        when(mockTracingTelemetry.createSpan(eq("spanName"), any(), eq(Level.INFO))).thenReturn(mock(Span.class));
        SpanFactory spanFactory = new SpanFactory(tracerSettings, mockTracingTelemetry);

        assertFalse(spanFactory.createSpan("spanName", null, Level.INFO) instanceof NoopSpan);
    }

    private Set<Setting<?>> getClusterSettings() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TRACER)).stream().forEach((allTracerSettings::add));
        return allTracerSettings;
    }
}
