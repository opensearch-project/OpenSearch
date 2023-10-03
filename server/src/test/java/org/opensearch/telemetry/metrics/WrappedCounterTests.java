/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class WrappedCounterTests extends OpenSearchTestCase {

    public void testCounterWithDisabledMetrics() throws Exception {
        Settings settings = Settings.builder().put(TelemetrySettings.METRICS_ENABLED_SETTING.getKey(), false).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        Counter mockCounter = mock(Counter.class);

        WrappedCounter wrappedCounter = new WrappedCounter(telemetrySettings, mockCounter);
        wrappedCounter.add(1.0);
        verify(mockCounter, never()).add(1.0);

        Tags tags = Tags.create().addTag("test", "test");
        wrappedCounter.add(1.0, tags);
        verify(mockCounter, never()).add(1.0, tags);

    }

    public void testCounterWithEnabledMetrics() throws Exception {
        Settings settings = Settings.builder().put(TelemetrySettings.METRICS_ENABLED_SETTING.getKey(), true).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        Counter mockCounter = mock(Counter.class);

        WrappedCounter wrappedCounter = new WrappedCounter(telemetrySettings, mockCounter);
        wrappedCounter.add(1.0);
        verify(mockCounter).add(1.0);

        Tags tags = Tags.create().addTag("test", "test");
        wrappedCounter.add(1.0, tags);
        verify(mockCounter).add(1.0, tags);
    }

    private Set<Setting<?>> getClusterSettings() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TELEMETRY)).stream().forEach((allTracerSettings::add));
        return allTracerSettings;
    }
}
