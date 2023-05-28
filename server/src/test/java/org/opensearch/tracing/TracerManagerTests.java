/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import org.junit.After;
import org.junit.Before;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tracing.noop.NoopTracer;

import static org.mockito.Mockito.mock;

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
        Settings settings = Settings.builder().put(TracerSettings.TRACER_LEVEL_SETTING.getKey(), Level.DISABLED).build();
        TracerSettings tracerSettings = new TracerSettings(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        TracerManager.initTracerManager(tracerSettings, null, null);

        Tracer tracer = TracerManager.getTracer();
        assertTrue(tracer instanceof NoopTracer);
    }

    public void testGetTracerWithTracingEnabledReturnsDefaultTracer() {
        Settings settings = Settings.builder().put(TracerSettings.TRACER_LEVEL_SETTING.getKey(), Level.INFO).build();
        TracerSettings tracerSettings = new TracerSettings(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        TracerManager.initTracerManager(tracerSettings, () -> mock(Tracer.class), null);

        Tracer tracer = TracerManager.getTracer();
        assertFalse(tracer instanceof NoopTracer);

    }
}
