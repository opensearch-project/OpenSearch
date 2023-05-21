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
import org.opensearch.threadpool.ThreadPool;

import static org.mockito.Mockito.mock;

public class TracerFactoryTests extends OpenSearchTestCase {

    @Before
    public void setup() {
        TracerFactory.clear();
    }

    @After
    public void close() {
        TracerFactory.closeTracer();
    }

    public void testGetTracerWithUninitializedTracerFactory() {
        Tracer tracer = TracerFactory.getTracer();
        assertTrue(tracer instanceof NoopTracer);
    }

    public void testGetTracerWithTracingDisabledReturnsNoopTracer() {
        Settings settings = Settings.builder().put(TracerSettings.TRACER_LEVEL_SETTING.getKey(), Level.DISABLED).build();
        TracerSettings tracerSettings = new TracerSettings(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        TracerFactory.initTracerFactory(mock(ThreadPool.class), tracerSettings);

        Tracer tracer = TracerFactory.getTracer();
        assertTrue(tracer instanceof NoopTracer);
    }

    public void testGetTracerWithTracingEnabledReturnsDefaultTracer() {
        Settings settings = Settings.builder().put(TracerSettings.TRACER_LEVEL_SETTING.getKey(), Level.INFO).build();
        TracerSettings tracerSettings = new TracerSettings(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        TracerFactory.initTracerFactory(mock(ThreadPool.class), tracerSettings);

        Tracer tracer = TracerFactory.getTracer();
        assertTrue(tracer instanceof DefaultTracer);

    }
}
