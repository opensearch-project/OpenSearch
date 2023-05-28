/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import io.opentelemetry.api.OpenTelemetry;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TracerPlugin;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Tracer plugin based on Otel
 */
public class OTelTracerModulePlugin extends Plugin implements TracerPlugin {

    public static final String OTEL_TRACER_NAME = "otel";

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
            // set Otel tracer as default tracer
            .put(TracerModule.TRACER_DEFAULT_TYPE_SETTING.getKey(), OTEL_TRACER_NAME)
            .build();
    }

    @Override
    public Map<String, Supplier<Tracer>> getTracers(ThreadPool threadPool, TracerSettings tracerSettings) {
        return Collections.singletonMap(OTEL_TRACER_NAME, () -> createDefaultTracer(threadPool, tracerSettings));
    }

    @Override
    public Map<String, TracerHeaderInjector> getHeaderInjectors() {
        return Collections.singletonMap(OTEL_TRACER_NAME, new OtelTracerHeaderInjector());
    }

    private Tracer createDefaultTracer(ThreadPool threadPool, TracerSettings tracerSettings) {
        OpenTelemetry openTelemetry = OTelResourceProvider.getOrCreateOpenTelemetryInstance(tracerSettings);
        return new DefaultTracer(openTelemetry, threadPool, tracerSettings);
    }
}
