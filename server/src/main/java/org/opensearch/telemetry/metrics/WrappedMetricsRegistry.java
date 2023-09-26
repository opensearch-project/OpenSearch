/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.telemetry.TelemetrySettings;

import java.io.IOException;

/**
 * Wrapper implementation of {@link MetricsRegistry}. This delegates call to right {@link MetricsRegistry} based on the settings
 */
@InternalApi
public class WrappedMetricsRegistry implements MetricsRegistry {

    private final MetricsRegistry defaultMetricsRegistry;
    private final TelemetrySettings telemetrySettings;

    /**
     * Constructor.
     * @param defaultMetricsRegistry default meter registry.
     * @param telemetrySettings telemetry settings.
     */
    public WrappedMetricsRegistry(TelemetrySettings telemetrySettings, MetricsRegistry defaultMetricsRegistry) {
        this.telemetrySettings = telemetrySettings;
        this.defaultMetricsRegistry = defaultMetricsRegistry;
    }

    @Override
    public Counter createCounter(String name, String description, String unit) {
        return new WrappedCounter(telemetrySettings, defaultMetricsRegistry.createCounter(name, description, unit));
    }

    @Override
    public Counter createUpDownCounter(String name, String description, String unit) {
        return new WrappedCounter(telemetrySettings, defaultMetricsRegistry.createUpDownCounter(name, description, unit));
    }

    @Override
    public void close() throws IOException {
        defaultMetricsRegistry.close();
    }
}
