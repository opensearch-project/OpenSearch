/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * {@link MetricsRegistryFactory} represents a single global class that is used to access meters.
 * <p>
 * The {@link MetricsRegistry} singleton object can be retrieved using MetricsRegistryFactory::getMeterRegistry. The MeterFactroy object
 * is created during class initialization and cannot subsequently be changed.
 *
 * @opensearch.internal
 */
@InternalApi
public class MetricsRegistryFactory implements Closeable {

    private static final Logger logger = LogManager.getLogger(MetricsRegistryFactory.class);

    private final TelemetrySettings telemetrySettings;
    private final MetricsRegistry metricsRegistry;

    public MetricsRegistryFactory(TelemetrySettings telemetrySettings, Optional<Telemetry> telemetry) {
        this.telemetrySettings = telemetrySettings;
        this.metricsRegistry = metricsRegistry(telemetry);
    }

    /**
     * Returns the meter instance
     *
     * @return meter instance
     */
    public MetricsRegistry getMeterRegistry() {
        return metricsRegistry;
    }

    /**
     * Closes the {@link Tracer}
     */
    @Override
    public void close() {
        try {
            metricsRegistry.close();
        } catch (IOException e) {
            logger.warn("Error closing meter", e);
        }
    }

    private MetricsRegistry metricsRegistry(Optional<Telemetry> telemetry) {
        MetricsRegistry meter = telemetry.map(Telemetry::getMetricsTelemetry)
            .map(metricsTelemetry -> createDefaultMeter(metricsTelemetry))
            .map(defaultTracer -> createWrappedMeterRegistry(defaultTracer))
            .orElse(NoopMetricsRegistry.INSTANCE);
        return meter;
    }

    private MetricsRegistry createDefaultMeter(MetricsTelemetry metricsTelemetry) {
        return new DefaultMetricsRegistry(metricsTelemetry);
    }

    private MetricsRegistry createWrappedMeterRegistry(MetricsRegistry defaultMeter) {
        return new WrappedMetricsRegistry(telemetrySettings, defaultMeter);
    }

}
