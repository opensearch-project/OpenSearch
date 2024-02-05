/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics.noop;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;

import java.io.IOException;

/**
 *No-op {@link MetricsRegistry}
 * {@opensearch.internal}
 */
@InternalApi
public class NoopMetricsRegistry implements MetricsRegistry {

    /**
     * No-op Meter instance
     */
    public final static NoopMetricsRegistry INSTANCE = new NoopMetricsRegistry();

    private NoopMetricsRegistry() {}

    @Override
    public Counter createCounter(String name, String description, String unit) {
        return NoopCounter.INSTANCE;
    }

    @Override
    public Counter createUpDownCounter(String name, String description, String unit) {
        return NoopCounter.INSTANCE;
    }

    @Override
    public Histogram createHistogram(String name, String description, String unit) {
        return NoopHistogram.INSTANCE;
    }

    @Override
    public void close() throws IOException {

    }
}
