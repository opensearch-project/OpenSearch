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
import org.opensearch.telemetry.metrics.tags.Tags;

/**
 * Wrapper implementation of {@link Counter}. This delegates call to right {@link Counter} based on the settings
 */
@InternalApi
public class WrappedCounter implements Counter {

    private final TelemetrySettings telemetrySettings;
    private final Counter delegate;

    /**
     * Constructor
     * @param telemetrySettings telemetry settings.
     * @param delegate delegate counter.
     */
    public WrappedCounter(TelemetrySettings telemetrySettings, Counter delegate) {
        this.telemetrySettings = telemetrySettings;
        this.delegate = delegate;
    }

    @Override
    public void add(double value) {
        if (isMetricsEnabled()) {
            delegate.add(value);
        }
    }

    @Override
    public void add(double value, Tags tags) {
        if (isMetricsEnabled()) {
            delegate.add(value, tags);
        }
    }

    private boolean isMetricsEnabled() {
        return telemetrySettings.isMetricsEnabled();
    }
}
