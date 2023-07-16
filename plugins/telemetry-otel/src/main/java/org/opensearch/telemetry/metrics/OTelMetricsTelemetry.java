/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import io.opentelemetry.api.OpenTelemetry;

/**
 * OTelMetricsTelemetry is an implementation of the MetricsTelemetry interface that uses
 * OpenTelemetry for metrics telemetry.
 */
public class OTelMetricsTelemetry implements MetricsTelemetry {
    private final OpenTelemetry openTelemetry;

    /**
     * Constructs an OTelMetricsTelemetry instance with the provided OpenTelemetry instance.
     *
     * @param openTelemetry the OpenTelemetry instance to use for metrics telemetry
     */
    public OTelMetricsTelemetry(OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
    }

    /**
     * Returns the underlying OpenTelemetry instance used for metrics telemetry.
     *
     * @return the OpenTelemetry instance
     */
    public OpenTelemetry getTelemetry() {
        return openTelemetry;
    }
}

