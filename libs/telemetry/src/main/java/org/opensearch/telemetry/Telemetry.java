/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.tracing.TracingTelemetry;

/**
 * Interface defining telemetry
 *
 * @opensearch.internal
 */
public interface Telemetry {

    /**
     * Provides tracing telemetry
     * @return tracing telemetry instance
     */
    TracingTelemetry getTracingTelemetry();

    /**
     * Provides metrics telemetry
     * @return metrics telemetry instance
     */
    MetricsTelemetry getMetricsTelemetry();

}
