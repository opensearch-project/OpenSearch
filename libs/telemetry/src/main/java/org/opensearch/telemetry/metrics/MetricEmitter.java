/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

/**
 * Consumers of this interface can implement custom metric emission logic. For example, it can be integrated
 * with OpenTelemetry meters or can be used just for printing on console for testing purposes
 */
public interface MetricEmitter {

    /**
     * Emits the provided metric according to the custom implementation logic.
     *
     * @param metric the metric to emit
     */
    void emitMetric(MetricPoint metric);
}
