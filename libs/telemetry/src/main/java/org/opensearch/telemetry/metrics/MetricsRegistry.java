/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;

/**
 * MetricsRegistry helps in creating the metric instruments.
 * @opensearch.experimental
 */
@ExperimentalApi
public interface MetricsRegistry extends Closeable {

    /**
     * Creates the counter.
     * @param name name of the counter.
     * @param description any description about the metric.
     * @param unit unit of the metric.
     * @return counter.
     */
    Counter createCounter(String name, String description, String unit);

    /**
     * Creates the upDown counter.
     * @param name name of the upDown counter.
     * @param description any description about the metric.
     * @param unit unit of the metric.
     * @return counter.
     */
    Counter createUpDownCounter(String name, String description, String unit);

    /**
     * Creates the histogram type of Metric. Implementation framework will take care
     * of the bucketing strategy.
     *
     * @param name        name of the histogram.
     * @param description any description about the metric.
     * @param unit        unit of the metric.
     * @return histogram.
     */
    Histogram createHistogram(String name, String description, String unit);
}
