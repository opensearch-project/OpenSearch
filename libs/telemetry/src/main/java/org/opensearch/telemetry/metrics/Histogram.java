/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.telemetry.metrics.tags.Tags;

/**
 * Histogram records the value for an existing metric.
 * {@opensearch.experimental}
 */
@ExperimentalApi
public interface Histogram {

    /**
     * record value.
     * @param value value to be added.
     */
    void record(double value);

    /**
     * record value along with the attributes.
     *
     * @param value value to be added.
     * @param tags  attributes/dimensions of the metric.
     */
    void record(double value, Tags tags);

}
