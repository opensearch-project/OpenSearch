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
 * Observable Measurement for the Asynchronous instruments.
 * @opensearch.experimental
 */
@ExperimentalApi
public class ObservableMeasurement {
    private final Double value;
    private final Tags tags;

    /**
     * Factory method to create the {@link ObservableMeasurement} object.
     * @param value value.
     * @param tags tags to be added per value.
     * @return ObservableMeasurement
     */
    public static ObservableMeasurement create(double value, Tags tags) {
        return new ObservableMeasurement(value, tags);
    }

    private ObservableMeasurement(double value, Tags tags) {
        this.value = value;
        this.tags = tags;
    }

    /**
     * Returns the value.
     * @return value
     */
    public Double getValue() {
        return value;
    }

    /**
     * Returns the tags.
     * @return tags
     */
    public Tags getTags() {
        return tags;
    }
}
