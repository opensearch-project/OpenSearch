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
public final class TaggedMeasurement {
    private final Double value;
    private final Tags tags;

    /**
     * Factory method to create the {@link TaggedMeasurement} object.
     * @param value value.
     * @param tags tags to be added per value.
     * @return tagged measurement TaggedMeasurement
     */
    public static TaggedMeasurement create(double value, Tags tags) {
        return new TaggedMeasurement(value, tags);
    }

    private TaggedMeasurement(double value, Tags tags) {
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
