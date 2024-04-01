/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.attributes;

import org.opensearch.common.annotation.InternalApi;

import java.util.Locale;

/**
 * Enum for Inferred Sampling*
 * @opensearch.internal*
 */
@InternalApi
public enum SamplingAttributes {

    /**
     * Attribute added if the span is sampled by inferred sampler*
     */
    INFERRED_SAMPLER,

    /**
     * Attribute Added if the span is an outlier*
     */
    SAMPLED,

    /**
     * Sampler Used in the framework*
     */
    SAMPLER;

    /**
     * returns lower case enum value*
     * @return String
     */
    public String getValue() {
        return name().toLowerCase(Locale.ROOT);
    }
}
