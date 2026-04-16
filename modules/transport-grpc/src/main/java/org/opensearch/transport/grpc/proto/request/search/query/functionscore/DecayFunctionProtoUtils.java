/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.protobufs.MultiValueMode;

/**
 * Common utility class for decay function Protocol Buffer conversions.
 * Contains shared methods used by {@link ExpDecayFunctionProtoUtils},
 * {@link GaussDecayFunctionProtoUtils}, and {@link LinearDecayFunctionProtoUtils}.
 */
class DecayFunctionProtoUtils {

    private DecayFunctionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer MultiValueMode enum to OpenSearch MultiValueMode.
     *
     * @param multiValueMode the Protocol Buffer MultiValueMode enum value
     * @return the corresponding OpenSearch MultiValueMode
     * @throws IllegalArgumentException if the multiValueMode is unsupported
     */
    static org.opensearch.search.MultiValueMode parseMultiValueMode(MultiValueMode multiValueMode) {
        return switch (multiValueMode) {
            case MULTI_VALUE_MODE_AVG -> org.opensearch.search.MultiValueMode.AVG;
            case MULTI_VALUE_MODE_MAX -> org.opensearch.search.MultiValueMode.MAX;
            case MULTI_VALUE_MODE_MIN -> org.opensearch.search.MultiValueMode.MIN;
            case MULTI_VALUE_MODE_SUM -> org.opensearch.search.MultiValueMode.SUM;
            default -> throw new IllegalArgumentException("Unsupported multi value mode: " + multiValueMode);
        };
    }
}
