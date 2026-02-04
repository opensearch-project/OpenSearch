/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

/**
 * Class validates vector after it has been parsed
 */
public interface VectorValidator {
    /**
     * Validate if the given byte vector is supported
     *
     * @param vector     the given vector
     */
    default void validateVector(byte[] vector) {}

    /**
     * Validate if the given float vector is supported
     *
     * @param vector     the given vector
     */
    default void validateVector(float[] vector) {}

    VectorValidator NOOP_VECTOR_VALIDATOR = new VectorValidator() {
    };
}
