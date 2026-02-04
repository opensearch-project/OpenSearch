/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

/**
 * Validates per dimension fields
 */
public interface PerDimensionValidator {
    /**
     * Validates the given float is valid for the configuration
     *
     * @param value to validate
     */
    default void validate(float value) {}

    /**
     * Validates the given float as a byte is valid for the configuration.
     *
     * @param value to validate
     */
    default void validateByte(float value) {}
}
