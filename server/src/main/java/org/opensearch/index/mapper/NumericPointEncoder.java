/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

/**
 * Interface for encoding a point value
 */
public interface NumericPointEncoder {
    byte[] encodePoint(Number value);

    /**
     * Encodes an Object value to byte array for Approximation Framework search_after optimization.
     * @param value the search_after value as Object
     * @param roundUp whether to round up (for lower bounds) or down (for upper bounds)
     * @return encoded byte array
     */
    byte[] encodePoint(Object value, boolean roundUp);
}
