/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

/**
 * Enum representing the compression level for float vectors. Compression in this sense refers to compressing a
 * full precision value into a smaller number of bits. For instance. "16x" compression would mean that 2 bits would
 * need to be used to represent a 32-bit floating point number.
 */
public interface CompressionLevel {

    /**
     * Gets the name of the compression level
     *
     * @return name of compression level
     */
    public String getName();
}
