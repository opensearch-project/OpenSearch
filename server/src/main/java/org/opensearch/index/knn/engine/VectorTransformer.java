/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

/**
 * Defines operations for transforming vectors in the k-NN search context.
 * Implementations can modify vectors while preserving their dimensional properties
 * for specific use cases such as normalization, scaling, or other transformations.
 */
public interface VectorTransformer {

    /**
     * Transforms a float vector in place.
     *
     * @param vector The input vector to transform (must not be null)
     * @throws IllegalArgumentException if the input vector is null
     */
    default void transform(final float[] vector) {
        if (vector == null) {
            throw new IllegalArgumentException("Input vector cannot be null");
        }
    }

    /**
     * Transforms a byte vector in place.
     *
     * @param vector The input vector to transform (must not be null)
     * @throws IllegalArgumentException if the input vector is null
     */
    default void transform(final byte[] vector) {
        if (vector == null) {
            throw new IllegalArgumentException("Input vector cannot be null");
        }
    }
}
