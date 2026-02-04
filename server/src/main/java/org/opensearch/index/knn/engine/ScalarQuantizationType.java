/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

/**
 * The ScalarQuantizationType enum defines the various scalar quantization types that can be used
 * for vector quantization. Each type corresponds to a different bit-width representation of the quantized values.
 *
 * <p>
 * Future Developers: If you change the name of any enum constant, do not change its associated value.
 * Serialization and deserialization depend on these values to maintain compatibility.
 * </p>
 */
public interface ScalarQuantizationType {
    public int getId();
}
