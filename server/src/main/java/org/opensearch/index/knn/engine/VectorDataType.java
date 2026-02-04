/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.util.BytesRef;

/**
 * General interface for vector data types
 */
public interface VectorDataType {
    /**
     * Creates a KnnVectorFieldType based on the VectorDataType using the provided dimension and
     * VectorSimilarityFunction.
     *
     * @param dimension                   Dimension of the vector
     * @param knnVectorSimilarityFunction KNNVectorSimilarityFunction for a given spaceType
     * @return FieldType
     */
    public FieldType createKnnVectorFieldType(int dimension, KNNVectorSimilarityFunction knnVectorSimilarityFunction);

    /**
     * Deserializes float vector from BytesRef.
     *
     * @param binaryValue Binary Value
     * @return float vector deserialized from binary value
     */
    public float[] getVectorFromBytesRef(BytesRef binaryValue);
}
