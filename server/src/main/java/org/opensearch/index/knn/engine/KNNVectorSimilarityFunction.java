/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Wrapper interface of VectorSimilarityFunction to support more function than what Lucene provides
 */
public interface KNNVectorSimilarityFunction {
    public VectorSimilarityFunction getVectorSimilarityFunction();

    public float compare(float[] var1, float[] var2);

    public float compare(byte[] var1, byte[] var2);

}
