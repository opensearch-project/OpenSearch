/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

/**
 * Interface contains spaces supported for approximate nearest neighbor search in the k-NN plugin. Each engine's methods are
 * expected to support a subset of these spaces. Validation should be done in the jni layer and an exception should be
 * propagated up to the Java layer. Additionally, naming translations should be done in jni layer as well. For example,
 * nmslib calls the inner_product space "negdotprod". This translation should take place in the nmslib's jni layer.
 */
public interface SpaceType {
    public abstract float scoreTranslation(float rawScore);

    /**
     * Get KNNVectorSimilarityFunction that maps to this SpaceType
     *
     * @return KNNVectorSimilarityFunction
     */
    public KNNVectorSimilarityFunction getKnnVectorSimilarityFunction();

    /**
     * Validate if the given byte vector is supported by this space type
     *
     * @param vector     the given vector
     */
    public void validateVector(byte[] vector);

    /**
     * Validate if the given float vector is supported by this space type
     *
     * @param vector     the given vector
     */
    public void validateVector(float[] vector);

    /**
     * Validate if given vector data type is supported by this space type
     *
     * @param vectorDataType the given vector data type
     */
    public void validateVectorDataType(VectorDataType vectorDataType);

    /**
     * Get space type name in engine
     *
     * @return name
     */
    public String getValue();

    /**
     * Translate a score to a distance for this space type
     *
     * @param score score to translate
     * @return translated distance
     */
    public float scoreToDistanceTranslation(float score);
}
