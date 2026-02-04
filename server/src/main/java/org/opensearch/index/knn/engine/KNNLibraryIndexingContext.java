/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

import java.util.Map;

/**
 * Context a library gives to build one of its indices
 */
public interface KNNLibraryIndexingContext {
    /**
     * Get map of parameters that get passed to the library to build the index
     *
     * @return Map of parameters
     */
    Map<String, Object> getLibraryParameters();

    /**
     * Get map of parameters that get passed to the quantization framework
     *
     * @return Map of parameters
     */
    QuantizationConfig getQuantizationConfig();

    /**
     *
     * @return Get the vector validator
     */
    VectorValidator getVectorValidator();

    /**
     *
     * @return Get the per dimension validator
     */
    PerDimensionValidator getPerDimensionValidator();

    /**
     *
     * @return Get the per dimension processor
     */
    PerDimensionProcessor getPerDimensionProcessor();

    /**
     * Get the vector transformer that will be used to transform the vector before indexing.
     * This will be applied at vector level once entire vector is parsed and validated.
     *
     * @return VectorTransformer
     */
    VectorTransformer getVectorTransformer();
}
