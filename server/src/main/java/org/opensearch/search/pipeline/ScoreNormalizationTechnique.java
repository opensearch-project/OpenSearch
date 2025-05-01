/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

/**
 * ScoreNormalizationTechnique DTO
 */
public interface ScoreNormalizationTechnique {

    /**
     * Performs score normalization based on input normalization technique.
     * Mutates input object by updating normalized scores.
     * @param normalizeScoresDTO is a data transfer object that contains queryTopDocs
     * original query results from multiple shards and multiple sub-queries, ScoreNormalizationTechnique,
     * and nullable rankConstant that is only used in RRF technique
     */
    void normalize(final NormalizeScoresDTO normalizeScoresDTO);

    /**
     * Returns the name of the normalization technique.
     */
    String techniqueName();
}
