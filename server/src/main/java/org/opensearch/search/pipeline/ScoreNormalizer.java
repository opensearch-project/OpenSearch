/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ScoreNormalizer {

    /**
     * Performs score normalization based on input normalization technique.
     * Mutates input object by updating normalized scores.
     * @param normalizeScoresDTO used as data transfer object to pass in queryTopDocs, original query results
     * from multiple shards and multiple sub-queries, scoreNormalizationTechnique exact normalization technique
     * that should be applied, and nullable rankConstant that is only used in RRF technique
     */
    public void normalizeScores(final NormalizeScoresDTO normalizeScoresDTO) {
        final List<CompoundTopDocs> queryTopDocs = normalizeScoresDTO.getQueryTopDocs();
        final ScoreNormalizationTechnique scoreNormalizationTechnique = normalizeScoresDTO.getNormalizationTechnique();
        if (canQueryResultsBeNormalized(queryTopDocs)) {
            scoreNormalizationTechnique.normalize(normalizeScoresDTO);
        }
    }

    private boolean canQueryResultsBeNormalized(final List<CompoundTopDocs> queryTopDocs) {
        return queryTopDocs.stream().filter(Objects::nonNull).anyMatch(topDocs -> topDocs.getTopDocs().size() > 0);
    }

    /**
     * Explain normalized scores based on input normalization technique. Does not mutate input object.
     * @param queryTopDocs original query results from multiple shards and multiple sub-queries
     * @param queryTopDocs
     * @param scoreNormalizationTechnique
     * @return map of doc id to explanation details
     */
    public Map<DocIdAtSearchShard, ExplanationDetails> explain(
        final List<CompoundTopDocs> queryTopDocs,
        final ExplainableTechnique scoreNormalizationTechnique
    ) {
        if (canQueryResultsBeNormalized(queryTopDocs)) {
            return scoreNormalizationTechnique.explain(queryTopDocs);
        }
        return Map.of();
    }
}
