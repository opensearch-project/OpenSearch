/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.search.pipeline.ScoreCombinationUtil.PARAM_NAME_WEIGHTS;

/**
 * Abstracts combination of scores based on arithmetic mean method
 */
public class ArithmeticMeanScoreCombinationTechnique implements ScoreCombinationTechnique, ExplainableTechnique {
    public static final String TECHNIQUE_NAME = "arithmetic_mean";
    private static final Set<String> SUPPORTED_PARAMS = Set.of(PARAM_NAME_WEIGHTS);
    private static final Float ZERO_SCORE = 0.0f;
    private final List<Float> weights;
    private final ScoreCombinationUtil scoreCombinationUtil;

    public ArithmeticMeanScoreCombinationTechnique(final Map<String, Object> params, final ScoreCombinationUtil combinationUtil) {
        scoreCombinationUtil = combinationUtil;
        scoreCombinationUtil.validateParams(params, SUPPORTED_PARAMS);
        weights = scoreCombinationUtil.getWeights(params);
    }

    /**
     * Arithmetic mean method for combining scores.
     * score = (weight1*score1 + weight2*score2 +...+ weightN*scoreN)/(weight1 + weight2 + ... + weightN)
     *
     * Zero (0.0) scores are excluded from number of scores N
     */
    @Override
    public float combine(final float[] scores) {
        scoreCombinationUtil.validateIfWeightsMatchScores(scores, weights);
        float combinedScore = 0.0f;
        float sumOfWeights = 0;
        for (int indexOfSubQuery = 0; indexOfSubQuery < scores.length; indexOfSubQuery++) {
            float score = scores[indexOfSubQuery];
            if (score >= 0.0) {
                float weight = scoreCombinationUtil.getWeightForSubQuery(weights, indexOfSubQuery);
                score = score * weight;
                combinedScore += score;
                sumOfWeights += weight;
            }
        }
        if (sumOfWeights == 0.0f) {
            return ZERO_SCORE;
        }
        return combinedScore / sumOfWeights;
    }

    @Override
    public String techniqueName() {
        return TECHNIQUE_NAME;
    }

    @Override
    public String describe() {
        return describeCombinationTechnique(TECHNIQUE_NAME, weights);
    }

    /**
     * Creates a string describing the combination technique and its parameters
     * @param techniqueName the name of the combination technique
     * @param weights the weights used in the combination technique
     * @return a string describing the combination technique and its parameters
     */
    public static String describeCombinationTechnique(final String techniqueName, final List<Float> weights) {
        if (Objects.isNull(techniqueName)) {
            throw new IllegalArgumentException("combination technique name cannot be null");
        }
        return Optional.ofNullable(weights)
            .filter(w -> !w.isEmpty())
            .map(w -> String.format(Locale.ROOT, "%s, weights %s", techniqueName, weights))
            .orElse(String.format(Locale.ROOT, "%s", techniqueName));
    }
}
