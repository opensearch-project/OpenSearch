/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Abstracts creation of exact score normalization method based on technique name
 */
public class ScoreNormalizationFactory {

    private static final ScoreNormalizationUtil scoreNormalizationUtil = new ScoreNormalizationUtil();

    public static final ScoreNormalizationTechnique DEFAULT_METHOD = new MinMaxScoreNormalizationTechnique();

    private static final Map<String, Function<Map<String, Object>, ScoreNormalizationTechnique>> SCORE_NORMALIZATION_METHODS = Map.of(
        MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME,
        params -> new MinMaxScoreNormalizationTechnique(params, scoreNormalizationUtil)
    );

    private static final Map<String, Set<String>> COMBINATION_TECHNIQUE_FOR_NORMALIZATION_METHODS = Map.of(
        MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME,
        Set.of(ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME)
    );

    /**
     * Get score normalization method by technique name
     * @param technique name of technique
     * @return instance of ScoreNormalizationMethod for technique name
     */
    public ScoreNormalizationTechnique createNormalization(final String technique) {
        return createNormalization(technique, Map.of());
    }

    public ScoreNormalizationTechnique createNormalization(final String technique, final Map<String, Object> params) {
        return Optional.ofNullable(SCORE_NORMALIZATION_METHODS.get(technique))
            .orElseThrow(() -> new IllegalArgumentException("provided normalization technique is not supported"))
            .apply(params);
    }

    /**
     * Validate normalization technique based on combination technique and other params that needs to be validated
     * @param techniqueCompatibilityCheckDTO data transfer object that contains combination technique and other params that needs to be validated
     */
    public void isTechniquesCompatible(TechniqueCompatibilityCheckDTO techniqueCompatibilityCheckDTO) {
        ScoreNormalizationTechnique normalizationTechnique = techniqueCompatibilityCheckDTO.getScoreNormalizationTechnique();
        Set<String> supportedTechniques = COMBINATION_TECHNIQUE_FOR_NORMALIZATION_METHODS.get(normalizationTechnique.techniqueName());

        if (supportedTechniques.contains(techniqueCompatibilityCheckDTO.getScoreCombinationTechnique().techniqueName()) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "provided combination technique %s is not supported for normalization technique %s. Supported techniques are: %s",
                    techniqueCompatibilityCheckDTO.getScoreCombinationTechnique().techniqueName(),
                    normalizationTechnique.techniqueName(),
                    String.join(", ", supportedTechniques)
                )
            );
        }
    }

}
