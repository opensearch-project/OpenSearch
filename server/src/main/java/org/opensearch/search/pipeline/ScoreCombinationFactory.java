/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Abstracts creation of exact score combination method based on technique name
 */
public class ScoreCombinationFactory {
    private static final ScoreCombinationUtil scoreCombinationUtil = new ScoreCombinationUtil();

    public static final ScoreCombinationTechnique DEFAULT_METHOD = new ArithmeticMeanScoreCombinationTechnique(
        Map.of(),
        scoreCombinationUtil
    );

    private final Map<String, Function<Map<String, Object>, ScoreCombinationTechnique>> scoreCombinationMethodsMap = Map.of(
        ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME,
        params -> new ArithmeticMeanScoreCombinationTechnique(params, scoreCombinationUtil)
    );

    /**
     * Get score combination method by technique name
     * @param technique name of technique
     * @return instance of ScoreCombinationTechnique for technique name
     */
    public ScoreCombinationTechnique createCombination(final String technique) {
        return createCombination(technique, Map.of());
    }

    /**
     * Get score combination method by technique name
     * @param technique name of technique
     * @param params parameters that combination technique may use
     * @return instance of ScoreCombinationTechnique for technique name
     */
    public ScoreCombinationTechnique createCombination(final String technique, final Map<String, Object> params) {
        return Optional.ofNullable(scoreCombinationMethodsMap.get(technique))
            .orElseThrow(() -> new IllegalArgumentException("provided combination technique is not supported"))
            .apply(params);
    }
}
