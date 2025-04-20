/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Collection of utility methods for score combination technique classes
 */
class ScoreNormalizationUtil {
    private static final Logger logger = LogManager.getLogger(ScoreNormalizationUtil.class);
    private static final String PARAM_NAME_WEIGHTS = "weights";
    private static final float DELTA_FOR_SCORE_ASSERTION = 0.01f;

    /**
     * Validate config parameters for this technique
     * @param actualParams map of parameters in form of name-value
     * @param supportedParams collection of parameters that we should validate against, typically that's what is supported by exact technique
     */
    public void validateParams(final Map<String, Object> actualParams, final Set<String> supportedParams) {
        if (Objects.isNull(actualParams) || actualParams.isEmpty()) {
            return;
        }
        // check if only supported params are passed
        Optional<String> optionalNotSupportedParam = actualParams.keySet()
            .stream()
            .filter(paramName -> !supportedParams.contains(paramName))
            .findFirst();
        if (optionalNotSupportedParam.isPresent()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "provided parameter for combination technique is not supported. supported parameters are [%s]",
                    String.join(",", supportedParams)
                )
            );
        }

        // check param types
        if (actualParams.keySet().stream().anyMatch(PARAM_NAME_WEIGHTS::equalsIgnoreCase)) {
            if (actualParams.get(PARAM_NAME_WEIGHTS) instanceof List == false) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "parameter [%s] must be a collection of numbers", PARAM_NAME_WEIGHTS)
                );
            }
        }
    }

    /**
     * Sets a normalized score for a specific document at a specific subquery index
     *
     * @param normalizedScores map of document IDs to their list of scores
     * @param docIdAtSearchShard document ID
     * @param subQueryIndex index of the subquery
     * @param normalizedScore normalized score to set
     */
    public static void setNormalizedScore(
        Map<DocIdAtSearchShard, List<Float>> normalizedScores,
        DocIdAtSearchShard docIdAtSearchShard,
        int subQueryIndex,
        int numberOfSubQueries,
        float normalizedScore
    ) {
        List<Float> scores = normalizedScores.get(docIdAtSearchShard);
        if (Objects.isNull(scores)) {
            scores = new ArrayList<>(numberOfSubQueries);
            for (int i = 0; i < numberOfSubQueries; i++) {
                scores.add(0.0f);
            }
            normalizedScores.put(docIdAtSearchShard, scores);
        }
        scores.set(subQueryIndex, normalizedScore);
    }

    /**
     * Validate parameters for this technique. Following is example of structured parameters that we will validate
     * {
     *     "technique": "arithmetic_mean",  // top-level parameter 1
     *     "details": {                     // top-level parameter 2
     *         "weights": [1, 2, 3],        // nested parameter 1
     *         "color": "green".            // nested parameter 2
     *     }
     * }
     * for this example client should pass:
     *     top-level parameters: ["technique", "details"]
     *     nested parameters: ["details" -> ["weights", "color"]]
     * @param actualParameters map of actual parameters in form of name-value
     * @param supportedParametersTopLevel collection of top-level parameters that we should validate against
     * @param supportedParametersNested map of nested parameters that we should validate against, key is one of top level parameters and value is set of allowed nested params
     */
    public void validateParameters(
        final Map<String, Object> actualParameters,
        final Set<String> supportedParametersTopLevel,
        final Map<String, Set<String>> supportedParametersNested
    ) {
        if (Objects.isNull(actualParameters) || actualParameters.isEmpty()) {
            return;
        }
        boolean hasUnknownParameters = false;
        for (Map.Entry<String, Object> entry : actualParameters.entrySet()) {
            String paramName = entry.getKey();
            Object paramValue = entry.getValue();

            if (supportedParametersTopLevel.contains(paramName) == false) {
                hasUnknownParameters = true;
                continue;
            }
            if (paramValue instanceof Map) {
                Map<String, Object> nestedParams = (Map<String, Object>) paramValue;
                validateNestedParameters(nestedParams, supportedParametersNested.get(paramName));
            } else if (paramValue instanceof List) {
                for (Object item : (List<?>) paramValue) {
                    if (item instanceof Map) {
                        validateNestedParameters((Map<String, Object>) item, supportedParametersNested.get(paramName));
                    } else {
                        hasUnknownParameters = true;
                    }
                }
            } else {
                if (supportedParametersNested.isEmpty()) {
                    continue;
                }
                hasUnknownParameters = true;
            }
        }
        if (hasUnknownParameters) {
            throw new IllegalArgumentException("unrecognized parameters in normalization technique");
        }
    }

    private void validateNestedParameters(Map<String, Object> parameters, Set<String> supportedNestedParams) {
        if (Objects.isNull(parameters) || parameters.isEmpty()) {
            return;
        }
        boolean hasUnknownParameters = false;
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            String paramName = entry.getKey();

            if (Objects.nonNull(supportedNestedParams) && !supportedNestedParams.contains(paramName)) {
                hasUnknownParameters = true;
                break;
            }
        }
        if (hasUnknownParameters) {
            throw new IllegalArgumentException("unrecognized parameters in normalization technique");
        }
    }
}
