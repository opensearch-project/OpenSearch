/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.apache.commons.lang3.Range;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Collection of utility methods for score combination technique classes
 */
public class ScoreCombinationUtil {
    public static final String PARAM_NAME_WEIGHTS = "weights";
    private static final float DELTA_FOR_SCORE_ASSERTION = 0.01f;
    private static final float DELTA_FOR_WEIGHTS_ASSERTION = 0.01f;

    /**
     * Get collection of weights based on user provided config
     * @param params map of named parameters and their values
     * @return collection of weights
     */
    public List<Float> getWeights(final Map<String, Object> params) {
        if (Objects.isNull(params) || params.isEmpty()) {
            return List.of();
        }
        // get weights, we don't need to check for instance as it's done during validation
        List<Float> weightsList = ((List<Double>) params.getOrDefault(PARAM_NAME_WEIGHTS, List.of())).stream()
            .map(Double::floatValue)
            .collect(Collectors.toUnmodifiableList());
        validateWeights(weightsList);
        return weightsList;
    }

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
                    supportedParams.stream().collect(Collectors.joining(","))
                )
            );
        }

        // check param types
        if (actualParams.keySet().stream().anyMatch(PARAM_NAME_WEIGHTS::equalsIgnoreCase)) {
            if (!(actualParams.get(PARAM_NAME_WEIGHTS) instanceof List)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "parameter [%s] must be a collection of numbers", PARAM_NAME_WEIGHTS)
                );
            }
        }
    }

    /**
     * Get weight for sub-query based on its index in the hybrid search query. Use user provided weight or 1.0 otherwise
     * @param weights collection of weights for sub-queries
     * @param indexOfSubQuery 0-based index of sub-query in the Hybrid Search query
     * @return weight for sub-query, use one that is set in processor/pipeline definition or 1.0 as default
     */
    public float getWeightForSubQuery(final List<Float> weights, final int indexOfSubQuery) {
        return indexOfSubQuery < weights.size() ? weights.get(indexOfSubQuery) : 1.0f;
    }

    /**
     * Check if number of weights matches number of queries. This does not apply for case when
     * weights were not provided, as this is valid default value
     * @param scores collection of scores from all sub-queries of a single hybrid search query
     * @param weights score combination weights that are defined as part of search result processor
     */
    protected void validateIfWeightsMatchScores(final float[] scores, final List<Float> weights) {
        if (weights.isEmpty()) {
            return;
        }
        if (scores.length != weights.size()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "number of weights [%d] must match number of sub-queries [%d] in hybrid query",
                    weights.size(),
                    scores.length
                )
            );
        }
    }

    /**
     * Check if provided weights are valid for combination. Following conditions are checked:
     * - every weight is between 0.0 and 1.0
     * - sum of all weights must be equal 1.0
     * @param weightsList
     */
    private void validateWeights(final List<Float> weightsList) {
        boolean isOutOfRange = weightsList.stream().anyMatch(weight -> !Range.of(0.0f, 1.0f).contains(weight));
        if (isOutOfRange) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "all weights must be in range [0.0 ... 1.0], submitted weights: %s",
                    Arrays.toString(weightsList.toArray(new Float[0]))
                )
            );
        }
        float sumOfWeights = weightsList.stream().reduce(0.0f, Float::sum);
        if (!fuzzyEquals(1.0f, sumOfWeights, DELTA_FOR_WEIGHTS_ASSERTION)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "sum of weights for combination must be equal to 1.0, submitted weights: %s",
                    Arrays.toString(weightsList.toArray(new Float[0]))
                )
            );
        }
    }

    public static boolean fuzzyEquals(double a, double b, double delta) {
        return Math.abs(a - b) <= delta;
    }
}
