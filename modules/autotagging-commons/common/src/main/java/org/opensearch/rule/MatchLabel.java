/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

/**
 * Represents a feature value along with a matching score.
 */
public class MatchLabel<V> {
    private final V featureValue;
    private final float matchScore;

    /**
     * Constructs a FeatureValueMatch with the given feature value and score.
     * @param featureValue the feature value
     * @param matchScore the matching score
     */
    public MatchLabel(V featureValue, float matchScore) {
        this.featureValue = featureValue;
        this.matchScore = matchScore;
    }

    /**
     * Returns the feature value.
     */
    public V getFeatureValue() {
        return featureValue;
    }

    /**
     * Returns the match score.
     */
    public float getMatchScore() {
        return matchScore;
    }
}
