/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import java.util.function.Supplier;

/**
 * Wrapper for params to create a fuzzy set.
 */
public class FuzzySetParameters {
    private final Supplier<Double> falsePositiveProbabilityProvider;
    private final FuzzySet.SetType setType;

    public static final double DEFAULT_FALSE_POSITIVE_PROBABILITY = 0.2047d;

    public FuzzySetParameters(Supplier<Double> falsePositiveProbabilityProvider) {
        this.falsePositiveProbabilityProvider = falsePositiveProbabilityProvider;
        this.setType = FuzzySet.SetType.BLOOM_FILTER_V1;
    }

    public double getFalsePositiveProbability() {
        return falsePositiveProbabilityProvider.get();
    }

    public FuzzySet.SetType getSetType() {
        return setType;
    }
}
