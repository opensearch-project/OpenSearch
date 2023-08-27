/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.index.FieldInfo;

public class FuzzySetParameters {
    private final double falsePositiveProbability;
    private final FuzzySet.SetType setType;

    public static final double DEFAULT_FALSE_POSITIVE_PROBABILITY = 0.2047d;

    public FuzzySetParameters(double falsePositiveProbability, FuzzySet.SetType setType) {
        this.falsePositiveProbability = falsePositiveProbability;
        this.setType = setType;
    }

    public double getFalsePositiveProbability() {
        return falsePositiveProbability;
    }

    public FuzzySet.SetType getSetType() {
        return setType;
    }
}
