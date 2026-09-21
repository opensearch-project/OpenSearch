/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * The sampling that produced an aggregation result, used to scale counts measured on a sample back up to the
 * population they were drawn from.
 * <p>
 * Passed to {@link InternalAggregation#finalizeSampling(SamplingContext)} once, during the final reduce, by the
 * aggregation that did the sampling.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class SamplingContext {

    /**
     * No sampling took place, so nothing is scaled.
     */
    public static final SamplingContext NONE = new SamplingContext(1.0);

    private final double probability;

    public SamplingContext(double probability) {
        if (Double.isNaN(probability) || probability <= 0.0 || probability > 1.0) {
            throw new IllegalArgumentException("[probability] must be greater than 0.0 and at most 1.0, but was [" + probability + "]");
        }
        this.probability = probability;
    }

    public double probability() {
        return probability;
    }

    /**
     * @return true when this context scales nothing, so callers can skip walking the aggregation tree
     */
    public boolean isNoop() {
        return probability == 1.0;
    }

    /**
     * Scales a count measured on the sample up to an estimate for the population.
     */
    public long scaleUp(long count) {
        if (isNoop()) {
            return count;
        }
        return Math.round(count / probability);
    }

    /**
     * Scales a sum, or any other quantity that grows linearly with the number of documents, up to an estimate for the
     * population. Averages, minima, maxima and variances must not be passed here: they estimate the population value
     * already.
     */
    public double scaleUp(double value) {
        if (isNoop()) {
            return value;
        }
        return value / probability;
    }
}
