/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import java.util.Objects;

/**
 * Used to calculate sums using the Kahan summation algorithm.
 *
 * <p>The Kahan summation algorithm (also known as compensated summation) reduces the numerical errors that
 * occur when adding a sequence of finite precision floating point numbers. Numerical errors arise due to
 * truncation and rounding. These errors can lead to numerical instability.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Kahan_summation_algorithm">Kahan Summation Algorithm</a>
 *
 * @opensearch.internal
 */
public class CompensatedSum {

    private static final double NO_CORRECTION = 0.0;

    private double value;
    private double delta;

    /**
     * Used to calculate sums using the Kahan summation algorithm.
     *
     * @param value the sum
     * @param delta correction term
     */
    public CompensatedSum(double value, double delta) {
        this.value = value;
        this.delta = delta;
    }

    /**
     * The value of the sum.
     */
    public double value() {
        return value;
    }

    /**
     * The correction term.
     */
    public double delta() {
        return delta;
    }

    /**
     * Increments the Kahan sum by adding a value without a correction term.
     */
    public CompensatedSum add(double value) {
        return add(value, NO_CORRECTION);
    }

    /**
     * Resets the internal state to use the new value and compensation delta
     */
    public void reset(double value, double delta) {
        this.value = value;
        this.delta = delta;
    }

    /**
     * Increments the Kahan sum by adding two sums, and updating the correction term for reducing numeric errors.
     */
    public CompensatedSum add(double value, double delta) {
        // If the value is Inf or NaN, just add it to the running tally to "convert" to
        // Inf/NaN. This keeps the behavior bwc from before kahan summing
        if (Double.isFinite(value) == false) {
            this.value = value + this.value;
        }

        if (Double.isFinite(this.value)) {
            double correctedSum = value + (this.delta + delta);
            double updatedValue = this.value + correctedSum;
            this.delta = correctedSum - (updatedValue - this.value);
            this.value = updatedValue;
        }

        return this;
    }

    /**
     * Increments the Kahan sum by adding two sums, and updating the correction term for reducing numeric errors.
     */
    public void add(double[] values, int count) {
        // If the value is Inf or NaN, just add it to the running tally to "convert" to
        // Inf/NaN. This keeps the behavior bwc from before kahan summing
        double sum = value;
        double c = delta; // Compensation for lost low-order bits

        for (int i = 0; i < count; i++) {
            double y = values[i] - c;
            double t = sum + y;
            c = (t - sum) - y; // Calculate the lost part
            sum = t;
        }

        this.value = sum;
        this.delta = c;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompensatedSum that = (CompensatedSum) o;
        return Double.compare(that.value, value) == 0 && Double.compare(that.delta, delta) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, delta);
    }

    @Override
    public String toString() {
        return value + " " + delta;
    }

}
