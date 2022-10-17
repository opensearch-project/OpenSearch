/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

/**
 * MovingAverage is used to calculate the moving average of last 'n' observations.
 *
 * @opensearch.internal
 */
public class MovingAverage {
    private final int windowSize;
    private final long[] observations;

    private long count = 0;
    private long sum = 0;
    private double average = 0;

    public MovingAverage(int windowSize) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("window size must be greater than zero");
        }

        this.windowSize = windowSize;
        this.observations = new long[windowSize];
    }

    /**
     * Records a new observation and evicts the n-th last observation.
     */
    public synchronized double record(long value) {
        long delta = value - observations[(int) (count % observations.length)];
        observations[(int) (count % observations.length)] = value;

        count++;
        sum += delta;
        average = (double) sum / Math.min(count, observations.length);
        return average;
    }

    public double getAverage() {
        return average;
    }

    public long getCount() {
        return count;
    }

    public boolean isReady() {
        return count >= windowSize;
    }
}
