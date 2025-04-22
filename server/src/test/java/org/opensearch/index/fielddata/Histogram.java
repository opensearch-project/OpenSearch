/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

public class Histogram {
    private final double[] values;
    private final long[] counts;

    public Histogram(double[] values, long[] counts) {
        this.values = values;
        this.counts = counts;
    }

    public double[] getValues() {
        return values;
    }

    public long[] getCounts() {
        return counts;
    }

    public int getSize() {
        return values.length;
    }
}
