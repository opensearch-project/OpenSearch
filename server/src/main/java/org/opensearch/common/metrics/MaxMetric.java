/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.metrics;

import java.util.concurrent.atomic.LongAccumulator;

/**
 * A metric for tracking the maximum value seen.
 *
 * @opensearch.internal
 */
public class MaxMetric implements Metric {
    private final LongAccumulator max = new LongAccumulator(Long::max, Long.MIN_VALUE);

    public void collect(long value) {
        max.accumulate(value);
    }

    public long get() {
        return max.get();
    }

    public void clear() {
        max.reset();
    }
}
