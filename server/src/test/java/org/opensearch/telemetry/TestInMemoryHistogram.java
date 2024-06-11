/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a simple implementation of Histogram which is utilized by TestInMemoryMetricsRegistry for
 * Unit Tests. It initializes an atomic integer to record the value of histogram which doesn't have any tags
 * along with a map to store the values recorded against the tags.
 * The map and atomic integer can then be used to get the recorded values.
 */
public class TestInMemoryHistogram implements Histogram {

    private AtomicInteger histogramValue = new AtomicInteger(0);
    private ConcurrentHashMap<HashMap<String, ?>, Double> histogramValueForTags = new ConcurrentHashMap<>();

    public Integer getHistogramValue() {
        return this.histogramValue.get();
    }

    public ConcurrentHashMap<HashMap<String, ?>, Double> getHistogramValueForTags() {
        return this.histogramValueForTags;
    }

    @Override
    public void record(double value) {
        histogramValue.addAndGet((int) value);
    }

    @Override
    public synchronized void record(double value, Tags tags) {
        HashMap<String, ?> hashMap = (HashMap<String, ?>) tags.getTagsMap();
        histogramValueForTags.put(hashMap, value);
    }
}
