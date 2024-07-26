/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a simple implementation of Counter which is utilized by TestInMemoryMetricsRegistry for
 * Unit Tests. It initializes an atomic integer to add the values of counter which doesn't have any tags
 * along with a map to store the values recorded against the tags.
 * The map and atomic integer can then be used to get the added values.
 */
public class TestInMemoryCounter implements Counter {

    private AtomicInteger counterValue = new AtomicInteger(0);
    private ConcurrentHashMap<HashMap<String, ?>, Double> counterValueForTags = new ConcurrentHashMap<>();

    public Integer getCounterValue() {
        return this.counterValue.get();
    }

    public ConcurrentHashMap<HashMap<String, ?>, Double> getCounterValueForTags() {
        return this.counterValueForTags;
    }

    @Override
    public void add(double value) {
        counterValue.addAndGet((int) value);
    }

    @Override
    public synchronized void add(double value, Tags tags) {
        HashMap<String, ?> hashMap = (HashMap<String, ?>) tags.getTagsMap();
        if (counterValueForTags.get(hashMap) == null) {
            counterValueForTags.put(hashMap, value);
        } else {
            value = counterValueForTags.get(hashMap) + value;
            counterValueForTags.put(hashMap, value);
        }
    }
}
