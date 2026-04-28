/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.Map;
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
    private final ConcurrentHashMap<Map<String, ?>, Double> counterValueForTags = new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    public TestInMemoryCounter() {}

    /**
     * returns the counter value.
     * @return
     */
    public Integer getCounterValue() {
        return this.counterValue.get();
    }

    /**
     * returns the counter value tags
     * @return
     */
    public ConcurrentHashMap<Map<String, ?>, Double> getCounterValueForTags() {
        return this.counterValueForTags;
    }

    @Override
    public void add(double value) {
        counterValue.addAndGet((int) value);
    }

    @Override
    public synchronized void add(double value, Tags tags) {
        Map<String, ?> tagsMap = tags.getTagsMap();
        if (counterValueForTags.get(tagsMap) == null) {
            counterValueForTags.put(tagsMap, value);
        } else {
            value = counterValueForTags.get(tagsMap) + value;
            counterValueForTags.put(tagsMap, value);
        }
    }
}
