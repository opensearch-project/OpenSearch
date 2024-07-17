/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.TaggedMeasurement;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * This is a simple implementation of MetricsRegistry which can be utilized by Unit Tests.
 * It just initializes and stores counters/histograms within a map, once created.
 * The maps can then be used to get the counters/histograms by their names.
 */
public class TestInMemoryMetricsRegistry implements MetricsRegistry {

    private ConcurrentHashMap<String, TestInMemoryCounter> counterStore = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, TestInMemoryHistogram> histogramStore = new ConcurrentHashMap<>();

    public ConcurrentHashMap<String, TestInMemoryCounter> getCounterStore() {
        return this.counterStore;
    }

    public ConcurrentHashMap<String, TestInMemoryHistogram> getHistogramStore() {
        return this.histogramStore;
    }

    @Override
    public Counter createCounter(String name, String description, String unit) {
        TestInMemoryCounter counter = new TestInMemoryCounter();
        counterStore.putIfAbsent(name, counter);
        return counter;
    }

    @Override
    public Counter createUpDownCounter(String name, String description, String unit) {
        /**
         * ToDo: To be implemented when required.
         */
        return null;
    }

    @Override
    public Histogram createHistogram(String name, String description, String unit) {
        TestInMemoryHistogram histogram = new TestInMemoryHistogram();
        histogramStore.putIfAbsent(name, histogram);
        return histogram;
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<Double> valueProvider, Tags tags) {
        /**
         * ToDo: To be implemented when required.
         */
        return null;
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<TaggedMeasurement> value) {
        return null;
    }

    @Override
    public void close() throws IOException {}
}
