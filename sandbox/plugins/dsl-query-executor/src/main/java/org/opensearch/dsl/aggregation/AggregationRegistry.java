/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.dsl.aggregation.bucket.BucketTranslator;
import org.opensearch.dsl.aggregation.metric.MetricTranslator;
import org.opensearch.search.aggregations.AggregationBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry of all aggregation translators — both metric and bucket.
 * Single map keyed by AggregationBuilder class, O(1) lookup.
 * Callers use {@code instanceof} to distinguish metric from bucket.
 */
public class AggregationRegistry {

    private final Map<Class<? extends AggregationBuilder>, AggregationType<?>> translators = new HashMap<>();

    /** Creates an empty registry. */
    public AggregationRegistry() {}

    /**
     * Registers a translator (metric or bucket).
     *
     * @param translator the translator to register
     */
    public void register(AggregationType<?> translator) {
        translators.put(translator.getAggregationType(), translator);
    }

    /**
     * Returns the translator for the given class, or null.
     * Caller checks {@code instanceof MetricTranslator} or {@code instanceof BucketTranslator}.
     *
     * @param aggClass the aggregation builder class
     * @return the translator, or null
     */
    public AggregationType<?> get(Class<? extends AggregationBuilder> aggClass) {
        return translators.get(aggClass);
    }

    /**
     * Returns the metric translator for the given class, or null.
     *
     * @param aggClass the aggregation builder class
     * @return the metric translator, or null
     */
    @SuppressWarnings("unchecked")
    public <T extends AggregationBuilder> MetricTranslator<T> getMetric(Class<? extends AggregationBuilder> aggClass) {
        AggregationType<?> translator = translators.get(aggClass);
        return translator instanceof MetricTranslator ? (MetricTranslator<T>) translator : null;
    }

    /**
     * Returns the bucket translator for the given class, or null.
     *
     * @param aggClass the aggregation builder class
     * @return the bucket translator, or null
     */
    @SuppressWarnings("unchecked")
    public <T extends AggregationBuilder> BucketTranslator<T> getBucket(Class<? extends AggregationBuilder> aggClass) {
        AggregationType<?> translator = translators.get(aggClass);
        return translator instanceof BucketTranslator ? (BucketTranslator<T>) translator : null;
    }
}
