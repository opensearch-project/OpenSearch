/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.dsl.aggregation.bucket.TermsBucketTranslator;
import org.opensearch.dsl.aggregation.metric.AvgMetricTranslator;
import org.opensearch.dsl.aggregation.metric.MaxMetricTranslator;
import org.opensearch.dsl.aggregation.metric.MinMetricTranslator;
import org.opensearch.dsl.aggregation.metric.SumMetricTranslator;

/**
 * Returns the process-wide {@link AggregationRegistry} populated with all supported
 * metric and bucket translators. Registrations are effectively immutable after class
 * init, and the registry is safe to share across threads (concurrent reads, no writes
 * at steady state).
 */
public class AggregationRegistryFactory {

    /** Built once at class init and cached forever. */
    private static final AggregationRegistry INSTANCE = build();

    private AggregationRegistryFactory() {}

    private static AggregationRegistry build() {
        AggregationRegistry registry = new AggregationRegistry();
        registry.register(new AvgMetricTranslator());
        registry.register(new SumMetricTranslator());
        registry.register(new MinMetricTranslator());
        registry.register(new MaxMetricTranslator());
        registry.register(new TermsBucketTranslator());
        // TODO: add other aggregation translators
        return registry;
    }

    /** Returns the shared registry. All callers see the same instance. */
    public static AggregationRegistry create() {
        return INSTANCE;
    }
}
