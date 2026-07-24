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
import org.opensearch.dsl.aggregation.metric.ExtendedStatsMetricTranslator;
import org.opensearch.dsl.aggregation.metric.MaxMetricTranslator;
import org.opensearch.dsl.aggregation.metric.MinMetricTranslator;
import org.opensearch.dsl.aggregation.metric.StatsMetricTranslator;
import org.opensearch.dsl.aggregation.metric.SumMetricTranslator;
import org.opensearch.dsl.aggregation.metric.ValueCountMetricTranslator;

/**
 * Creates an {@link AggregationRegistry} populated with all supported translators.
 */
public class AggregationRegistryFactory {

    private AggregationRegistryFactory() {}

    /** Creates a registry with all supported metric and bucket translators. */
    public static AggregationRegistry create() {
        AggregationRegistry registry = new AggregationRegistry();
        registry.register(new AvgMetricTranslator());
        registry.register(new SumMetricTranslator());
        registry.register(new MinMetricTranslator());
        registry.register(new MaxMetricTranslator());
        registry.register(new StatsMetricTranslator());
        registry.register(new ExtendedStatsMetricTranslator());
        registry.register(new ValueCountMetricTranslator());
        registry.register(new TermsBucketTranslator());
        return registry;
    }
}
