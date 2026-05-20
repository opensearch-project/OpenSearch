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
        registry.register(new TermsBucketTranslator());
        // TODO: add other aggregation translators
        return registry;
    }
}
