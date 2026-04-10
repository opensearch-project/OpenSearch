/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.dsl.aggregation.bucket.FilterBucketTranslator;
import org.opensearch.dsl.aggregation.bucket.FiltersBucketTranslator;
import org.opensearch.dsl.aggregation.bucket.TermsBucketTranslator;
import org.opensearch.dsl.aggregation.metric.AvgMetricTranslator;
import org.opensearch.dsl.aggregation.metric.MaxMetricTranslator;
import org.opensearch.dsl.aggregation.metric.MinMetricTranslator;
import org.opensearch.dsl.aggregation.metric.SumMetricTranslator;
import org.opensearch.dsl.aggregation.pipeline.parent.BucketSortTranslator;
import org.opensearch.dsl.aggregation.pipeline.sibling.AvgBucketTranslator;
import org.opensearch.dsl.aggregation.pipeline.sibling.ExtendedStatsBucketTranslator;
import org.opensearch.dsl.aggregation.pipeline.sibling.MaxBucketTranslator;
import org.opensearch.dsl.aggregation.pipeline.sibling.MinBucketTranslator;
import org.opensearch.dsl.aggregation.pipeline.sibling.PercentilesBucketTranslator;
import org.opensearch.dsl.aggregation.pipeline.sibling.StatsBucketTranslator;
import org.opensearch.dsl.aggregation.pipeline.sibling.SumBucketTranslator;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.query.QueryRegistryFactory;

/**
 * Creates an {@link AggregationRegistry} populated with all supported translators.
 */
public class AggregationRegistryFactory {

    private AggregationRegistryFactory() {}

    /** Creates a registry with all supported metric, bucket, and pipeline translators. */
    public static AggregationRegistry create() {
        QueryRegistry queryRegistry = QueryRegistryFactory.create();
        AggregationRegistry registry = new AggregationRegistry();
        registry.register(new AvgMetricTranslator());
        registry.register(new SumMetricTranslator());
        registry.register(new MinMetricTranslator());
        registry.register(new MaxMetricTranslator());
        registry.register(new TermsBucketTranslator());
        registry.register(new FilterBucketTranslator(queryRegistry));
        registry.register(new FiltersBucketTranslator(queryRegistry));

        // Sibling pipeline translators
        registry.registerPipeline(new AvgBucketTranslator());
        registry.registerPipeline(new SumBucketTranslator());
        registry.registerPipeline(new MinBucketTranslator());
        registry.registerPipeline(new MaxBucketTranslator());
        registry.registerPipeline(new StatsBucketTranslator());
        registry.registerPipeline(new ExtendedStatsBucketTranslator());
        registry.registerPipeline(new PercentilesBucketTranslator());

        // Parent pipeline translators
        registry.registerPipeline(new BucketSortTranslator());

        return registry;
    }
}
