/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Integration tests for DSL aggregation conversion.
 * Uses matchAllQuery; focus is on aggregation plan building.
 */
public class DslAggregationIT extends DslIntegTestBase {

    public void testMetricOnly() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.avg("avg_price").field("price"))));
    }

    public void testMultipleMetrics() {
        createTestIndex();
        assertOk(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(AggregationBuilders.avg("avg_price").field("price"))
                    .aggregation(AggregationBuilders.sum("total_price").field("price"))
                    .aggregation(AggregationBuilders.min("min_price").field("price"))
                    .aggregation(AggregationBuilders.max("max_price").field("price"))
            )
        );
    }

    public void testTermsBucket() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().size(0).aggregation(new TermsAggregationBuilder("by_brand").field("brand"))));
    }

    public void testTermsBucketWithMetric() {
        createTestIndex();
        assertOk(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(
                        new TermsAggregationBuilder("by_brand").field("brand")
                            .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
                    )
            )
        );
    }

    public void testNestedBuckets() {
        createTestIndex();
        assertOk(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(
                        new TermsAggregationBuilder("by_brand").field("brand")
                            .subAggregation(AggregationBuilders.sum("total").field("price"))
                            .subAggregation(
                                new TermsAggregationBuilder("by_name").field("name")
                                    .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
                            )
                    )
            )
        );
    }

    public void testAggsWithHits() {
        createTestIndex();
        // size > 0 with aggs produces both HITS + AGGREGATION plans
        assertOk(search(new SearchSourceBuilder().size(10).aggregation(AggregationBuilders.avg("avg_price").field("price"))));
    }

    public void testTermsBucketOrderByKeyAsc() {
        createTestIndex();
        assertOk(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.key(true)))
            )
        );
    }

    public void testTermsBucketOrderByKeyDesc() {
        createTestIndex();
        assertOk(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.key(false)))
            )
        );
    }

    public void testTermsBucketOrderByCountAsc() {
        createTestIndex();
        assertOk(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.count(true)))
            )
        );
    }

    public void testTermsBucketOrderByMetric() {
        createTestIndex();
        assertOk(
            search(
                new SearchSourceBuilder().size(0)
                    .aggregation(
                        new TermsAggregationBuilder("by_brand").field("brand")
                            .order(BucketOrder.aggregation("avg_price", false))
                            .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
                    )
            )
        );
    }
}
