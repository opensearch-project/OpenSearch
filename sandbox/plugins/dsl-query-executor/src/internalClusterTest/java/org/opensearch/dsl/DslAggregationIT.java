/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;

/**
 * Integration tests for DSL aggregation conversion.
 * Uses matchAllQuery; focus is on aggregation plan building.
 */
public class DslAggregationIT extends DslIntegTestBase {

    public void testMetricOnly() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(AggregationBuilders.avg("avg_price").field("price"))
        ));
    }

    public void testMultipleMetrics() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(AggregationBuilders.avg("avg_price").field("price"))
            .aggregation(AggregationBuilders.sum("total_price").field("price"))
            .aggregation(AggregationBuilders.min("min_price").field("price"))
            .aggregation(AggregationBuilders.max("max_price").field("price"))
        ));
    }

    public void testTermsBucket() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand"))
        ));
    }

    public void testTermsBucketWithMetric() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand")
                .subAggregation(AggregationBuilders.avg("avg_price").field("price")))
        ));
    }

    public void testNestedBuckets() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand")
                .subAggregation(AggregationBuilders.sum("total").field("price"))
                .subAggregation(new TermsAggregationBuilder("by_name").field("name")
                    .subAggregation(AggregationBuilders.avg("avg_price").field("price"))))
        ));
    }

    public void testAggsWithHits() {
        createTestIndex();
        // size > 0 with aggs produces both HITS + AGGREGATION plans
        assertOk(search(new SearchSourceBuilder()
            .size(10)
            .aggregation(AggregationBuilders.avg("avg_price").field("price"))
        ));
    }

    public void testHistogram() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(AggregationBuilders.histogram("price_histogram").field("price").interval(100))
        ));
    }

    public void testHistogramWithOffset() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(AggregationBuilders.histogram("price_histogram").field("price").interval(20).offset(5))
        ));
    }

    public void testHistogramWithMinDocCount() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(AggregationBuilders.histogram("price_histogram").field("price").interval(10).minDocCount(2))
        ));
    }

    public void testHistogramWithMetric() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(AggregationBuilders.histogram("price_histogram").field("price").interval(50)
                .subAggregation(AggregationBuilders.avg("avg_price").field("price")))
        ));
    }

    public void testDateHistogramCalendarInterval() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(dateHistogram("date_hist").field("timestamp").calendarInterval(DateHistogramInterval.MONTH))
        ));
    }

    public void testDateHistogramFixedInterval() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(dateHistogram("date_hist").field("timestamp").fixedInterval(DateHistogramInterval.hours(2)))
        ));
    }

    public void testDateHistogramWithMinDocCount() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(dateHistogram("date_hist").field("timestamp").fixedInterval(DateHistogramInterval.hours(1)).minDocCount(2))
        ));
    }

    public void testDateHistogramWithMetricSubAgg() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(dateHistogram("date_hist")
                .field("timestamp")
                .calendarInterval(DateHistogramInterval.DAY)
                .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
            )
        ));
    }

    public void testTermsWithDateHistogramSubAgg() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(AggregationBuilders.terms("category_terms")
                .field("category")
                .subAggregation(dateHistogram("date_hist").field("timestamp").calendarInterval(DateHistogramInterval.DAY))
            )
        ));
    }

    public void testDateHistogramWithTermsSubAgg() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(dateHistogram("date_hist")
                .field("timestamp")
                .calendarInterval(DateHistogramInterval.DAY)
                .subAggregation(AggregationBuilders.terms("category_terms").field("category"))
            )
        ));
    }

    public void testNestedDateHistograms() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(dateHistogram("by_day")
                .field("timestamp")
                .calendarInterval(DateHistogramInterval.DAY)
                .subAggregation(dateHistogram("by_hour").field("timestamp").fixedInterval(DateHistogramInterval.hours(6)))
            )
        ));
    }

    public void testSiblingDateHistogramsDifferentIntervals() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()
            .size(0)
            .aggregation(dateHistogram("by_day").field("timestamp").calendarInterval(DateHistogramInterval.DAY))
            .aggregation(dateHistogram("by_hour").field("timestamp").fixedInterval(DateHistogramInterval.hours(1)))
        ));
    }
}
