/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.range.Range;
import org.opensearch.search.aggregations.metrics.Avg;
import org.opensearch.search.aggregations.metrics.ExtendedStats;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.Min;
import org.opensearch.search.aggregations.metrics.PercentileRanks;
import org.opensearch.search.aggregations.metrics.Percentiles;
import org.opensearch.search.aggregations.metrics.Stats;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.aggregations.metrics.ValueCount;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;

import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.opensearch.search.aggregations.AggregationBuilders.avg;
import static org.opensearch.search.aggregations.AggregationBuilders.extendedStats;
import static org.opensearch.search.aggregations.AggregationBuilders.min;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.percentileRanks;
import static org.opensearch.search.aggregations.AggregationBuilders.range;
import static org.opensearch.search.aggregations.AggregationBuilders.stats;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.search.aggregations.AggregationBuilders.count;
import static org.opensearch.search.aggregations.AggregationBuilders.percentiles;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public class HistogramFieldQueryTests extends OpenSearchSingleNodeTestCase {

    private static final String defaultHistogramField = "histogram_field_name";
    private static final String defaultIndexName = "test";

    protected XContentBuilder createMapping() throws Exception {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(defaultHistogramField)
            .field("type", "histogram")
            .endObject()
            .endObject()
            .endObject();
    }

    public void testExistsQuery() throws Exception {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        // Index document with histogram
        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(defaultHistogramField)
                .array("values", new double[]{0.1, 0.2, 0.3})
                .array("counts", new long[]{3, 7, 23})
                .endObject()
                .endObject())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        // Index document without histogram
        client().prepareIndex(defaultIndexName)
            .setId("2")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .field("other_field", "value")
                .endObject())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .setQuery(QueryBuilders.existsQuery(defaultHistogramField))
            .get();

        assertSearchResponse(response);
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testMinAggregation() throws Exception {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(defaultHistogramField)
                .array("values", new double[]{0.1, 0.2, 0.3})
                .array("counts", new long[]{3, 7, 23})
                .endObject()
                .endObject())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .addAggregation(min("min_value").field(defaultHistogramField))
            .get();

        assertSearchResponse(response);
        Min min = response.getAggregations().get("min_value");
        assertThat(min.getValue(), equalTo(0.1));
    }

    public void testMaxAggregation() throws Exception {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(defaultHistogramField)
                .array("values", new double[]{0.1, 0.2, 0.3})
                .array("counts", new long[]{3, 7, 23})
                .endObject()
                .endObject())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .addAggregation(max("max_value").field(defaultHistogramField))
            .get();

        assertSearchResponse(response);
        Max max = response.getAggregations().get("max_value");
        assertThat(max.getValue(), equalTo(0.3));
    }

    public void testSumAggregation() throws Exception {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(defaultHistogramField)
                .array("values", new double[]{0.1, 0.2, 0.3})
                .array("counts", new long[]{3, 7, 23})
                .endObject()
                .endObject())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .addAggregation(sum("sum_value").field(defaultHistogramField))
            .get();

        assertSearchResponse(response);
        Sum sum = response.getAggregations().get("sum_value");
        // Expected sum: (0.1 * 3) + (0.2 * 7) + (0.3 * 23) = 8.6
        assertThat(sum.getValue(), equalTo(8.6));
    }

    public void testValueCountAggregation() throws Exception {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(defaultHistogramField)
                .array("values", new double[]{0.1, 0.2, 0.3})
                .array("counts", new long[]{3, 7, 23})
                .endObject()
                .endObject())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .addAggregation(count("value_count").field(defaultHistogramField))
            .get();

        assertSearchResponse(response);
        ValueCount valueCount = response.getAggregations().get("value_count");
        assertThat(valueCount.getValue(), equalTo(33L)); // Sum of counts: 3 + 7 + 23
    }

    public void testAvgAggregation() throws Exception {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(defaultHistogramField)
                .array("values", new double[]{0.1, 0.2, 0.3})
                .array("counts", new long[]{3, 7, 23})
                .endObject()
                .endObject())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .addAggregation(avg("avg_value").field(defaultHistogramField))
            .get();

        assertSearchResponse(response);
        Avg avg = response.getAggregations().get("avg_value");
        // Expected avg: ((0.1 * 3) + (0.2 * 7) + (0.3 * 23)) / (3 + 7 + 23) = 8.6 / 33 ≈ 0.26
        assertThat(avg.getValue(), closeTo(0.26, 0.01));
    }

    public void testPercentilesAggregation() throws Exception {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(defaultHistogramField)
                .array("values", new double[]{0.1, 0.2, 0.3})
                .array("counts", new long[]{3, 7, 23})
                .endObject()
                .endObject())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .addAggregation(
                percentiles("percentiles")
                    .field(defaultHistogramField)
                    .percentiles(50.0, 95.0)
            )
            .get();

        assertSearchResponse(response);
        Percentiles percentiles = response.getAggregations().get("percentiles");
        assertThat(percentiles, notNullValue());

        // Values should be within reasonable bounds given our distribution
        double p50 = percentiles.percentile(50.0);
        double p95 = percentiles.percentile(95.0);

        // Basic sanity checks
        assertThat(p50, greaterThanOrEqualTo(0.1));  // Should be at least our minimum value
        assertThat(p50, lessThanOrEqualTo(0.3));     // Should not exceed our maximum value
        assertThat(p95, greaterThanOrEqualTo(p50));  // 95th percentile should be >= 50th percentile
        assertThat(p95, lessThanOrEqualTo(0.3));     // Should not exceed our maximum value
    }

    public void testStatsAggregation() throws Exception {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(defaultHistogramField)
                .array("values", new double[]{0.1, 0.2, 0.3})
                .array("counts", new long[]{3, 7, 23})
                .endObject()
                .endObject())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .addAggregation(stats("stats_value").field(defaultHistogramField))
            .get();

        assertSearchResponse(response);
        Stats stats = response.getAggregations().get("stats_value");
        assertThat(stats.getMin(), equalTo(0.1));
        assertThat(stats.getMax(), equalTo(0.3));
        assertThat(stats.getSum(), equalTo(8.6)); // 0.1*3 + 0.2*7 + 0.3*23
        assertThat(stats.getCount(), equalTo(33L)); // sum of counts
        assertThat(stats.getAvg(), closeTo(0.26, 0.01)); // 8.6 / 33
    }

    public void testExtendedStatsAggregation() throws Exception {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(defaultHistogramField)
                .array("values", new double[]{0.1, 0.2, 0.3})
                .array("counts", new long[]{3, 7, 23})
                .endObject()
                .endObject())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .addAggregation(extendedStats("extended_stats").field(defaultHistogramField))
            .get();

        assertSearchResponse(response);
        ExtendedStats stats = response.getAggregations().get("extended_stats");
        assertThat(stats.getCount(), equalTo(33L));
        assertThat(stats.getMin(), equalTo(0.1));
        assertThat(stats.getMax(), equalTo(0.3));
        assertThat(stats.getSum(), equalTo(8.6));
        assertThat(stats.getAvg(), closeTo(0.26, 0.01));
        assertThat(stats.getVariance(), greaterThanOrEqualTo(0.0));
        assertThat(stats.getStdDeviation(), greaterThanOrEqualTo(0.0));
    }

    public void testPercentileRanksAggregation() throws Exception {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(defaultHistogramField)
                .array("values", new double[]{0.1, 0.2, 0.3})
                .array("counts", new long[]{3, 7, 23})
                .endObject()
                .endObject())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .addAggregation(percentileRanks("percentile_ranks", new double[]{0.2, 0.3}).field(defaultHistogramField))
            .get();

        assertSearchResponse(response);
        PercentileRanks ranks = response.getAggregations().get("percentile_ranks");
        assertThat(ranks, notNullValue());
        assertThat(ranks.percent(0.2), greaterThanOrEqualTo(0.0));
        assertThat(ranks.percent(0.3), greaterThanOrEqualTo(0.0));
    }

    public void testRangeAggregation() throws Exception {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(defaultHistogramField)
                .array("values", new double[]{0.1, 0.2, 0.3})
                .array("counts", new long[]{3, 7, 23})
                .endObject()
                .endObject())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .addAggregation(
                range("range_agg")
                    .field(defaultHistogramField)
                    .addUnboundedTo(0.15)
                    .addRange(0.15, 0.25)
                    .addUnboundedFrom(0.25)
            )
            .get();

        assertSearchResponse(response);
        Range range = response.getAggregations().get("range_agg");
        assertThat(range, notNullValue());
        for (Range.Bucket bucket : range.getBuckets()) {
            assertThat(bucket.getDocCount(), greaterThanOrEqualTo(0L));
        }
    }

    public void testHistogramAggregation() throws Exception {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(defaultHistogramField)
                .array("values", new double[]{0.1, 0.2, 0.3})
                .array("counts", new long[]{3, 7, 23})
                .endObject()
                .endObject())
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .addAggregation(
                histogram("histogram_agg")
                    .field(defaultHistogramField)
                    .interval(0.1)
            )
            .get();

        assertSearchResponse(response);
        Histogram histogram = response.getAggregations().get("histogram_agg");
        assertThat(histogram, notNullValue());
        for (Histogram.Bucket bucket : histogram.getBuckets()) {
            assertThat(bucket.getDocCount(), greaterThanOrEqualTo(0L));
        }
    }


}
