/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.Min;
import org.opensearch.search.aggregations.metrics.Percentiles;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.aggregations.metrics.ValueCount;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;

import java.util.List;

import static org.opensearch.search.aggregations.AggregationBuilders.min;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
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
        // Expected sum: (0.1 * 3) + (0.2 * 7) + (0.3 * 23) = 8.0
        assertThat(sum.getValue(), equalTo(8.0));
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
        assertThat(percentiles.percentile(50.0), greaterThan(0.0));
        assertThat(percentiles.percentile(95.0), greaterThan(0.0));
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
                .array("values", new double[]{0.1, 0.2, 0.3, 0.4, 0.5})
                .array("counts", new long[]{3, 7, 23, 12, 6})
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
        List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
        assertThat(buckets.size(), greaterThan(0));
    }
}
