/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;
import org.opensearch.search.aggregations.metrics.Cardinality;
import org.opensearch.search.aggregations.metrics.GeoCentroid;
import org.opensearch.search.aggregations.metrics.Percentiles;
import org.opensearch.search.aggregations.metrics.Stats;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE;
import static org.opensearch.search.aggregations.AggregationBuilders.cardinality;
import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.opensearch.search.aggregations.AggregationBuilders.geoCentroid;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.percentiles;
import static org.opensearch.search.aggregations.AggregationBuilders.stats;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class MissingValueIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public MissingValueIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {
                Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), CONCURRENT_SEGMENT_SEARCH_MODE_ALL).build() },
            new Object[] {
                Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), CONCURRENT_SEGMENT_SEARCH_MODE_AUTO).build() },
            new Object[] {
                Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), CONCURRENT_SEGMENT_SEARCH_MODE_NONE).build() }
        );
    }

    @Override
    protected int maximumNumberOfShards() {
        return 2;
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx").setMapping("date", "type=date", "location", "type=geo_point", "str", "type=keyword").get());
        indexRandom(
            true,
            client().prepareIndex("idx").setId("1").setSource(),
            client().prepareIndex("idx")
                .setId("2")
                .setSource("str", "foo", "long", 3L, "double", 5.5, "date", "2015-05-07", "location", "1,2")
        );
    }

    public void testUnmappedTerms() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(terms("my_terms").field("non_existing_field").missing("bar"))
            .get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("my_terms");
        assertEquals(1, terms.getBuckets().size());
        assertEquals(2, terms.getBucketByKey("bar").getDocCount());
    }

    public void testStringTerms() {
        for (ExecutionMode mode : ExecutionMode.values()) {
            SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("my_terms").field("str").executionHint(mode.toString()).missing("bar"))
                .get();
            assertSearchResponse(response);
            Terms terms = response.getAggregations().get("my_terms");
            assertEquals(2, terms.getBuckets().size());
            assertEquals(1, terms.getBucketByKey("foo").getDocCount());
            assertEquals(1, terms.getBucketByKey("bar").getDocCount());

            response = client().prepareSearch("idx").addAggregation(terms("my_terms").field("str").missing("foo")).get();
            assertSearchResponse(response);
            terms = response.getAggregations().get("my_terms");
            assertEquals(1, terms.getBuckets().size());
            assertEquals(2, terms.getBucketByKey("foo").getDocCount());
        }
    }

    public void testLongTerms() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(terms("my_terms").field("long").missing(4)).get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("my_terms");
        assertEquals(2, terms.getBuckets().size());
        assertEquals(1, terms.getBucketByKey("3").getDocCount());
        assertEquals(1, terms.getBucketByKey("4").getDocCount());

        response = client().prepareSearch("idx").addAggregation(terms("my_terms").field("long").missing(3)).get();
        assertSearchResponse(response);
        terms = response.getAggregations().get("my_terms");
        assertEquals(1, terms.getBuckets().size());
        assertEquals(2, terms.getBucketByKey("3").getDocCount());
    }

    public void testDoubleTerms() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(terms("my_terms").field("double").missing(4.5)).get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("my_terms");
        assertEquals(2, terms.getBuckets().size());
        assertEquals(1, terms.getBucketByKey("4.5").getDocCount());
        assertEquals(1, terms.getBucketByKey("5.5").getDocCount());

        response = client().prepareSearch("idx").addAggregation(terms("my_terms").field("double").missing(5.5)).get();
        assertSearchResponse(response);
        terms = response.getAggregations().get("my_terms");
        assertEquals(1, terms.getBuckets().size());
        assertEquals(2, terms.getBucketByKey("5.5").getDocCount());
    }

    public void testUnmappedHistogram() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(histogram("my_histogram").field("non-existing_field").interval(5).missing(12))
            .get();
        assertSearchResponse(response);
        Histogram histogram = response.getAggregations().get("my_histogram");
        assertEquals(1, histogram.getBuckets().size());
        assertEquals(10d, histogram.getBuckets().get(0).getKey());
        assertEquals(2, histogram.getBuckets().get(0).getDocCount());
    }

    public void testHistogram() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(histogram("my_histogram").field("long").interval(5).missing(7))
            .get();
        assertSearchResponse(response);
        Histogram histogram = response.getAggregations().get("my_histogram");
        assertEquals(2, histogram.getBuckets().size());
        assertEquals(0d, histogram.getBuckets().get(0).getKey());
        assertEquals(1, histogram.getBuckets().get(0).getDocCount());
        assertEquals(5d, histogram.getBuckets().get(1).getKey());
        assertEquals(1, histogram.getBuckets().get(1).getDocCount());

        response = client().prepareSearch("idx").addAggregation(histogram("my_histogram").field("long").interval(5).missing(3)).get();
        assertSearchResponse(response);
        histogram = response.getAggregations().get("my_histogram");
        assertEquals(1, histogram.getBuckets().size());
        assertEquals(0d, histogram.getBuckets().get(0).getKey());
        assertEquals(2, histogram.getBuckets().get(0).getDocCount());
    }

    public void testDateHistogram() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(dateHistogram("my_histogram").field("date").calendarInterval(DateHistogramInterval.YEAR).missing("2014-05-07"))
            .get();
        assertSearchResponse(response);
        Histogram histogram = response.getAggregations().get("my_histogram");
        assertEquals(2, histogram.getBuckets().size());
        assertEquals("2014-01-01T00:00:00.000Z", histogram.getBuckets().get(0).getKeyAsString());
        assertEquals(1, histogram.getBuckets().get(0).getDocCount());
        assertEquals("2015-01-01T00:00:00.000Z", histogram.getBuckets().get(1).getKeyAsString());
        assertEquals(1, histogram.getBuckets().get(1).getDocCount());

        response = client().prepareSearch("idx")
            .addAggregation(dateHistogram("my_histogram").field("date").calendarInterval(DateHistogramInterval.YEAR).missing("2015-05-07"))
            .get();
        assertSearchResponse(response);
        histogram = response.getAggregations().get("my_histogram");
        assertEquals(1, histogram.getBuckets().size());
        assertEquals("2015-01-01T00:00:00.000Z", histogram.getBuckets().get(0).getKeyAsString());
        assertEquals(2, histogram.getBuckets().get(0).getDocCount());
    }

    public void testCardinality() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(cardinality("card").field("long").missing(2)).get();
        assertSearchResponse(response);
        Cardinality cardinality = response.getAggregations().get("card");
        assertEquals(2, cardinality.getValue());
    }

    public void testPercentiles() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(percentiles("percentiles").field("long").missing(1000))
            .get();
        assertSearchResponse(response);
        Percentiles percentiles = response.getAggregations().get("percentiles");
        assertEquals(1000, percentiles.percentile(100), 0);
    }

    public void testStats() {
        SearchResponse response = client().prepareSearch("idx").addAggregation(stats("stats").field("long").missing(5)).get();
        assertSearchResponse(response);
        Stats stats = response.getAggregations().get("stats");
        assertEquals(2, stats.getCount());
        assertEquals(4, stats.getAvg(), 0);
    }

    public void testGeoCentroid() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(geoCentroid("centroid").field("location").missing("2,1"))
            .get();
        assertSearchResponse(response);
        GeoCentroid centroid = response.getAggregations().get("centroid");
        GeoPoint point = new GeoPoint(1.5, 1.5);
        assertThat(point.lat(), closeTo(centroid.centroid().lat(), 1E-5));
        assertThat(point.lon(), closeTo(centroid.centroid().lon(), 1E-5));
    }
}
