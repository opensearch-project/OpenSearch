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

package org.opensearch.search.aggregations.bucket;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.OpenSearchException;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.adjacency.AdjacencyMatrix;
import org.opensearch.search.aggregations.bucket.adjacency.AdjacencyMatrix.Bucket;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.metrics.Avg;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedDynamicSettingsOpenSearchIntegTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.adjacencyMatrix;
import static org.opensearch.search.aggregations.AggregationBuilders.avg;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class AdjacencyMatrixIT extends ParameterizedDynamicSettingsOpenSearchIntegTestCase {

    static int numDocs, numSingleTag1Docs, numSingleTag2Docs, numTag1Docs, numTag2Docs, numMultiTagDocs;
    static final int MAX_NUM_FILTERS = 3;

    public AdjacencyMatrixIT(Settings dynamicSettings) {
        super(dynamicSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx2");
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("idx")
                .setSettings(Settings.builder().put(IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.getKey(), MAX_NUM_FILTERS))
                .get()
        );

        numDocs = randomIntBetween(5, 20);
        numTag1Docs = randomIntBetween(1, numDocs - 1);
        numTag2Docs = randomIntBetween(1, numDocs - numTag1Docs);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numTag1Docs; i++) {
            numSingleTag1Docs++;
            XContentBuilder source = jsonBuilder().startObject().field("value", i + 1).field("tag", "tag1").endObject();
            builders.add(client().prepareIndex("idx").setId("" + i).setSource(source));
            if (randomBoolean()) {
                // randomly index the document twice so that we have deleted
                // docs that match the filter
                builders.add(client().prepareIndex("idx").setId("" + i).setSource(source));
            }
        }
        for (int i = numTag1Docs; i < (numTag1Docs + numTag2Docs); i++) {
            numSingleTag2Docs++;
            XContentBuilder source = jsonBuilder().startObject().field("value", i + 1).field("tag", "tag2").endObject();
            builders.add(client().prepareIndex("idx").setId("" + i).setSource(source));
            if (randomBoolean()) {
                builders.add(client().prepareIndex("idx").setId("" + i).setSource(source));
            }
        }
        for (int i = numTag1Docs + numTag2Docs; i < numDocs; i++) {
            numMultiTagDocs++;
            numTag1Docs++;
            numTag2Docs++;
            XContentBuilder source = jsonBuilder().startObject().field("value", i + 1).array("tag", "tag1", "tag2").endObject();
            builders.add(client().prepareIndex("idx").setId("" + i).setSource(source));
            if (randomBoolean()) {
                builders.add(client().prepareIndex("idx").setId("" + i).setSource(source));
            }
        }
        prepareCreate("empty_bucket_idx").setMapping("value", "type=integer").get();
        for (int i = 0; i < 2; i++) {
            builders.add(
                client().prepareIndex("empty_bucket_idx")
                    .setId("" + i)
                    .setSource(jsonBuilder().startObject().field("value", i * 2).endObject())
            );
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testSimple() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(adjacencyMatrix("tags", newMap("tag1", termQuery("tag", "tag1")).add("tag2", termQuery("tag", "tag2"))))
            .get();

        assertSearchResponse(response);

        AdjacencyMatrix matrix = response.getAggregations().get("tags");
        assertThat(matrix, notNullValue());
        assertThat(matrix.getName(), equalTo("tags"));

        int expected = numMultiTagDocs > 0 ? 3 : 2;
        assertThat(matrix.getBuckets().size(), equalTo(expected));

        AdjacencyMatrix.Bucket bucket = matrix.getBucketByKey("tag1");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

        bucket = matrix.getBucketByKey("tag2");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));

        bucket = matrix.getBucketByKey("tag1&tag2");
        if (numMultiTagDocs == 0) {
            assertThat(bucket, Matchers.nullValue());
        } else {
            assertThat(bucket, Matchers.notNullValue());
            assertThat(bucket.getDocCount(), equalTo((long) numMultiTagDocs));
        }

    }

    public void testCustomSeparator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(adjacencyMatrix("tags", "\t", newMap("tag1", termQuery("tag", "tag1")).add("tag2", termQuery("tag", "tag2"))))
            .get();

        assertSearchResponse(response);

        AdjacencyMatrix matrix = response.getAggregations().get("tags");
        assertThat(matrix, notNullValue());

        AdjacencyMatrix.Bucket bucket = matrix.getBucketByKey("tag1\ttag2");
        if (numMultiTagDocs == 0) {
            assertThat(bucket, Matchers.nullValue());
        } else {
            assertThat(bucket, Matchers.notNullValue());
            assertThat(bucket.getDocCount(), equalTo((long) numMultiTagDocs));
        }

    }

    // See NullPointer issue when filters are empty:
    // https://github.com/elastic/elasticsearch/issues/8438
    public void testEmptyFilterDeclarations() throws Exception {
        QueryBuilder emptyFilter = new BoolQueryBuilder();
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(adjacencyMatrix("tags", newMap("all", emptyFilter).add("tag1", termQuery("tag", "tag1"))))
            .get();

        assertSearchResponse(response);

        AdjacencyMatrix filters = response.getAggregations().get("tags");
        assertThat(filters, notNullValue());
        AdjacencyMatrix.Bucket allBucket = filters.getBucketByKey("all");
        assertThat(allBucket.getDocCount(), equalTo((long) numDocs));

        AdjacencyMatrix.Bucket bucket = filters.getBucketByKey("tag1");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));
    }

    public void testWithSubAggregation() throws Exception {
        BoolQueryBuilder boolQ = new BoolQueryBuilder();
        boolQ.must(termQuery("tag", "tag1"));
        boolQ.must(termQuery("tag", "tag2"));
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                adjacencyMatrix("tags", newMap("tag1", termQuery("tag", "tag1")).add("tag2", termQuery("tag", "tag2")).add("both", boolQ))
                    .subAggregation(avg("avg_value").field("value"))
            )
            .get();

        assertSearchResponse(response);

        AdjacencyMatrix matrix = response.getAggregations().get("tags");
        assertThat(matrix, notNullValue());
        assertThat(matrix.getName(), equalTo("tags"));

        int expectedBuckets = 0;
        if (numTag1Docs > 0) {
            expectedBuckets++;
        }
        if (numTag2Docs > 0) {
            expectedBuckets++;
        }
        if (numMultiTagDocs > 0) {
            // both, both&tag1, both&tag2, tag1&tag2
            expectedBuckets += 4;
        }

        assertThat(matrix.getBuckets().size(), equalTo(expectedBuckets));
        assertThat(((InternalAggregation) matrix).getProperty("_bucket_count"), equalTo(expectedBuckets));

        Object[] propertiesKeys = (Object[]) ((InternalAggregation) matrix).getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) ((InternalAggregation) matrix).getProperty("_count");
        Object[] propertiesCounts = (Object[]) ((InternalAggregation) matrix).getProperty("avg_value.value");

        assertEquals(expectedBuckets, propertiesKeys.length);
        assertEquals(propertiesKeys.length, propertiesDocCounts.length);
        assertEquals(propertiesKeys.length, propertiesCounts.length);

        for (int i = 0; i < propertiesCounts.length; i++) {
            AdjacencyMatrix.Bucket bucket = matrix.getBucketByKey(propertiesKeys[i].toString());
            assertThat(bucket, Matchers.notNullValue());
            Avg avgValue = bucket.getAggregations().get("avg_value");
            assertThat(avgValue, notNullValue());
            assertThat((long) propertiesDocCounts[i], equalTo(bucket.getDocCount()));
            assertThat((double) propertiesCounts[i], equalTo(avgValue.getValue()));
        }

        AdjacencyMatrix.Bucket tag1Bucket = matrix.getBucketByKey("tag1");
        assertThat(tag1Bucket, Matchers.notNullValue());
        assertThat(tag1Bucket.getDocCount(), equalTo((long) numTag1Docs));
        long sum = 0;
        for (int i = 0; i < numSingleTag1Docs; i++) {
            sum += i + 1;
        }
        for (int i = numSingleTag1Docs + numSingleTag2Docs; i < numDocs; i++) {
            sum += i + 1;
        }
        assertThat(tag1Bucket.getAggregations().asList().isEmpty(), is(false));
        Avg avgBucket1Value = tag1Bucket.getAggregations().get("avg_value");
        assertThat(avgBucket1Value, notNullValue());
        assertThat(avgBucket1Value.getName(), equalTo("avg_value"));
        assertThat(avgBucket1Value.getValue(), equalTo((double) sum / numTag1Docs));

        Bucket tag2Bucket = matrix.getBucketByKey("tag2");
        assertThat(tag2Bucket, Matchers.notNullValue());
        assertThat(tag2Bucket.getDocCount(), equalTo((long) numTag2Docs));
        sum = 0;
        for (int i = numSingleTag1Docs; i < numDocs; i++) {
            sum += i + 1;
        }
        assertThat(tag2Bucket.getAggregations().asList().isEmpty(), is(false));
        Avg avgBucket2Value = tag2Bucket.getAggregations().get("avg_value");
        assertThat(avgBucket2Value, notNullValue());
        assertThat(avgBucket2Value.getName(), equalTo("avg_value"));
        assertThat(avgBucket2Value.getValue(), equalTo((double) sum / numTag2Docs));

        // Check intersection buckets are computed correctly by comparing with
        // ANDed query bucket results
        Bucket bucketBothQ = matrix.getBucketByKey("both");
        if (numMultiTagDocs == 0) {
            // Empty intersections are not returned.
            assertThat(bucketBothQ, Matchers.nullValue());
            Bucket bucketIntersectQ = matrix.getBucketByKey("tag1&tag2");
            assertThat(bucketIntersectQ, Matchers.nullValue());
            Bucket tag1Both = matrix.getBucketByKey("both&tag1");
            assertThat(tag1Both, Matchers.nullValue());
        } else {
            assertThat(bucketBothQ, Matchers.notNullValue());
            assertThat(bucketBothQ.getDocCount(), equalTo((long) numMultiTagDocs));
            Avg avgValueBothQ = bucketBothQ.getAggregations().get("avg_value");

            Bucket bucketIntersectQ = matrix.getBucketByKey("tag1&tag2");
            assertThat(bucketIntersectQ, Matchers.notNullValue());
            assertThat(bucketIntersectQ.getDocCount(), equalTo((long) numMultiTagDocs));
            Avg avgValueIntersectQ = bucketBothQ.getAggregations().get("avg_value");
            assertThat(avgValueIntersectQ.getValue(), equalTo(avgValueBothQ.getValue()));

            Bucket tag1Both = matrix.getBucketByKey("both&tag1");
            assertThat(tag1Both, Matchers.notNullValue());
            assertThat(tag1Both.getDocCount(), equalTo((long) numMultiTagDocs));
            Avg avgValueTag1BothIntersectQ = tag1Both.getAggregations().get("avg_value");
            assertThat(avgValueTag1BothIntersectQ.getValue(), equalTo(avgValueBothQ.getValue()));
        }

    }

    public void testTooLargeMatrix() throws Exception {

        // Create more filters than is permitted by index settings.
        MapBuilder filtersMap = new MapBuilder();
        for (int i = 0; i <= MAX_NUM_FILTERS; i++) {
            filtersMap.add("tag" + i, termQuery("tag", "tag" + i));
        }

        try {
            client().prepareSearch("idx").addAggregation(adjacencyMatrix("tags", "\t", filtersMap)).get();
            fail("SearchPhaseExecutionException should have been thrown");
        } catch (SearchPhaseExecutionException ex) {
            assertThat(ex.getCause().getMessage(), containsString("Number of filters is too large"));
        }
    }

    public void testAsSubAggregation() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field("value").interval(2L).subAggregation(adjacencyMatrix("matrix", newMap("all", matchAllQuery())))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getBuckets().size(), greaterThanOrEqualTo(1));

        for (Histogram.Bucket bucket : histo.getBuckets()) {
            AdjacencyMatrix matrix = bucket.getAggregations().get("matrix");
            assertThat(matrix, notNullValue());
            assertThat(matrix.getBuckets().size(), equalTo(1));
            AdjacencyMatrix.Bucket filterBucket = matrix.getBuckets().get(0);
            assertEquals(bucket.getDocCount(), filterBucket.getDocCount());
        }
    }

    public void testWithContextBasedSubAggregation() throws Exception {

        try {
            client().prepareSearch("idx")
                .addAggregation(
                    adjacencyMatrix("tags", newMap("tag1", termQuery("tag", "tag1")).add("tag2", termQuery("tag", "tag2"))).subAggregation(
                        avg("avg_value")
                    )
                )
                .get();

            fail(
                "expected execution to fail - an attempt to have a context based numeric sub-aggregation, but there is not value source"
                    + "context which the sub-aggregation can inherit"
            );

        } catch (OpenSearchException e) {
            assertThat(e.getMessage(), is("all shards failed"));
        }
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                histogram("histo").field("value")
                    .interval(1L)
                    .minDocCount(0)
                    .subAggregation(adjacencyMatrix("matrix", newMap("all", matchAllQuery())))
            )
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        AdjacencyMatrix matrix = bucket.getAggregations().get("matrix");
        assertThat(matrix, notNullValue());
        AdjacencyMatrix.Bucket all = matrix.getBucketByKey("all");
        assertThat(all, Matchers.nullValue());
    }

    // Helper methods for building maps of QueryBuilders
    static MapBuilder newMap(String name, QueryBuilder builder) {
        return new MapBuilder().add(name, builder);
    }

    static class MapBuilder extends HashMap<String, QueryBuilder> {
        public MapBuilder add(String name, QueryBuilder builder) {
            put(name, builder);
            return this;
        }
    }

}
