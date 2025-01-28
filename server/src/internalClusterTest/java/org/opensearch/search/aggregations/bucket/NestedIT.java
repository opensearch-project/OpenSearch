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

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.nested.Nested;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.Stats;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.nestedQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.filter;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.nested;
import static org.opensearch.search.aggregations.AggregationBuilders.stats;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsNull.notNullValue;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class NestedIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static int numParents;
    private static int[] numChildren;
    private static SubAggCollectionMode aggCollectionMode;

    public NestedIT(Settings staticSettings) {
        super(staticSettings);
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

        assertAcked(prepareCreate("idx").setMapping("nested", "type=nested", "incorrect", "type=object"));
        ensureGreen("idx");

        List<IndexRequestBuilder> builders = new ArrayList<>();

        numParents = randomIntBetween(3, 10);
        numChildren = new int[numParents];
        aggCollectionMode = randomFrom(SubAggCollectionMode.values());
        logger.info("AGG COLLECTION MODE: {}", aggCollectionMode);
        int totalChildren = 0;
        for (int i = 0; i < numParents; ++i) {
            if (i == numParents - 1 && totalChildren == 0) {
                // we need at least one child overall
                numChildren[i] = randomIntBetween(1, 5);
            } else {
                numChildren[i] = randomInt(5);
            }
            totalChildren += numChildren[i];
        }
        assertTrue(totalChildren > 0);

        for (int i = 0; i < numParents; i++) {
            XContentBuilder source = jsonBuilder().startObject().field("value", i + 1).startArray("nested");
            for (int j = 0; j < numChildren[i]; ++j) {
                source = source.startObject().field("value", i + 1 + j).endObject();
            }
            source = source.endArray().endObject();
            builders.add(client().prepareIndex("idx").setId("" + i + 1).setSource(source));
        }

        prepareCreate("empty_bucket_idx").setMapping("value", "type=integer", "nested", "type=nested").get();
        ensureGreen("empty_bucket_idx");
        for (int i = 0; i < 2; i++) {
            builders.add(
                client().prepareIndex("empty_bucket_idx")
                    .setId("" + i)
                    .setSource(
                        jsonBuilder().startObject()
                            .field("value", i * 2)
                            .startArray("nested")
                            .startObject()
                            .field("value", i + 1)
                            .endObject()
                            .startObject()
                            .field("value", i + 2)
                            .endObject()
                            .startObject()
                            .field("value", i + 3)
                            .endObject()
                            .startObject()
                            .field("value", i + 4)
                            .endObject()
                            .startObject()
                            .field("value", i + 5)
                            .endObject()
                            .endArray()
                            .endObject()
                    )
            );
        }

        assertAcked(
            prepareCreate("idx_nested_nested_aggs").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("nested1")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("nested2")
                    .field("type", "nested")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen("idx_nested_nested_aggs");

        builders.add(
            client().prepareIndex("idx_nested_nested_aggs")
                .setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .startArray("nested1")
                        .startObject()
                        .field("a", "a")
                        .startArray("nested2")
                        .startObject()
                        .field("b", 2)
                        .endObject()
                        .endArray()
                        .endObject()
                        .startObject()
                        .field("a", "b")
                        .startArray("nested2")
                        .startObject()
                        .field("b", 2)
                        .endObject()
                        .endArray()
                        .endObject()
                        .endArray()
                        .endObject()
                )
        );
        indexRandom(true, builders);
        indexRandomForMultipleSlices("idx");
        ensureSearchable();
    }

    public void testSimple() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(nested("nested", "nested").subAggregation(stats("nested_value_stats").field("nested.value")))
            .get();

        assertSearchResponse(response);

        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        long sum = 0;
        long count = 0;
        for (int i = 0; i < numParents; ++i) {
            for (int j = 0; j < numChildren[i]; ++j) {
                final long value = i + 1 + j;
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
                ++count;
            }
        }

        Nested nested = response.getAggregations().get("nested");
        assertThat(nested, notNullValue());
        assertThat(nested.getName(), equalTo("nested"));
        assertThat(nested.getDocCount(), equalTo(count));
        assertThat(nested.getAggregations().asList().isEmpty(), is(false));

        Stats stats = nested.getAggregations().get("nested_value_stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getMin(), equalTo(min));
        assertThat(stats.getMax(), equalTo(max));
        assertThat(stats.getCount(), equalTo(count));
        assertThat(stats.getSum(), equalTo((double) sum));
        assertThat(stats.getAvg(), equalTo((double) sum / count));
    }

    public void testNonExistingNestedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
            .addAggregation(nested("nested", "value").subAggregation(stats("nested_value_stats").field("nested.value")))
            .get();

        Nested nested = searchResponse.getAggregations().get("nested");
        assertThat(nested, Matchers.notNullValue());
        assertThat(nested.getName(), equalTo("nested"));
        assertThat(nested.getDocCount(), is(0L));
    }

    public void testNestedWithSubTermsAgg() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                nested("nested", "nested").subAggregation(terms("values").field("nested.value").size(100).collectMode(aggCollectionMode))
            )
            .get();

        assertSearchResponse(response);

        long docCount = 0;
        long[] counts = new long[numParents + 6];
        for (int i = 0; i < numParents; ++i) {
            for (int j = 0; j < numChildren[i]; ++j) {
                final int value = i + 1 + j;
                ++counts[value];
                ++docCount;
            }
        }
        int uniqueValues = 0;
        for (long count : counts) {
            if (count > 0) {
                ++uniqueValues;
            }
        }

        Nested nested = response.getAggregations().get("nested");
        assertThat(nested, notNullValue());
        assertThat(nested.getName(), equalTo("nested"));
        assertThat(nested.getDocCount(), equalTo(docCount));
        assertThat(((InternalAggregation) nested).getProperty("_count"), equalTo(docCount));
        assertThat(nested.getAggregations().asList().isEmpty(), is(false));

        LongTerms values = nested.getAggregations().get("values");
        assertThat(values, notNullValue());
        assertThat(values.getName(), equalTo("values"));
        assertThat(values.getBuckets(), notNullValue());
        assertThat(values.getBuckets().size(), equalTo(uniqueValues));
        for (int i = 0; i < counts.length; ++i) {
            final String key = Long.toString(i);
            if (counts[i] == 0) {
                assertNull(values.getBucketByKey(key));
            } else {
                Bucket bucket = values.getBucketByKey(key);
                assertNotNull(bucket);
                assertEquals(counts[i], bucket.getDocCount());
            }
        }
        assertThat(((InternalAggregation) nested).getProperty("values"), sameInstance(values));
    }

    public void testNestedAsSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("top_values").field("value")
                    .size(100)
                    .collectMode(aggCollectionMode)
                    .subAggregation(nested("nested", "nested").subAggregation(max("max_value").field("nested.value")))
            )
            .get();

        assertSearchResponse(response);

        LongTerms values = response.getAggregations().get("top_values");
        assertThat(values, notNullValue());
        assertThat(values.getName(), equalTo("top_values"));
        assertThat(values.getBuckets(), notNullValue());
        assertThat(values.getBuckets().size(), equalTo(numParents));

        for (int i = 0; i < numParents; i++) {
            String topValue = "" + (i + 1);
            assertThat(values.getBucketByKey(topValue), notNullValue());
            Nested nested = values.getBucketByKey(topValue).getAggregations().get("nested");
            assertThat(nested, notNullValue());
            Max max = nested.getAggregations().get("max_value");
            assertThat(max, notNullValue());
            assertThat(max.getValue(), equalTo(numChildren[i] == 0 ? Double.NEGATIVE_INFINITY : (double) i + numChildren[i]));
        }
    }

    public void testNestNestedAggs() throws Exception {
        indexRandomForConcurrentSearch("idx_nested_nested_aggs");
        SearchResponse response = client().prepareSearch("idx_nested_nested_aggs")
            .addAggregation(
                nested("level1", "nested1").subAggregation(
                    terms("a").field("nested1.a.keyword")
                        .collectMode(aggCollectionMode)
                        .subAggregation(nested("level2", "nested1.nested2").subAggregation(sum("sum").field("nested1.nested2.b")))
                )
            )
            .get();
        assertSearchResponse(response);

        Nested level1 = response.getAggregations().get("level1");
        assertThat(level1, notNullValue());
        assertThat(level1.getName(), equalTo("level1"));
        assertThat(level1.getDocCount(), equalTo(2L));

        StringTerms a = level1.getAggregations().get("a");
        Terms.Bucket bBucket = a.getBucketByKey("a");
        assertThat(bBucket.getDocCount(), equalTo(1L));

        Nested level2 = bBucket.getAggregations().get("level2");
        assertThat(level2.getDocCount(), equalTo(1L));
        Sum sum = level2.getAggregations().get("sum");
        assertThat(sum.getValue(), equalTo(2d));

        a = level1.getAggregations().get("a");
        bBucket = a.getBucketByKey("b");
        assertThat(bBucket.getDocCount(), equalTo(1L));

        level2 = bBucket.getAggregations().get("level2");
        assertThat(level2.getDocCount(), equalTo(1L));
        sum = level2.getAggregations().get("sum");
        assertThat(sum.getValue(), equalTo(2d));
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
            .setQuery(matchAllQuery())
            .addAggregation(histogram("histo").field("value").interval(1L).minDocCount(0).subAggregation(nested("nested", "nested")))
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Nested nested = bucket.getAggregations().get("nested");
        assertThat(nested, Matchers.notNullValue());
        assertThat(nested.getName(), equalTo("nested"));
        assertThat(nested.getDocCount(), is(0L));
    }

    public void testNestedOnObjectField() throws Exception {
        try {
            client().prepareSearch("idx").setQuery(matchAllQuery()).addAggregation(nested("object_field", "incorrect")).get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString(), containsString("[nested] nested path [incorrect] is not nested"));
        }
    }

    // Test based on: https://github.com/elastic/elasticsearch/issues/9280
    public void testParentFilterResolvedCorrectly() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("comments")
            .field("type", "nested")
            .startObject("properties")
            .startObject("cid")
            .field("type", "long")
            .endObject()
            .startObject("identifier")
            .field("type", "keyword")
            .endObject()
            .startObject("tags")
            .field("type", "nested")
            .startObject("properties")
            .startObject("tid")
            .field("type", "long")
            .endObject()
            .startObject("name")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .startObject("dates")
            .field("type", "object")
            .startObject("properties")
            .startObject("day")
            .field("type", "date")
            .field("format", "date_optional_time")
            .endObject()
            .startObject("month")
            .field("type", "object")
            .startObject("properties")
            .startObject("end")
            .field("type", "date")
            .field("format", "date_optional_time")
            .endObject()
            .startObject("start")
            .field("type", "date")
            .field("format", "date_optional_time")
            .endObject()
            .startObject("label")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            prepareCreate("idx2").setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping(mapping)
        );
        ensureGreen("idx2");

        List<IndexRequestBuilder> indexRequests = new ArrayList<>(2);
        indexRequests.add(
            client().prepareIndex("idx2")
                .setId("1")
                .setSource(
                    "{\"dates\": {\"month\": {\"label\": \"2014-11\", \"end\": \"2014-11-30\", \"start\": \"2014-11-01\"}, "
                        + "\"day\": \"2014-11-30\"}, \"comments\": [{\"cid\": 3,\"identifier\": \"29111\"}, {\"cid\": 4,\"tags\": ["
                        + "{\"tid\" :44,\"name\": \"Roles\"}], \"identifier\": \"29101\"}]}",
                    MediaTypeRegistry.JSON
                )
        );
        indexRequests.add(
            client().prepareIndex("idx2")
                .setId("2")
                .setSource(
                    "{\"dates\": {\"month\": {\"label\": \"2014-12\", \"end\": \"2014-12-31\", \"start\": \"2014-12-01\"}, "
                        + "\"day\": \"2014-12-03\"}, \"comments\": [{\"cid\": 1, \"identifier\": \"29111\"}, {\"cid\": 2,\"tags\": ["
                        + "{\"tid\" : 22, \"name\": \"DataChannels\"}], \"identifier\": \"29101\"}]}",
                    MediaTypeRegistry.JSON
                )
        );
        indexRandom(true, indexRequests);

        SearchResponse response = client().prepareSearch("idx2")
            .addAggregation(
                terms("startDate").field("dates.month.start")
                    .subAggregation(
                        terms("endDate").field("dates.month.end")
                            .subAggregation(
                                terms("period").field("dates.month.label")
                                    .subAggregation(
                                        nested("ctxt_idfier_nested", "comments").subAggregation(
                                            filter("comment_filter", termQuery("comments.identifier", "29111")).subAggregation(
                                                nested("nested_tags", "comments.tags").subAggregation(
                                                    terms("tag").field("comments.tags.name")
                                                )
                                            )
                                        )
                                    )
                            )
                    )
            )
            .get();
        assertNoFailures(response);
        assertHitCount(response, 2);

        Terms startDate = response.getAggregations().get("startDate");
        assertThat(startDate.getBuckets().size(), equalTo(2));
        Terms.Bucket bucket = startDate.getBucketByKey("2014-11-01T00:00:00.000Z");
        assertThat(bucket.getDocCount(), equalTo(1L));
        Terms endDate = bucket.getAggregations().get("endDate");
        bucket = endDate.getBucketByKey("2014-11-30T00:00:00.000Z");
        assertThat(bucket.getDocCount(), equalTo(1L));
        Terms period = bucket.getAggregations().get("period");
        bucket = period.getBucketByKey("2014-11");
        assertThat(bucket.getDocCount(), equalTo(1L));
        Nested comments = bucket.getAggregations().get("ctxt_idfier_nested");
        assertThat(comments.getDocCount(), equalTo(2L));
        Filter filter = comments.getAggregations().get("comment_filter");
        assertThat(filter.getDocCount(), equalTo(1L));
        Nested nestedTags = filter.getAggregations().get("nested_tags");
        assertThat(nestedTags.getDocCount(), equalTo(0L)); // This must be 0
        Terms tags = nestedTags.getAggregations().get("tag");
        assertThat(tags.getBuckets().size(), equalTo(0)); // and this must be empty

        bucket = startDate.getBucketByKey("2014-12-01T00:00:00.000Z");
        assertThat(bucket.getDocCount(), equalTo(1L));
        endDate = bucket.getAggregations().get("endDate");
        bucket = endDate.getBucketByKey("2014-12-31T00:00:00.000Z");
        assertThat(bucket.getDocCount(), equalTo(1L));
        period = bucket.getAggregations().get("period");
        bucket = period.getBucketByKey("2014-12");
        assertThat(bucket.getDocCount(), equalTo(1L));
        comments = bucket.getAggregations().get("ctxt_idfier_nested");
        assertThat(comments.getDocCount(), equalTo(2L));
        filter = comments.getAggregations().get("comment_filter");
        assertThat(filter.getDocCount(), equalTo(1L));
        nestedTags = filter.getAggregations().get("nested_tags");
        assertThat(nestedTags.getDocCount(), equalTo(0L)); // This must be 0
        tags = nestedTags.getAggregations().get("tag");
        assertThat(tags.getBuckets().size(), equalTo(0)); // and this must be empty
        internalCluster().wipeIndices("idx2");
    }

    public void testNestedSameDocIdProcessedMultipleTime() throws Exception {
        assertAcked(
            prepareCreate("idx4").setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping("categories", "type=keyword", "name", "type=text", "property", "type=nested")
        );
        ensureGreen("idx4");

        client().prepareIndex("idx4")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "product1")
                    .array("categories", "1", "2", "3", "4")
                    .startArray("property")
                    .startObject()
                    .field("id", 1)
                    .endObject()
                    .startObject()
                    .field("id", 2)
                    .endObject()
                    .startObject()
                    .field("id", 3)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        client().prepareIndex("idx4")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "product2")
                    .array("categories", "1", "2")
                    .startArray("property")
                    .startObject()
                    .field("id", 1)
                    .endObject()
                    .startObject()
                    .field("id", 5)
                    .endObject()
                    .startObject()
                    .field("id", 4)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        refresh();
        indexRandomForConcurrentSearch("idx4");

        SearchResponse response = client().prepareSearch("idx4")
            .addAggregation(
                terms("category").field("categories")
                    .subAggregation(nested("property", "property").subAggregation(terms("property_id").field("property.id")))
            )
            .get();
        assertNoFailures(response);
        assertHitCount(response, 2);

        Terms category = response.getAggregations().get("category");
        assertThat(category.getBuckets().size(), equalTo(4));

        Terms.Bucket bucket = category.getBucketByKey("1");
        assertThat(bucket.getDocCount(), equalTo(2L));
        Nested property = bucket.getAggregations().get("property");
        assertThat(property.getDocCount(), equalTo(6L));
        Terms propertyId = property.getAggregations().get("property_id");
        assertThat(propertyId.getBuckets().size(), equalTo(5));
        assertThat(propertyId.getBucketByKey("1").getDocCount(), equalTo(2L));
        assertThat(propertyId.getBucketByKey("2").getDocCount(), equalTo(1L));
        assertThat(propertyId.getBucketByKey("3").getDocCount(), equalTo(1L));
        assertThat(propertyId.getBucketByKey("4").getDocCount(), equalTo(1L));
        assertThat(propertyId.getBucketByKey("5").getDocCount(), equalTo(1L));

        bucket = category.getBucketByKey("2");
        assertThat(bucket.getDocCount(), equalTo(2L));
        property = bucket.getAggregations().get("property");
        assertThat(property.getDocCount(), equalTo(6L));
        propertyId = property.getAggregations().get("property_id");
        assertThat(propertyId.getBuckets().size(), equalTo(5));
        assertThat(propertyId.getBucketByKey("1").getDocCount(), equalTo(2L));
        assertThat(propertyId.getBucketByKey("2").getDocCount(), equalTo(1L));
        assertThat(propertyId.getBucketByKey("3").getDocCount(), equalTo(1L));
        assertThat(propertyId.getBucketByKey("4").getDocCount(), equalTo(1L));
        assertThat(propertyId.getBucketByKey("5").getDocCount(), equalTo(1L));

        bucket = category.getBucketByKey("3");
        assertThat(bucket.getDocCount(), equalTo(1L));
        property = bucket.getAggregations().get("property");
        assertThat(property.getDocCount(), equalTo(3L));
        propertyId = property.getAggregations().get("property_id");
        assertThat(propertyId.getBuckets().size(), equalTo(3));
        assertThat(propertyId.getBucketByKey("1").getDocCount(), equalTo(1L));
        assertThat(propertyId.getBucketByKey("2").getDocCount(), equalTo(1L));
        assertThat(propertyId.getBucketByKey("3").getDocCount(), equalTo(1L));

        bucket = category.getBucketByKey("4");
        assertThat(bucket.getDocCount(), equalTo(1L));
        property = bucket.getAggregations().get("property");
        assertThat(property.getDocCount(), equalTo(3L));
        propertyId = property.getAggregations().get("property_id");
        assertThat(propertyId.getBuckets().size(), equalTo(3));
        assertThat(propertyId.getBucketByKey("1").getDocCount(), equalTo(1L));
        assertThat(propertyId.getBucketByKey("2").getDocCount(), equalTo(1L));
        assertThat(propertyId.getBucketByKey("3").getDocCount(), equalTo(1L));
        internalCluster().wipeIndices("idx4");
    }

    public void testFilterAggInsideNestedAgg() throws Exception {
        assertAcked(
            prepareCreate("classes").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("name")
                    .field("type", "text")
                    .endObject()
                    .startObject("methods")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("name")
                    .field("type", "text")
                    .endObject()
                    .startObject("return_type")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("parameters")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("name")
                    .field("type", "text")
                    .endObject()
                    .startObject("type")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        client().prepareIndex("classes")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "QueryBuilder")
                    .startArray("methods")
                    .startObject()
                    .field("name", "toQuery")
                    .field("return_type", "Query")
                    .startArray("parameters")
                    .startObject()
                    .field("name", "context")
                    .field("type", "QueryShardContext")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("name", "queryName")
                    .field("return_type", "QueryBuilder")
                    .startArray("parameters")
                    .startObject()
                    .field("name", "queryName")
                    .field("type", "String")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("name", "boost")
                    .field("return_type", "QueryBuilder")
                    .startArray("parameters")
                    .startObject()
                    .field("name", "boost")
                    .field("type", "float")
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        client().prepareIndex("classes")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "Document")
                    .startArray("methods")
                    .startObject()
                    .field("name", "add")
                    .field("return_type", "void")
                    .startArray("parameters")
                    .startObject()
                    .field("name", "field")
                    .field("type", "IndexableField")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("name", "removeField")
                    .field("return_type", "void")
                    .startArray("parameters")
                    .startObject()
                    .field("name", "name")
                    .field("type", "String")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("name", "removeFields")
                    .field("return_type", "void")
                    .startArray("parameters")
                    .startObject()
                    .field("name", "name")
                    .field("type", "String")
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        refresh();
        indexRandomForConcurrentSearch("classes");

        SearchResponse response = client().prepareSearch("classes")
            .addAggregation(
                nested("to_method", "methods").subAggregation(
                    filter(
                        "num_string_params",
                        nestedQuery("methods.parameters", termQuery("methods.parameters.type", "String"), ScoreMode.None)
                    )
                )
            )
            .get();
        Nested toMethods = response.getAggregations().get("to_method");
        Filter numStringParams = toMethods.getAggregations().get("num_string_params");
        assertThat(numStringParams.getDocCount(), equalTo(3L));

        response = client().prepareSearch("classes")
            .addAggregation(
                nested("to_method", "methods").subAggregation(
                    terms("return_type").field("methods.return_type")
                        .subAggregation(
                            filter(
                                "num_string_params",
                                nestedQuery("methods.parameters", termQuery("methods.parameters.type", "String"), ScoreMode.None)
                            )
                        )
                )
            )
            .get();
        toMethods = response.getAggregations().get("to_method");
        Terms terms = toMethods.getAggregations().get("return_type");
        Bucket bucket = terms.getBucketByKey("void");
        assertThat(bucket.getDocCount(), equalTo(3L));
        numStringParams = bucket.getAggregations().get("num_string_params");
        assertThat(numStringParams.getDocCount(), equalTo(2L));

        bucket = terms.getBucketByKey("QueryBuilder");
        assertThat(bucket.getDocCount(), equalTo(2L));
        numStringParams = bucket.getAggregations().get("num_string_params");
        assertThat(numStringParams.getDocCount(), equalTo(1L));

        bucket = terms.getBucketByKey("Query");
        assertThat(bucket.getDocCount(), equalTo(1L));
        numStringParams = bucket.getAggregations().get("num_string_params");
        assertThat(numStringParams.getDocCount(), equalTo(0L));
        internalCluster().wipeIndices("classes");
    }

    public void testExtractInnerHitBuildersWithDuplicateHitName() throws Exception {
        assertAcked(
            prepareCreate("idxduplicatehitnames").setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping("categories", "type=keyword", "name", "type=text", "property", "type=nested")
        );
        ensureGreen("idxduplicatehitnames");

        SearchRequestBuilder searchRequestBuilder = client().prepareSearch("idxduplicatehitnames")
            .setQuery(
                boolQuery().should(
                    nestedQuery("property", termQuery("property.id", 1D), ScoreMode.None).innerHit(new InnerHitBuilder("ih1"))
                )
                    .should(nestedQuery("property", termQuery("property.id", 1D), ScoreMode.None).innerHit(new InnerHitBuilder("ih2")))
                    .should(nestedQuery("property", termQuery("property.id", 1D), ScoreMode.None).innerHit(new InnerHitBuilder("ih1")))
            );

        assertFailures(
            searchRequestBuilder,
            RestStatus.BAD_REQUEST,
            containsString("[inner_hits] already contains an entry for key [ih1]")
        );
        internalCluster().wipeIndices("idxduplicatehitnames");
    }

    public void testExtractInnerHitBuildersWithDuplicatePath() throws Exception {
        assertAcked(
            prepareCreate("idxnullhitnames").setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping("categories", "type=keyword", "name", "type=text", "property", "type=nested")
        );
        ensureGreen("idxnullhitnames");

        SearchRequestBuilder searchRequestBuilder = client().prepareSearch("idxnullhitnames")
            .setQuery(
                boolQuery().should(nestedQuery("property", termQuery("property.id", 1D), ScoreMode.None).innerHit(new InnerHitBuilder()))
                    .should(nestedQuery("property", termQuery("property.id", 1D), ScoreMode.None).innerHit(new InnerHitBuilder()))
                    .should(nestedQuery("property", termQuery("property.id", 1D), ScoreMode.None).innerHit(new InnerHitBuilder()))
            );

        assertFailures(
            searchRequestBuilder,
            RestStatus.BAD_REQUEST,
            containsString("[inner_hits] already contains an entry for key [property]")
        );
        internalCluster().wipeIndices("idxnullhitnames");
    }
}
