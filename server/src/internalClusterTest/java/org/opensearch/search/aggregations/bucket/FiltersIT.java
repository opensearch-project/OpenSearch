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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.filter.Filters;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.metrics.Avg;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedDynamicSettingsOpenSearchIntegTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.avg;
import static org.opensearch.search.aggregations.AggregationBuilders.filters;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class FiltersIT extends ParameterizedDynamicSettingsOpenSearchIntegTestCase {

    static int numDocs, numTag1Docs, numTag2Docs, numOtherDocs;

    public FiltersIT(Settings dynamicSettings) {
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
        numDocs = randomIntBetween(5, 20);
        numTag1Docs = randomIntBetween(1, numDocs - 1);
        numTag2Docs = randomIntBetween(1, numDocs - numTag1Docs);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numTag1Docs; i++) {
            XContentBuilder source = jsonBuilder().startObject().field("value", i + 1).field("tag", "tag1").endObject();
            builders.add(client().prepareIndex("idx").setId("" + i).setSource(source));
            if (randomBoolean()) {
                // randomly index the document twice so that we have deleted docs that match the filter
                builders.add(client().prepareIndex("idx").setId("" + i).setSource(source));
            }
        }
        for (int i = numTag1Docs; i < (numTag1Docs + numTag2Docs); i++) {
            XContentBuilder source = jsonBuilder().startObject()
                .field("value", i)
                .field("tag", "tag2")
                .field("name", "name" + i)
                .endObject();
            builders.add(client().prepareIndex("idx").setId("" + i).setSource(source));
            if (randomBoolean()) {
                builders.add(client().prepareIndex("idx").setId("" + i).setSource(source));
            }
        }
        for (int i = numTag1Docs + numTag2Docs; i < numDocs; i++) {
            numOtherDocs++;
            XContentBuilder source = jsonBuilder().startObject()
                .field("value", i)
                .field("tag", "tag3")
                .field("name", "name" + i)
                .endObject();
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
            .addAggregation(
                filters(
                    "tags",
                    randomOrder(new KeyedFilter("tag1", termQuery("tag", "tag1")), new KeyedFilter("tag2", termQuery("tag", "tag2")))
                )
            )
            .get();

        assertSearchResponse(response);

        Filters filters = response.getAggregations().get("tags");
        assertThat(filters, notNullValue());
        assertThat(filters.getName(), equalTo("tags"));

        assertThat(filters.getBuckets().size(), equalTo(2));

        Filters.Bucket bucket = filters.getBucketByKey("tag1");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

        bucket = filters.getBucketByKey("tag2");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));
    }

    // See NullPointer issue when filters are empty:
    // https://github.com/elastic/elasticsearch/issues/8438
    public void testEmptyFilterDeclarations() throws Exception {
        QueryBuilder emptyFilter = new BoolQueryBuilder();
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                filters("tags", randomOrder(new KeyedFilter("all", emptyFilter), new KeyedFilter("tag1", termQuery("tag", "tag1"))))
            )
            .get();

        assertSearchResponse(response);

        Filters filters = response.getAggregations().get("tags");
        assertThat(filters, notNullValue());
        Filters.Bucket allBucket = filters.getBucketByKey("all");
        assertThat(allBucket.getDocCount(), equalTo((long) numDocs));

        Filters.Bucket bucket = filters.getBucketByKey("tag1");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));
    }

    public void testWithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                filters(
                    "tags",
                    randomOrder(new KeyedFilter("tag1", termQuery("tag", "tag1")), new KeyedFilter("tag2", termQuery("tag", "tag2")))
                ).subAggregation(avg("avg_value").field("value"))
            )
            .get();

        assertSearchResponse(response);

        Filters filters = response.getAggregations().get("tags");
        assertThat(filters, notNullValue());
        assertThat(filters.getName(), equalTo("tags"));

        assertThat(filters.getBuckets().size(), equalTo(2));
        assertThat(((InternalAggregation) filters).getProperty("_bucket_count"), equalTo(2));
        Object[] propertiesKeys = (Object[]) ((InternalAggregation) filters).getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) ((InternalAggregation) filters).getProperty("_count");
        Object[] propertiesCounts = (Object[]) ((InternalAggregation) filters).getProperty("avg_value.value");

        Filters.Bucket bucket = filters.getBucketByKey("tag1");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));
        long sum = 0;
        for (int i = 0; i < numTag1Docs; ++i) {
            sum += i + 1;
        }
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        Avg avgValue = bucket.getAggregations().get("avg_value");
        assertThat(avgValue, notNullValue());
        assertThat(avgValue.getName(), equalTo("avg_value"));
        assertThat(avgValue.getValue(), equalTo((double) sum / numTag1Docs));
        assertThat((String) propertiesKeys[0], equalTo("tag1"));
        assertThat((long) propertiesDocCounts[0], equalTo((long) numTag1Docs));
        assertThat((double) propertiesCounts[0], equalTo((double) sum / numTag1Docs));

        bucket = filters.getBucketByKey("tag2");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));
        sum = 0;
        for (int i = numTag1Docs; i < (numTag1Docs + numTag2Docs); ++i) {
            sum += i;
        }
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        avgValue = bucket.getAggregations().get("avg_value");
        assertThat(avgValue, notNullValue());
        assertThat(avgValue.getName(), equalTo("avg_value"));
        assertThat(avgValue.getValue(), equalTo((double) sum / numTag2Docs));
        assertThat(propertiesKeys[1], equalTo("tag2"));
        assertThat(propertiesDocCounts[1], equalTo((long) numTag2Docs));
        assertThat(propertiesCounts[1], equalTo((double) sum / numTag2Docs));
    }

    public void testAsSubAggregation() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(histogram("histo").field("value").interval(2L).subAggregation(filters("filters", matchAllQuery())))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getBuckets().size(), greaterThanOrEqualTo(1));

        for (Histogram.Bucket bucket : histo.getBuckets()) {
            Filters filters = bucket.getAggregations().get("filters");
            assertThat(filters, notNullValue());
            assertThat(filters.getBuckets().size(), equalTo(1));
            Filters.Bucket filterBucket = filters.getBuckets().get(0);
            assertEquals(bucket.getDocCount(), filterBucket.getDocCount());
        }
    }

    public void testWithContextBasedSubAggregation() throws Exception {

        try {
            client().prepareSearch("idx")
                .addAggregation(
                    filters(
                        "tags",
                        randomOrder(new KeyedFilter("tag1", termQuery("tag", "tag1")), new KeyedFilter("tag2", termQuery("tag", "tag2")))
                    ).subAggregation(avg("avg_value"))
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
                    .subAggregation(filters("filters", new KeyedFilter("all", matchAllQuery())))
            )
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Filters filters = bucket.getAggregations().get("filters");
        assertThat(filters, notNullValue());
        Filters.Bucket all = filters.getBucketByKey("all");
        assertThat(all, Matchers.notNullValue());
        assertThat(all.getKeyAsString(), equalTo("all"));
        assertThat(all.getDocCount(), is(0L));
    }

    public void testSimpleNonKeyed() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(filters("tags", termQuery("tag", "tag1"), termQuery("tag", "tag2")))
            .get();

        assertSearchResponse(response);

        Filters filters = response.getAggregations().get("tags");
        assertThat(filters, notNullValue());
        assertThat(filters.getName(), equalTo("tags"));

        assertThat(filters.getBuckets().size(), equalTo(2));

        Collection<? extends Filters.Bucket> buckets = filters.getBuckets();
        Iterator<? extends Filters.Bucket> itr = buckets.iterator();

        Filters.Bucket bucket = itr.next();
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

        bucket = itr.next();
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));
    }

    public void testOtherBucket() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                filters(
                    "tags",
                    randomOrder(new KeyedFilter("tag1", termQuery("tag", "tag1")), new KeyedFilter("tag2", termQuery("tag", "tag2")))
                ).otherBucket(true)
            )
            .get();

        assertSearchResponse(response);

        Filters filters = response.getAggregations().get("tags");
        assertThat(filters, notNullValue());
        assertThat(filters.getName(), equalTo("tags"));

        assertThat(filters.getBuckets().size(), equalTo(3));

        Filters.Bucket bucket = filters.getBucketByKey("tag1");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

        bucket = filters.getBucketByKey("tag2");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));

        bucket = filters.getBucketByKey("_other_");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numOtherDocs));
    }

    public void testOtherNamedBucket() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                filters(
                    "tags",
                    randomOrder(new KeyedFilter("tag1", termQuery("tag", "tag1")), new KeyedFilter("tag2", termQuery("tag", "tag2")))
                ).otherBucket(true).otherBucketKey("foobar")
            )
            .get();

        assertSearchResponse(response);

        Filters filters = response.getAggregations().get("tags");
        assertThat(filters, notNullValue());
        assertThat(filters.getName(), equalTo("tags"));

        assertThat(filters.getBuckets().size(), equalTo(3));

        Filters.Bucket bucket = filters.getBucketByKey("tag1");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

        bucket = filters.getBucketByKey("tag2");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));

        bucket = filters.getBucketByKey("foobar");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numOtherDocs));
    }

    public void testOtherNonKeyed() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(filters("tags", termQuery("tag", "tag1"), termQuery("tag", "tag2")).otherBucket(true))
            .get();

        assertSearchResponse(response);

        Filters filters = response.getAggregations().get("tags");
        assertThat(filters, notNullValue());
        assertThat(filters.getName(), equalTo("tags"));

        assertThat(filters.getBuckets().size(), equalTo(3));

        Collection<? extends Filters.Bucket> buckets = filters.getBuckets();
        Iterator<? extends Filters.Bucket> itr = buckets.iterator();

        Filters.Bucket bucket = itr.next();
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));

        bucket = itr.next();
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));

        bucket = itr.next();
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numOtherDocs));
    }

    public void testOtherWithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                filters(
                    "tags",
                    randomOrder(new KeyedFilter("tag1", termQuery("tag", "tag1")), new KeyedFilter("tag2", termQuery("tag", "tag2")))
                ).otherBucket(true).subAggregation(avg("avg_value").field("value"))
            )
            .get();

        assertSearchResponse(response);

        Filters filters = response.getAggregations().get("tags");
        assertThat(filters, notNullValue());
        assertThat(filters.getName(), equalTo("tags"));

        assertThat(filters.getBuckets().size(), equalTo(3));
        assertThat(((InternalAggregation) filters).getProperty("_bucket_count"), equalTo(3));
        Object[] propertiesKeys = (Object[]) ((InternalAggregation) filters).getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) ((InternalAggregation) filters).getProperty("_count");
        Object[] propertiesCounts = (Object[]) ((InternalAggregation) filters).getProperty("avg_value.value");

        Filters.Bucket bucket = filters.getBucketByKey("tag1");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag1Docs));
        long sum = 0;
        for (int i = 0; i < numTag1Docs; ++i) {
            sum += i + 1;
        }
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        Avg avgValue = bucket.getAggregations().get("avg_value");
        assertThat(avgValue, notNullValue());
        assertThat(avgValue.getName(), equalTo("avg_value"));
        assertThat(avgValue.getValue(), equalTo((double) sum / numTag1Docs));
        assertThat(propertiesKeys[0], equalTo("tag1"));
        assertThat(propertiesDocCounts[0], equalTo((long) numTag1Docs));
        assertThat(propertiesCounts[0], equalTo((double) sum / numTag1Docs));

        bucket = filters.getBucketByKey("tag2");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numTag2Docs));
        sum = 0;
        for (int i = numTag1Docs; i < (numTag1Docs + numTag2Docs); ++i) {
            sum += i;
        }
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        avgValue = bucket.getAggregations().get("avg_value");
        assertThat(avgValue, notNullValue());
        assertThat(avgValue.getName(), equalTo("avg_value"));
        assertThat(avgValue.getValue(), equalTo((double) sum / numTag2Docs));
        assertThat(propertiesKeys[1], equalTo("tag2"));
        assertThat(propertiesDocCounts[1], equalTo((long) numTag2Docs));
        assertThat(propertiesCounts[1], equalTo((double) sum / numTag2Docs));

        bucket = filters.getBucketByKey("_other_");
        assertThat(bucket, Matchers.notNullValue());
        assertThat(bucket.getDocCount(), equalTo((long) numOtherDocs));
        sum = 0;
        for (int i = numTag1Docs + numTag2Docs; i < numDocs; ++i) {
            sum += i;
        }
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        avgValue = bucket.getAggregations().get("avg_value");
        assertThat(avgValue, notNullValue());
        assertThat(avgValue.getName(), equalTo("avg_value"));
        assertThat(avgValue.getValue(), equalTo((double) sum / numOtherDocs));
        assertThat(propertiesKeys[2], equalTo("_other_"));
        assertThat(propertiesDocCounts[2], equalTo((long) numOtherDocs));
        assertThat(propertiesCounts[2], equalTo((double) sum / numOtherDocs));
    }

    public void testEmptyAggregationWithOtherBucket() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                histogram("histo").field("value")
                    .interval(1L)
                    .minDocCount(0)
                    .subAggregation(filters("filters", new KeyedFilter("foo", matchAllQuery())).otherBucket(true).otherBucketKey("bar"))
            )
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Filters filters = bucket.getAggregations().get("filters");
        assertThat(filters, notNullValue());

        Filters.Bucket other = filters.getBucketByKey("bar");
        assertThat(other, Matchers.notNullValue());
        assertThat(other.getKeyAsString(), equalTo("bar"));
        assertThat(other.getDocCount(), is(0L));
    }

    private static KeyedFilter[] randomOrder(KeyedFilter... filters) {
        Collections.shuffle(Arrays.asList(filters), random());
        return filters;
    }
}
