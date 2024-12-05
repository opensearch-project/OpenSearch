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

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.range.Range;
import org.opensearch.search.aggregations.bucket.range.Range.Bucket;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregatorFactory;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE;
import static org.opensearch.search.aggregations.AggregationBuilders.extendedStats;
import static org.opensearch.search.aggregations.AggregationBuilders.filter;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.min;
import static org.opensearch.search.aggregations.AggregationBuilders.percentiles;
import static org.opensearch.search.aggregations.AggregationBuilders.range;
import static org.opensearch.search.aggregations.AggregationBuilders.stats;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAllSuccessful;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 * Additional tests that aim at testing more complex aggregation trees on larger random datasets, so that things like
 * the growth of dynamic arrays is tested.
 */
public class EquivalenceIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public EquivalenceIT(Settings staticSettings) {
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
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("floor(_value / interval)", vars -> {
                Double value = (Double) vars.get("_value");
                Integer interval = (Integer) vars.get("interval");
                return Math.floor(value / interval.doubleValue());
            });
        }
    }

    @Before
    private void setupMaxBuckets() {
        // disables the max bucket limit for this test
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Collections.singletonMap("search.max_buckets", Integer.MAX_VALUE))
            .get();
    }

    @After
    private void cleanupMaxBuckets() {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Collections.singletonMap("search.max_buckets", null)).get();
    }

    // Make sure that unordered, reversed, disjoint and/or overlapping ranges are supported
    // Duel with filters
    public void testRandomRanges() throws Exception {
        final int numDocs = scaledRandomIntBetween(500, 5000);
        final double[][] docs = new double[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            final int numValues = randomInt(5);
            docs[i] = new double[numValues];
            for (int j = 0; j < numValues; ++j) {
                docs[i][j] = randomDouble() * 100;
            }
        }

        prepareCreate("idx").setMapping(
            jsonBuilder().startObject()
                .startObject("properties")
                .startObject("values")
                .field("type", "double")
                .endObject()
                .endObject()
                .endObject()
        ).get();

        for (int i = 0; i < docs.length; ++i) {
            XContentBuilder source = jsonBuilder().startObject().startArray("values");
            for (int j = 0; j < docs[i].length; ++j) {
                source = source.value(docs[i][j]);
            }
            source = source.endArray().endObject();
            client().prepareIndex("idx").setSource(source).get();
        }
        assertNoFailures(client().admin().indices().prepareRefresh("idx").setIndicesOptions(IndicesOptions.lenientExpandOpen()).get());

        final int numRanges = randomIntBetween(1, 20);
        final double[][] ranges = new double[numRanges][];
        for (int i = 0; i < ranges.length; ++i) {
            switch (randomInt(2)) {
                case 0:
                    ranges[i] = new double[] { Double.NEGATIVE_INFINITY, randomInt(100) };
                    break;
                case 1:
                    ranges[i] = new double[] { randomInt(100), Double.POSITIVE_INFINITY };
                    break;
                case 2:
                    ranges[i] = new double[] { randomInt(100), randomInt(100) };
                    break;
                default:
                    throw new AssertionError();
            }
        }

        RangeAggregationBuilder query = range("range").field("values");
        for (int i = 0; i < ranges.length; ++i) {
            String key = Integer.toString(i);
            if (ranges[i][0] == Double.NEGATIVE_INFINITY) {
                query.addUnboundedTo(key, ranges[i][1]);
            } else if (ranges[i][1] == Double.POSITIVE_INFINITY) {
                query.addUnboundedFrom(key, ranges[i][0]);
            } else {
                query.addRange(key, ranges[i][0], ranges[i][1]);
            }
        }

        SearchRequestBuilder reqBuilder = client().prepareSearch("idx").addAggregation(query);
        for (int i = 0; i < ranges.length; ++i) {
            RangeQueryBuilder filter = QueryBuilders.rangeQuery("values");
            if (ranges[i][0] != Double.NEGATIVE_INFINITY) {
                filter = filter.from(ranges[i][0]);
            }
            if (ranges[i][1] != Double.POSITIVE_INFINITY) {
                filter = filter.to(ranges[i][1]);
            }
            reqBuilder = reqBuilder.addAggregation(filter("filter" + i, filter));
        }

        SearchResponse resp = reqBuilder.get();
        Range range = resp.getAggregations().get("range");
        List<? extends Bucket> buckets = range.getBuckets();

        HashMap<String, Bucket> bucketMap = new HashMap<>(buckets.size());
        for (Bucket bucket : buckets) {
            bucketMap.put(bucket.getKeyAsString(), bucket);
        }

        for (int i = 0; i < ranges.length; ++i) {

            long count = 0;
            for (double[] values : docs) {
                for (double value : values) {
                    if (value >= ranges[i][0] && value < ranges[i][1]) {
                        ++count;
                        break;
                    }
                }
            }

            final Range.Bucket bucket = bucketMap.get(Integer.toString(i));
            assertEquals(bucket.getKeyAsString(), Integer.toString(i), bucket.getKeyAsString());
            assertEquals(bucket.getKeyAsString(), count, bucket.getDocCount());

            final Filter filter = resp.getAggregations().get("filter" + i);
            assertThat(filter.getDocCount(), equalTo(count));
        }
    }

    // test long/double/string terms aggs with high number of buckets that require array growth
    public void testDuelTerms() throws Exception {
        final int numDocs = scaledRandomIntBetween(1000, 2000);
        final int maxNumTerms = randomIntBetween(10, 5000);

        final Set<Integer> valuesSet = new HashSet<>();
        cluster().wipeIndices("idx");
        prepareCreate("idx").setMapping(
            jsonBuilder().startObject()
                .startObject("properties")
                .startObject("num")
                .field("type", "double")
                .endObject()
                .startObject("string_values")
                .field("type", "keyword")
                .startObject("fields")
                .startObject("doc_values")
                .field("type", "keyword")
                .field("index", false)
                .endObject()
                .endObject()
                .endObject()
                .startObject("long_values")
                .field("type", "long")
                .endObject()
                .startObject("double_values")
                .field("type", "double")
                .endObject()
                .endObject()
                .endObject()
        ).get();

        List<IndexRequestBuilder> indexingRequests = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            final int[] values = new int[randomInt(4)];
            for (int j = 0; j < values.length; ++j) {
                values[j] = randomInt(maxNumTerms - 1) - 1000;
                valuesSet.add(values[j]);
            }
            XContentBuilder source = jsonBuilder().startObject().field("num", randomDouble()).startArray("long_values");
            for (int j = 0; j < values.length; ++j) {
                source = source.value(values[j]);
            }
            source = source.endArray().startArray("double_values");
            for (int j = 0; j < values.length; ++j) {
                source = source.value((double) values[j]);
            }
            source = source.endArray().startArray("string_values");
            for (int j = 0; j < values.length; ++j) {
                source = source.value(Integer.toString(values[j]));
            }
            source = source.endArray().endObject();
            indexingRequests.add(client().prepareIndex("idx").setSource(source));
        }
        indexRandom(true, indexingRequests);

        assertNoFailures(
            client().admin().indices().prepareRefresh("idx").setIndicesOptions(IndicesOptions.lenientExpandOpen()).execute().get()
        );

        SearchResponse resp = client().prepareSearch("idx")
            .addAggregation(
                terms("long").field("long_values")
                    .size(maxNumTerms)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .subAggregation(min("min").field("num"))
            )
            .addAggregation(
                terms("double").field("double_values")
                    .size(maxNumTerms)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .subAggregation(max("max").field("num"))
            )
            .addAggregation(
                terms("string_map").field("string_values")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .executionHint(TermsAggregatorFactory.ExecutionMode.MAP.toString())
                    .size(maxNumTerms)
                    .subAggregation(stats("stats").field("num"))
            )
            .addAggregation(
                terms("string_global_ordinals").field("string_values")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .executionHint(TermsAggregatorFactory.ExecutionMode.GLOBAL_ORDINALS.toString())
                    .size(maxNumTerms)
                    .subAggregation(extendedStats("stats").field("num"))
            )
            .addAggregation(
                terms("string_global_ordinals_doc_values").field("string_values.doc_values")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .executionHint(TermsAggregatorFactory.ExecutionMode.GLOBAL_ORDINALS.toString())
                    .size(maxNumTerms)
                    .subAggregation(extendedStats("stats").field("num"))
            )
            .get();
        assertAllSuccessful(resp);
        assertEquals(numDocs, resp.getHits().getTotalHits().value);

        final Terms longTerms = resp.getAggregations().get("long");
        final Terms doubleTerms = resp.getAggregations().get("double");
        final Terms stringMapTerms = resp.getAggregations().get("string_map");
        final Terms stringGlobalOrdinalsTerms = resp.getAggregations().get("string_global_ordinals");
        final Terms stringGlobalOrdinalsDVTerms = resp.getAggregations().get("string_global_ordinals_doc_values");

        assertEquals(valuesSet.size(), longTerms.getBuckets().size());
        assertEquals(valuesSet.size(), doubleTerms.getBuckets().size());
        assertEquals(valuesSet.size(), stringMapTerms.getBuckets().size());
        assertEquals(valuesSet.size(), stringGlobalOrdinalsTerms.getBuckets().size());
        assertEquals(valuesSet.size(), stringGlobalOrdinalsDVTerms.getBuckets().size());
        for (Terms.Bucket bucket : longTerms.getBuckets()) {
            final Terms.Bucket doubleBucket = doubleTerms.getBucketByKey(Double.toString(Long.parseLong(bucket.getKeyAsString())));
            final Terms.Bucket stringMapBucket = stringMapTerms.getBucketByKey(bucket.getKeyAsString());
            final Terms.Bucket stringGlobalOrdinalsBucket = stringGlobalOrdinalsTerms.getBucketByKey(bucket.getKeyAsString());
            final Terms.Bucket stringGlobalOrdinalsDVBucket = stringGlobalOrdinalsDVTerms.getBucketByKey(bucket.getKeyAsString());
            assertNotNull(doubleBucket);
            assertNotNull(stringMapBucket);
            assertNotNull(stringGlobalOrdinalsBucket);
            assertNotNull(stringGlobalOrdinalsDVBucket);
            assertEquals(bucket.getDocCount(), doubleBucket.getDocCount());
            assertEquals(bucket.getDocCount(), stringMapBucket.getDocCount());
            assertEquals(bucket.getDocCount(), stringGlobalOrdinalsBucket.getDocCount());
            assertEquals(bucket.getDocCount(), stringGlobalOrdinalsDVBucket.getDocCount());
        }
    }

    // Duel between histograms and scripted terms
    public void testDuelTermsHistogram() throws Exception {
        prepareCreate("idx").setMapping(
            jsonBuilder().startObject()
                .startObject("properties")
                .startObject("num")
                .field("type", "double")
                .endObject()
                .endObject()
                .endObject()
        ).get();

        final int numDocs = scaledRandomIntBetween(500, 5000);
        final int maxNumTerms = randomIntBetween(10, 2000);
        final int interval = randomIntBetween(1, 100);

        final Integer[] values = new Integer[maxNumTerms];
        for (int i = 0; i < values.length; ++i) {
            values[i] = randomInt(maxNumTerms * 3) - maxNumTerms;
        }

        for (int i = 0; i < numDocs; ++i) {
            XContentBuilder source = jsonBuilder().startObject().field("num", randomDouble()).startArray("values");
            final int numValues = randomInt(4);
            for (int j = 0; j < numValues; ++j) {
                source = source.value(randomFrom(values));
            }
            source = source.endArray().endObject();
            client().prepareIndex("idx").setSource(source).get();
        }
        assertNoFailures(
            client().admin().indices().prepareRefresh("idx").setIndicesOptions(IndicesOptions.lenientExpandOpen()).execute().get()
        );

        Map<String, Object> params = new HashMap<>();
        params.put("interval", interval);

        SearchResponse resp = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("values")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "floor(_value / interval)", params))
                    .size(maxNumTerms)
            )
            .addAggregation(histogram("histo").field("values").interval(interval).minDocCount(1))
            .get();

        assertSearchResponse(resp);

        Terms terms = resp.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        Histogram histo = resp.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(terms.getBuckets().size(), equalTo(histo.getBuckets().size()));
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            final double key = ((Number) bucket.getKey()).doubleValue() / interval;
            final Terms.Bucket termsBucket = terms.getBucketByKey(String.valueOf(key));
            assertEquals(bucket.getDocCount(), termsBucket.getDocCount());
        }
    }

    public void testLargeNumbersOfPercentileBuckets() throws Exception {
        // test high numbers of percentile buckets to make sure paging and release work correctly
        prepareCreate("idx").setMapping(
            jsonBuilder().startObject()
                .startObject("properties")
                .startObject("double_value")
                .field("type", "double")
                .endObject()
                .endObject()
                .endObject()
        ).get();

        final int numDocs = scaledRandomIntBetween(2500, 5000);
        logger.info("Indexing [{}] docs", numDocs);
        List<IndexRequestBuilder> indexingRequests = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            indexingRequests.add(client().prepareIndex("idx").setId(Integer.toString(i)).setSource("double_value", randomDouble()));
        }
        indexRandom(true, indexingRequests);

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("double_value")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .subAggregation(percentiles("pcts").field("double_value"))
            )
            .get();
        assertAllSuccessful(response);
        assertEquals(numDocs, response.getHits().getTotalHits().value);
    }

    // https://github.com/elastic/elasticsearch/issues/6435
    public void testReduce() throws Exception {
        createIndex("idx");
        final int value = randomIntBetween(0, 10);
        indexRandom(true, client().prepareIndex("idx").setSource("f", value));
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                filter("filter", QueryBuilders.matchAllQuery()).subAggregation(
                    range("range").field("f").addUnboundedTo(6).addUnboundedFrom(6).subAggregation(sum("sum").field("f"))
                )
            )
            .get();

        assertSearchResponse(response);

        Filter filter = response.getAggregations().get("filter");
        assertNotNull(filter);
        assertEquals(1, filter.getDocCount());

        Range range = filter.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Bucket> buckets = range.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-6.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(6.0));
        assertThat(bucket.getDocCount(), equalTo(value < 6 ? 1L : 0L));
        Sum sum = bucket.getAggregations().get("sum");
        assertEquals(value < 6 ? value : 0, sum.getValue(), 0d);

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("6.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(6.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getDocCount(), equalTo(value >= 6 ? 1L : 0L));
        sum = bucket.getAggregations().get("sum");
        assertEquals(value >= 6 ? value : 0, sum.getValue(), 0d);
    }

    private void assertEquals(Terms t1, Terms t2) {
        List<? extends Terms.Bucket> t1Buckets = t1.getBuckets();
        List<? extends Terms.Bucket> t2Buckets = t1.getBuckets();
        assertEquals(t1Buckets.size(), t2Buckets.size());
        for (Iterator<? extends Terms.Bucket> it1 = t1Buckets.iterator(), it2 = t2Buckets.iterator(); it1.hasNext();) {
            final Terms.Bucket b1 = it1.next();
            final Terms.Bucket b2 = it2.next();
            assertEquals(b1.getDocCount(), b2.getDocCount());
            assertEquals(b1.getKey(), b2.getKey());
        }
    }

    public void testDuelDepthBreadthFirst() throws Exception {
        createIndex("idx");
        final int numDocs = randomIntBetween(100, 500);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            final int v1 = randomInt(1 << randomInt(7));
            final int v2 = randomInt(1 << randomInt(7));
            final int v3 = randomInt(1 << randomInt(7));
            reqs.add(client().prepareIndex("idx").setSource("f1", v1, "f2", v2, "f3", v3));
        }
        indexRandom(true, reqs);

        final SearchResponse r1 = client().prepareSearch("idx")
            .addAggregation(
                terms("f1").field("f1")
                    .collectMode(SubAggCollectionMode.DEPTH_FIRST)
                    .subAggregation(
                        terms("f2").field("f2")
                            .collectMode(SubAggCollectionMode.DEPTH_FIRST)
                            .subAggregation(terms("f3").field("f3").collectMode(SubAggCollectionMode.DEPTH_FIRST))
                    )
            )
            .get();
        assertSearchResponse(r1);
        final SearchResponse r2 = client().prepareSearch("idx")
            .addAggregation(
                terms("f1").field("f1")
                    .collectMode(SubAggCollectionMode.BREADTH_FIRST)
                    .subAggregation(
                        terms("f2").field("f2")
                            .collectMode(SubAggCollectionMode.BREADTH_FIRST)
                            .subAggregation(terms("f3").field("f3").collectMode(SubAggCollectionMode.BREADTH_FIRST))
                    )
            )
            .get();
        assertSearchResponse(r2);

        final Terms t1 = r1.getAggregations().get("f1");
        final Terms t2 = r2.getAggregations().get("f1");
        assertEquals(t1, t2);
        for (Terms.Bucket b1 : t1.getBuckets()) {
            final Terms.Bucket b2 = t2.getBucketByKey(b1.getKeyAsString());
            final Terms sub1 = b1.getAggregations().get("f2");
            final Terms sub2 = b2.getAggregations().get("f2");
            assertEquals(sub1, sub2);
            for (Terms.Bucket subB1 : sub1.getBuckets()) {
                final Terms.Bucket subB2 = sub2.getBucketByKey(subB1.getKeyAsString());
                final Terms subSub1 = subB1.getAggregations().get("f3");
                final Terms subSub2 = subB2.getAggregations().get("f3");
                assertEquals(subSub1, subSub2);
            }
        }
    }

}
