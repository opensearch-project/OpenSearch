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

package org.opensearch.search.aggregations.pipeline;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Percentile;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.search.aggregations.PipelineAggregatorBuilders.percentilesBucket;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsNull.notNullValue;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class PercentilesBucketIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";
    private static final double[] PERCENTS = { 0.0, 1.0, 25.0, 50.0, 75.0, 99.0, 100.0 };
    static int numDocs;
    static int interval;
    static int minRandomValue;
    static int maxRandomValue;
    static int numValueBuckets;
    static long[] valueCounts;

    public PercentilesBucketIT(Settings staticSettings) {
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
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("idx").setMapping("tag", "type=keyword").get());
        createIndex("idx_unmapped");

        numDocs = randomIntBetween(6, 20);
        interval = randomIntBetween(2, 5);

        minRandomValue = 0;
        maxRandomValue = 20;

        numValueBuckets = ((maxRandomValue - minRandomValue) / interval) + 1;
        valueCounts = new long[numValueBuckets];

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            int fieldValue = randomIntBetween(minRandomValue, maxRandomValue);
            builders.add(
                client().prepareIndex("idx")
                    .setSource(
                        jsonBuilder().startObject()
                            .field(SINGLE_VALUED_FIELD_NAME, fieldValue)
                            .field("tag", "tag" + (i % interval))
                            .endObject()
                    )
            );
            final int bucket = (fieldValue / interval); // + (fieldValue < 0 ? -1 : 0) - (minRandomValue / interval - 1);
            valueCounts[bucket]++;
        }

        assertAcked(prepareCreate("empty_bucket_idx").setMapping(SINGLE_VALUED_FIELD_NAME, "type=integer"));
        for (int i = 0; i < 2; i++) {
            builders.add(
                client().prepareIndex("empty_bucket_idx")
                    .setId("" + i)
                    .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i * 2).endObject())
            );
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testDocCountopLevel() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).extendedBounds(minRandomValue, maxRandomValue)
            )
            .addAggregation(percentilesBucket("percentiles_bucket", "histo>_count").setPercents(PERCENTS))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        double[] values = new double[numValueBuckets];
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
            values[i] = bucket.getDocCount();
        }

        Arrays.sort(values);

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentiles_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
        assertPercentileBucket(PERCENTS, values, percentilesBucketValue);
    }

    public void testDocCountAsSubAgg() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).extendedBounds(minRandomValue, maxRandomValue)
                    )
                    .subAggregation(percentilesBucket("percentiles_bucket", "histo>_count").setPercents(PERCENTS))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Histogram.Bucket> buckets = histo.getBuckets();

            double[] values = new double[numValueBuckets];
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                values[j] = bucket.getDocCount();
            }

            Arrays.sort(values);

            PercentilesBucket percentilesBucketValue = termsBucket.getAggregations().get("percentiles_bucket");
            assertThat(percentilesBucketValue, notNullValue());
            assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
            assertPercentileBucket(PERCENTS, values, percentilesBucketValue);
        }
    }

    public void testMetricTopLevel() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(terms("terms").field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
            .addAggregation(percentilesBucket("percentiles_bucket", "terms>sum").setPercents(PERCENTS))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(interval));

        double[] values = new double[interval];
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((String) bucket.getKey(), equalTo("tag" + (i % interval)));
            assertThat(bucket.getDocCount(), greaterThan(0L));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            values[i] = sum.value();
        }

        Arrays.sort(values);

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentiles_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
        assertPercentileBucket(PERCENTS, values, percentilesBucketValue);
    }

    public void testMetricTopLevelDefaultPercents() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(terms("terms").field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
            .addAggregation(percentilesBucket("percentiles_bucket", "terms>sum"))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(interval));

        double[] values = new double[interval];
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((String) bucket.getKey(), equalTo("tag" + (i % interval)));
            assertThat(bucket.getDocCount(), greaterThan(0L));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            values[i] = sum.value();
        }

        Arrays.sort(values);

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentiles_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
        assertPercentileBucket(values, percentilesBucketValue);
    }

    public void testMetricAsSubAgg() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                            .interval(interval)
                            .extendedBounds(minRandomValue, maxRandomValue)
                            .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
                    )
                    .subAggregation(percentilesBucket("percentiles_bucket", "histo>sum").setPercents(PERCENTS))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Histogram.Bucket> buckets = histo.getBuckets();

            List<Double> values = new ArrayList<>(numValueBuckets);
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                if (bucket.getDocCount() != 0) {
                    Sum sum = bucket.getAggregations().get("sum");
                    assertThat(sum, notNullValue());
                    values.add(sum.value());
                }
            }

            Collections.sort(values);

            PercentilesBucket percentilesBucketValue = termsBucket.getAggregations().get("percentiles_bucket");
            assertThat(percentilesBucketValue, notNullValue());
            assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
            assertPercentileBucket(PERCENTS, values.stream().mapToDouble(Double::doubleValue).toArray(), percentilesBucketValue);
        }
    }

    public void testMetricAsSubAggWithInsertZeros() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                            .interval(interval)
                            .extendedBounds(minRandomValue, maxRandomValue)
                            .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
                    )
                    .subAggregation(
                        percentilesBucket("percentiles_bucket", "histo>sum").gapPolicy(BucketHelpers.GapPolicy.INSERT_ZEROS)
                            .setPercents(PERCENTS)
                    )
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Histogram.Bucket> buckets = histo.getBuckets();

            double[] values = new double[numValueBuckets];
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                Sum sum = bucket.getAggregations().get("sum");
                assertThat(sum, notNullValue());

                values[j] = sum.value();
            }

            Arrays.sort(values);

            PercentilesBucket percentilesBucketValue = termsBucket.getAggregations().get("percentiles_bucket");
            assertThat(percentilesBucketValue, notNullValue());
            assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
            assertPercentileBucket(PERCENTS, values, percentilesBucketValue);
        }
    }

    public void testNoBuckets() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .includeExclude(new IncludeExclude(null, "tag.*"))
                    .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
            )
            .addAggregation(percentilesBucket("percentiles_bucket", "terms>sum").setPercents(PERCENTS))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(0));

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentiles_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
        for (Double p : PERCENTS) {
            assertThat(percentilesBucketValue.percentile(p), equalTo(Double.NaN));
        }
    }

    public void testWrongPercents() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .includeExclude(new IncludeExclude(null, "tag.*"))
                    .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
            )
            .addAggregation(percentilesBucket("percentiles_bucket", "terms>sum").setPercents(PERCENTS))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(0));

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentiles_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));

        try {
            percentilesBucketValue.percentile(2.0);
            fail("2.0 was not a valid percent, should have thrown exception");
        } catch (IllegalArgumentException exception) {
            // All good
        }
    }

    public void testBadPercents() throws Exception {
        double[] badPercents = { -1.0, 110.0 };

        try {
            client().prepareSearch("idx")
                .addAggregation(terms("terms").field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                .addAggregation(percentilesBucket("percentiles_bucket", "terms>sum").setPercents(badPercents))
                .get();

            fail("Illegal percent's were provided but no exception was thrown.");
        } catch (Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause == null) {
                throw e;
            } else if (cause instanceof SearchPhaseExecutionException) {
                SearchPhaseExecutionException spee = (SearchPhaseExecutionException) e;
                Throwable rootCause = spee.getRootCause();
                if (!(rootCause instanceof IllegalArgumentException)) {
                    throw e;
                }
            } else if (!(cause instanceof IllegalArgumentException)) {
                throw e;
            }
        }

    }

    public void testBadPercents_asSubAgg() throws Exception {
        double[] badPercents = { -1.0, 110.0 };

        try {
            client().prepareSearch("idx")
                .addAggregation(
                    terms("terms").field("tag")
                        .order(BucketOrder.key(true))
                        .subAggregation(
                            histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                                .interval(interval)
                                .extendedBounds(minRandomValue, maxRandomValue)
                        )
                        .subAggregation(percentilesBucket("percentiles_bucket", "histo>_count").setPercents(badPercents))
                )
                .get();

            fail("Illegal percent's were provided but no exception was thrown.");
        } catch (Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause == null) {
                throw e;
            } else if (cause instanceof SearchPhaseExecutionException) {
                SearchPhaseExecutionException spee = (SearchPhaseExecutionException) e;
                Throwable rootCause = spee.getRootCause();
                if (!(rootCause instanceof IllegalArgumentException)) {
                    throw e;
                }
            } else if (!(cause instanceof IllegalArgumentException)) {
                throw e;
            }
        }

    }

    public void testNested() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).extendedBounds(minRandomValue, maxRandomValue)
                    )
                    .subAggregation(percentilesBucket("percentile_histo_bucket", "histo>_count").setPercents(PERCENTS))
            )
            .addAggregation(percentilesBucket("percentile_terms_bucket", "terms>percentile_histo_bucket.50").setPercents(PERCENTS))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        double[] values = new double[termsBuckets.size()];
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Histogram.Bucket> buckets = histo.getBuckets();

            double[] innerValues = new double[numValueBuckets];
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));

                innerValues[j] = bucket.getDocCount();
            }
            Arrays.sort(innerValues);

            PercentilesBucket percentilesBucketValue = termsBucket.getAggregations().get("percentile_histo_bucket");
            assertThat(percentilesBucketValue, notNullValue());
            assertThat(percentilesBucketValue.getName(), equalTo("percentile_histo_bucket"));
            assertPercentileBucket(PERCENTS, innerValues, percentilesBucketValue);
            values[i] = percentilesBucketValue.percentile(50.0);
        }

        Arrays.sort(values);

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentile_terms_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentile_terms_bucket"));
        assertPercentileBucket(PERCENTS, values, percentilesBucketValue);
    }

    public void testNestedWithDecimal() throws Exception {
        double[] percent = { 99.9 };
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).extendedBounds(minRandomValue, maxRandomValue)
                    )
                    .subAggregation(percentilesBucket("percentile_histo_bucket", "histo>_count").setPercents(percent))
            )
            .addAggregation(percentilesBucket("percentile_terms_bucket", "terms>percentile_histo_bucket[99.9]").setPercents(percent))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        double[] values = new double[termsBuckets.size()];
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Histogram.Bucket> buckets = histo.getBuckets();

            double[] innerValues = new double[numValueBuckets];
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));

                innerValues[j] = bucket.getDocCount();
            }
            Arrays.sort(innerValues);

            PercentilesBucket percentilesBucketValue = termsBucket.getAggregations().get("percentile_histo_bucket");
            assertThat(percentilesBucketValue, notNullValue());
            assertThat(percentilesBucketValue.getName(), equalTo("percentile_histo_bucket"));
            assertPercentileBucket(innerValues, percentilesBucketValue);
            values[i] = percentilesBucketValue.percentile(99.9);
        }

        Arrays.sort(values);

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentile_terms_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentile_terms_bucket"));
        for (Double p : percent) {
            double expected = values[(int) ((p / 100) * values.length)];
            assertThat(percentilesBucketValue.percentile(p), equalTo(expected));
        }
    }

    private void assertPercentileBucket(double[] values, PercentilesBucket percentiles) {
        for (Percentile percentile : percentiles) {
            assertEquals(percentiles.percentile(percentile.getPercent()), percentile.getValue(), 0d);
            int index = (int) Math.round((percentile.getPercent() / 100.0) * (values.length - 1));
            assertThat(percentile.getValue(), equalTo(values[index]));
        }
    }

    private void assertPercentileBucket(double[] percents, double[] values, PercentilesBucket percentiles) {
        Iterator<Percentile> it = percentiles.iterator();
        for (int i = 0; i < percents.length; ++i) {
            assertTrue(it.hasNext());
            assertEquals(percents[i], it.next().getPercent(), 0d);
        }
        assertFalse(it.hasNext());
        assertPercentileBucket(values, percentiles);
    }
}
