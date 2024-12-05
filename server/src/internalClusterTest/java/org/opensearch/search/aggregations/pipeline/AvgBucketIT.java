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

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.search.aggregations.PipelineAggregatorBuilders.avgBucket;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsNull.notNullValue;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class AvgBucketIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";

    static int numDocs;
    static int interval;
    static int minRandomValue;
    static int maxRandomValue;
    static int numValueBuckets;
    static long[] valueCounts;

    public AvgBucketIT(Settings staticSettings) {
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

    public void testDocCountTopLevel() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).extendedBounds(minRandomValue, maxRandomValue)
            )
            .addAggregation(avgBucket("avg_bucket", "histo>_count"))
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        double sum = 0;
        int count = 0;
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
            count++;
            sum += bucket.getDocCount();
        }

        double avgValue = count == 0 ? Double.NaN : (sum / count);
        InternalSimpleValue avgBucketValue = response.getAggregations().get("avg_bucket");
        assertThat(avgBucketValue, notNullValue());
        assertThat(avgBucketValue.getName(), equalTo("avg_bucket"));
        assertThat(avgBucketValue.value(), equalTo(avgValue));
    }

    public void testDocCountAsSubAgg() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).extendedBounds(minRandomValue, maxRandomValue)
                    )
                    .subAggregation(avgBucket("avg_bucket", "histo>_count"))
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
            List<? extends Bucket> buckets = histo.getBuckets();

            double sum = 0;
            int count = 0;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                count++;
                sum += bucket.getDocCount();
            }

            double avgValue = count == 0 ? Double.NaN : (sum / count);
            InternalSimpleValue avgBucketValue = termsBucket.getAggregations().get("avg_bucket");
            assertThat(avgBucketValue, notNullValue());
            assertThat(avgBucketValue.getName(), equalTo("avg_bucket"));
            assertThat(avgBucketValue.value(), equalTo(avgValue));
        }
    }

    public void testMetricTopLevel() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(terms("terms").field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
            .addAggregation(avgBucket("avg_bucket", "terms>sum"))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(interval));

        double bucketSum = 0;
        int count = 0;
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((String) bucket.getKey(), equalTo("tag" + (i % interval)));
            assertThat(bucket.getDocCount(), greaterThan(0L));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            count++;
            bucketSum += sum.value();
        }

        double avgValue = count == 0 ? Double.NaN : (bucketSum / count);
        InternalSimpleValue avgBucketValue = response.getAggregations().get("avg_bucket");
        assertThat(avgBucketValue, notNullValue());
        assertThat(avgBucketValue.getName(), equalTo("avg_bucket"));
        assertThat(avgBucketValue.value(), equalTo(avgValue));
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
                    .subAggregation(avgBucket("avg_bucket", "histo>sum"))
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
            List<? extends Bucket> buckets = histo.getBuckets();

            double bucketSum = 0;
            int count = 0;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                if (bucket.getDocCount() != 0) {
                    Sum sum = bucket.getAggregations().get("sum");
                    assertThat(sum, notNullValue());
                    count++;
                    bucketSum += sum.value();
                }
            }

            double avgValue = count == 0 ? Double.NaN : (bucketSum / count);
            InternalSimpleValue avgBucketValue = termsBucket.getAggregations().get("avg_bucket");
            assertThat(avgBucketValue, notNullValue());
            assertThat(avgBucketValue.getName(), equalTo("avg_bucket"));
            assertThat(avgBucketValue.value(), equalTo(avgValue));
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
                    .subAggregation(avgBucket("avg_bucket", "histo>sum").gapPolicy(GapPolicy.INSERT_ZEROS))
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
            List<? extends Bucket> buckets = histo.getBuckets();

            double bucketSum = 0;
            int count = 0;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                Sum sum = bucket.getAggregations().get("sum");
                assertThat(sum, notNullValue());

                count++;
                bucketSum += sum.value();
            }

            double avgValue = count == 0 ? Double.NaN : (bucketSum / count);
            InternalSimpleValue avgBucketValue = termsBucket.getAggregations().get("avg_bucket");
            assertThat(avgBucketValue, notNullValue());
            assertThat(avgBucketValue.getName(), equalTo("avg_bucket"));
            assertThat(avgBucketValue.value(), equalTo(avgValue));
        }
    }

    public void testNoBuckets() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .includeExclude(new IncludeExclude(null, "tag.*"))
                    .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
            )
            .addAggregation(avgBucket("avg_bucket", "terms>sum"))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(0));

        InternalSimpleValue avgBucketValue = response.getAggregations().get("avg_bucket");
        assertThat(avgBucketValue, notNullValue());
        assertThat(avgBucketValue.getName(), equalTo("avg_bucket"));
        assertThat(avgBucketValue.value(), equalTo(Double.NaN));
    }

    public void testNested() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).extendedBounds(minRandomValue, maxRandomValue)
                    )
                    .subAggregation(avgBucket("avg_histo_bucket", "histo>_count"))
            )
            .addAggregation(avgBucket("avg_terms_bucket", "terms>avg_histo_bucket"))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        double aggTermsSum = 0;
        int aggTermsCount = 0;
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Bucket> buckets = histo.getBuckets();

            double aggHistoSum = 0;
            int aggHistoCount = 0;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));

                aggHistoCount++;
                aggHistoSum += bucket.getDocCount();
            }

            double avgHistoValue = aggHistoCount == 0 ? Double.NaN : (aggHistoSum / aggHistoCount);
            InternalSimpleValue avgBucketValue = termsBucket.getAggregations().get("avg_histo_bucket");
            assertThat(avgBucketValue, notNullValue());
            assertThat(avgBucketValue.getName(), equalTo("avg_histo_bucket"));
            assertThat(avgBucketValue.value(), equalTo(avgHistoValue));

            aggTermsCount++;
            aggTermsSum += avgHistoValue;
        }

        double avgTermsValue = aggTermsCount == 0 ? Double.NaN : (aggTermsSum / aggTermsCount);
        InternalSimpleValue avgBucketValue = response.getAggregations().get("avg_terms_bucket");
        assertThat(avgBucketValue, notNullValue());
        assertThat(avgBucketValue.getName(), equalTo("avg_terms_bucket"));
        assertThat(avgBucketValue.value(), equalTo(avgTermsValue));
    }
}
