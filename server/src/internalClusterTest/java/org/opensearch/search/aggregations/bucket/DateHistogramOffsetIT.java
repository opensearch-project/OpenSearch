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

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateFormatters;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 * The serialisation of offsets for the date histogram aggregation was corrected in version 1.4 to allow negative offsets and as such the
 * serialisation of negative offsets in these tests would break in pre 1.4 versions.  These tests are separated from the other
 * DateHistogramTests so the AssertingLocalTransport for these tests can be set to only use versions 1.4 onwards while keeping the other
 * tests using all versions
 */
@OpenSearchIntegTestCase.SuiteScopeTestCase
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class DateHistogramOffsetIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final String DATE_FORMAT = "yyyy-MM-dd:hh-mm-ss";
    private static final DateFormatter FORMATTER = DateFormatter.forPattern(DATE_FORMAT);

    public DateHistogramOffsetIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    private ZonedDateTime date(String date) {
        return DateFormatters.from(DateFieldMapper.getDefaultDateTimeFormatter().parse(date));
    }

    @Before
    public void beforeEachTest() throws IOException {
        prepareCreate("idx2").setMapping("date", "type=date").get();
    }

    @After
    public void afterEachTest() throws IOException {
        internalCluster().wipeIndices("idx2");
    }

    private void prepareIndex(ZonedDateTime date, int numHours, int stepSizeHours, int idxIdStart) throws IOException,
        InterruptedException {

        IndexRequestBuilder[] reqs = new IndexRequestBuilder[numHours];
        for (int i = idxIdStart; i < idxIdStart + reqs.length; i++) {
            reqs[i - idxIdStart] = client().prepareIndex("idx2")
                .setId("" + i)
                .setSource(jsonBuilder().startObject().timeField("date", date).endObject());
            date = date.plusHours(stepSizeHours);
        }
        indexRandom(true, reqs);
    }

    public void testSingleValueWithPositiveOffset() throws Exception {
        prepareIndex(date("2014-03-11T00:00:00+00:00"), 5, 1, 0);

        SearchResponse response = client().prepareSearch("idx2")
            .setQuery(matchAllQuery())
            .addAggregation(
                dateHistogram("date_histo").field("date").offset("2h").format(DATE_FORMAT).dateHistogramInterval(DateHistogramInterval.DAY)
            )
            .get();

        assertThat(response.getHits().getTotalHits().value(), equalTo(5L));

        Histogram histo = response.getAggregations().get("date_histo");
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        checkBucketFor(buckets.get(0), ZonedDateTime.of(2014, 3, 10, 2, 0, 0, 0, ZoneOffset.UTC), 2L);
        checkBucketFor(buckets.get(1), ZonedDateTime.of(2014, 3, 11, 2, 0, 0, 0, ZoneOffset.UTC), 3L);
    }

    public void testSingleValueWithNegativeOffset() throws Exception {
        prepareIndex(date("2014-03-11T00:00:00+00:00"), 5, -1, 0);

        SearchResponse response = client().prepareSearch("idx2")
            .setQuery(matchAllQuery())
            .addAggregation(
                dateHistogram("date_histo").field("date").offset("-2h").format(DATE_FORMAT).dateHistogramInterval(DateHistogramInterval.DAY)
            )
            .get();

        assertThat(response.getHits().getTotalHits().value(), equalTo(5L));

        Histogram histo = response.getAggregations().get("date_histo");
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(2));

        checkBucketFor(buckets.get(0), ZonedDateTime.of(2014, 3, 9, 22, 0, 0, 0, ZoneOffset.UTC), 2L);
        checkBucketFor(buckets.get(1), ZonedDateTime.of(2014, 3, 10, 22, 0, 0, 0, ZoneOffset.UTC), 3L);
    }

    /**
     * Set offset so day buckets start at 6am. Index first 12 hours for two days, with one day gap.
     */
    public void testSingleValueWithOffsetMinDocCount() throws Exception {
        prepareIndex(date("2014-03-11T00:00:00+00:00"), 12, 1, 0);
        prepareIndex(date("2014-03-14T00:00:00+00:00"), 12, 1, 13);

        SearchResponse response = client().prepareSearch("idx2")
            .setQuery(matchAllQuery())
            .addAggregation(
                dateHistogram("date_histo").field("date")
                    .offset("6h")
                    .minDocCount(0)
                    .format(DATE_FORMAT)
                    .dateHistogramInterval(DateHistogramInterval.DAY)
            )
            .get();

        assertThat(response.getHits().getTotalHits().value(), equalTo(24L));

        Histogram histo = response.getAggregations().get("date_histo");
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(5));

        checkBucketFor(buckets.get(0), ZonedDateTime.of(2014, 3, 10, 6, 0, 0, 0, ZoneOffset.UTC), 6L);
        checkBucketFor(buckets.get(1), ZonedDateTime.of(2014, 3, 11, 6, 0, 0, 0, ZoneOffset.UTC), 6L);
        checkBucketFor(buckets.get(2), ZonedDateTime.of(2014, 3, 12, 6, 0, 0, 0, ZoneOffset.UTC), 0L);
        checkBucketFor(buckets.get(3), ZonedDateTime.of(2014, 3, 13, 6, 0, 0, 0, ZoneOffset.UTC), 6L);
        checkBucketFor(buckets.get(4), ZonedDateTime.of(2014, 3, 14, 6, 0, 0, 0, ZoneOffset.UTC), 6L);
    }

    /**
     * @param bucket the bucket to check assertions for
     * @param key the expected key
     * @param expectedSize the expected size of the bucket
     */
    private static void checkBucketFor(Histogram.Bucket bucket, ZonedDateTime key, long expectedSize) {
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKeyAsString(), equalTo(FORMATTER.format(key)));
        assertThat(((ZonedDateTime) bucket.getKey()), equalTo(key));
        assertThat(bucket.getDocCount(), equalTo(expectedSize));
    }
}
