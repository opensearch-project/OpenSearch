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

package org.opensearch.search.simple;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.apache.lucene.search.TotalHits.Relation.EQUAL_TO;
import static org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;

public class SimpleSearchIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public SimpleSearchIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() }
        );
    }

    public void testSearchNullIndex() {
        expectThrows(
            NullPointerException.class,
            () -> client().prepareSearch((String) null).setQuery(QueryBuilders.termQuery("_id", "XXX1")).get()
        );

        expectThrows(
            NullPointerException.class,
            () -> client().prepareSearch((String[]) null).setQuery(QueryBuilders.termQuery("_id", "XXX1")).get()
        );

    }

    public void testSearchRandomPreference() throws InterruptedException, ExecutionException {
        createIndex("test");
        indexRandom(
            true,
            client().prepareIndex("test").setId("1").setSource("field", "value"),
            client().prepareIndex("test").setId("2").setSource("field", "value"),
            client().prepareIndex("test").setId("3").setSource("field", "value"),
            client().prepareIndex("test").setId("4").setSource("field", "value"),
            client().prepareIndex("test").setId("5").setSource("field", "value"),
            client().prepareIndex("test").setId("6").setSource("field", "value")
        );

        int iters = scaledRandomIntBetween(10, 20);
        for (int i = 0; i < iters; i++) {
            String randomPreference = randomUnicodeOfLengthBetween(0, 4);
            // randomPreference should not start with '_' (reserved for known preference types (e.g. _shards, _primary)
            while (randomPreference.startsWith("_")) {
                randomPreference = randomUnicodeOfLengthBetween(0, 4);
            }
            // id is not indexed, but lets see that we automatically convert to
            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .setPreference(randomPreference)
                .get();
            assertHitCount(searchResponse, 6L);

        }
    }

    public void testSimpleIp() throws Exception {
        createIndex("test");

        client().admin()
            .indices()
            .preparePutMapping("test")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(MapperService.SINGLE_MAPPING_NAME)
                    .startObject("properties")
                    .startObject("from")
                    .field("type", "ip")
                    .endObject()
                    .startObject("to")
                    .field("type", "ip")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        client().prepareIndex("test").setId("1").setSource("from", "192.168.0.5", "to", "192.168.0.10").setRefreshPolicy(IMMEDIATE).get();

        SearchResponse search = client().prepareSearch()
            .setQuery(boolQuery().must(rangeQuery("from").lte("192.168.0.7")).must(rangeQuery("to").gte("192.168.0.7")))
            .get();

        assertHitCount(search, 1L);
    }

    public void testIpCidr() throws Exception {
        createIndex("test");

        client().admin()
            .indices()
            .preparePutMapping("test")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(MapperService.SINGLE_MAPPING_NAME)
                    .startObject("properties")
                    .startObject("ip")
                    .field("type", "ip")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("ip", "192.168.0.1").get();
        client().prepareIndex("test").setId("2").setSource("ip", "192.168.0.2").get();
        client().prepareIndex("test").setId("3").setSource("ip", "192.168.0.3").get();
        client().prepareIndex("test").setId("4").setSource("ip", "192.168.1.4").get();
        client().prepareIndex("test").setId("5").setSource("ip", "2001:db8::ff00:42:8329").get();
        refresh();

        SearchResponse search = client().prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.0.1"))).get();
        assertHitCount(search, 1L);

        search = client().prepareSearch().setQuery(queryStringQuery("ip: 192.168.0.1")).get();
        assertHitCount(search, 1L);

        search = client().prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.0.1/32"))).get();
        assertHitCount(search, 1L);

        search = client().prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.0.0/24"))).get();
        assertHitCount(search, 3L);

        search = client().prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.0.0.0/8"))).get();
        assertHitCount(search, 4L);

        search = client().prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "0.0.0.0/0"))).get();
        assertHitCount(search, 4L);

        search = client().prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "2001:db8::ff00:42:8329/128"))).get();
        assertHitCount(search, 1L);

        search = client().prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "2001:db8::/64"))).get();
        assertHitCount(search, 1L);

        search = client().prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "::/0"))).get();
        assertHitCount(search, 5L);

        search = client().prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "192.168.1.5/32"))).get();
        assertHitCount(search, 0L);

        assertFailures(
            client().prepareSearch().setQuery(boolQuery().must(QueryBuilders.termQuery("ip", "0/0/0/0/0"))),
            RestStatus.BAD_REQUEST,
            containsString("Expected [ip/prefix] but was [0/0/0/0/0]")
        );
    }

    public void testSimpleId() {
        createIndex("test");

        client().prepareIndex("test").setId("XXX1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        // id is not indexed, but lets see that we automatically convert to
        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.termQuery("_id", "XXX1")).get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch().setQuery(QueryBuilders.queryStringQuery("_id:XXX1")).get();
        assertHitCount(searchResponse, 1L);
    }

    public void testSimpleDateRange() throws Exception {
        createIndex("test");
        client().prepareIndex("test").setId("1").setSource("field", "2010-01-05T02:00").get();
        client().prepareIndex("test").setId("2").setSource("field", "2010-01-06T02:00").get();
        ensureGreen();
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-03||+2d").lte("2010-01-04||+2d/d"))
            .get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2L);

        searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-05T02:00").lte("2010-01-06T02:00"))
            .get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2L);

        searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.rangeQuery("field").gte("2010-01-05T02:00").lt("2010-01-06T02:00"))
            .get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.rangeQuery("field").gt("2010-01-05T02:00").lt("2010-01-06T02:00"))
            .get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 0L);

        searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.queryStringQuery("field:[2010-01-03||+2d TO 2010-01-04||+2d/d]"))
            .get();
        assertHitCount(searchResponse, 2L);
    }

    public void dotestSimpleTerminateAfterCountWithSize(int size, int max) throws Exception {
        prepareCreate("test").setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)).get();
        ensureGreen();
        List<IndexRequestBuilder> docbuilders = new ArrayList<>(max);

        for (int i = 1; i <= max; i++) {
            String id = String.valueOf(i);
            docbuilders.add(client().prepareIndex("test").setId(id).setSource("field", i));
        }

        indexRandom(true, docbuilders);
        ensureGreen();
        refresh();

        SearchResponse searchResponse;
        for (int i = 1; i < max; i++) {
            searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(max))
                .setTerminateAfter(i)
                .setSize(size)
                .setTrackTotalHits(true)
                .get();

            // Do not expect an exact match as an optimization introduced by https://issues.apache.org/jira/browse/LUCENE-10620
            // can produce a total hit count > terminated_after, but this only kicks in
            // when size = 0 which is when TotalHitCountCollector is used.
            if (size == 0) {
                assertHitCount(searchResponse, i, max);
            } else {
                assertHitCount(searchResponse, i);
            }
            assertTrue(searchResponse.isTerminatedEarly());
            assertEquals(Math.min(i, size), searchResponse.getHits().getHits().length);
        }

        searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(max))
            .setTerminateAfter(2 * max)
            .get();

        assertHitCount(searchResponse, max);
        assertFalse(searchResponse.isTerminatedEarly());
    }

    public void testSimpleTerminateAfterCountSize0() throws Exception {
        int max = randomIntBetween(3, 29);
        dotestSimpleTerminateAfterCountWithSize(0, max);
    }

    public void testSimpleTerminateAfterCountRandomSize() throws Exception {
        int max = randomIntBetween(3, 29);
        dotestSimpleTerminateAfterCountWithSize(randomIntBetween(1, max), max);
    }

    /**
     * Special cases when size = 0:
     *
     * If track_total_hits = true:
     *   Weight#count optimization can cause totalHits in the response to be up to the total doc count regardless of terminate_after.
     *   So, we will have to do a range check, not an equality check.
     *
     * If track_total_hits != true, but set to a value AND terminate_after is set:
     *   Again, due to the optimization, any count can be returned.
     *   Up to terminate_after, relation == EQUAL_TO.
     *   But if track_total_hits_up_to &ge; terminate_after, relation can be EQ _or_ GTE.
     *   This ambiguity is due to the fact that totalHits == track_total_hits_up_to
     *   or totalHits &gt; track_total_hits_up_to and SearchPhaseController sets totalHits = track_total_hits_up_to when returning results
     *   in which case relation = GTE.
     *
     * @param size
     * @throws Exception
     */
    public void doTestSimpleTerminateAfterTrackTotalHitsUpTo(int size) throws Exception {
        prepareCreate("test").setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)).get();
        ensureGreen();
        int numDocs = 29;
        List<IndexRequestBuilder> docbuilders = new ArrayList<>(numDocs);

        for (int i = 1; i <= numDocs; i++) {
            String id = String.valueOf(i);
            docbuilders.add(client().prepareIndex("test").setId(id).setSource("field", i));
        }

        indexRandom(true, docbuilders);
        ensureGreen();
        refresh();

        SearchResponse searchResponse;

        searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(numDocs))
            .setTerminateAfter(10)
            .setSize(size)
            .setTrackTotalHitsUpTo(5)
            .get();
        assertTrue(searchResponse.isTerminatedEarly());
        assertEquals(5, searchResponse.getHits().getTotalHits().value());
        assertEquals(GREATER_THAN_OR_EQUAL_TO, searchResponse.getHits().getTotalHits().relation());

        // For size = 0, the following queries terminate early, but hits and relation can vary.
        if (size > 0) {
            searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(numDocs))
                .setTerminateAfter(5)
                .setSize(size)
                .setTrackTotalHitsUpTo(10)
                .get();
            assertTrue(searchResponse.isTerminatedEarly());
            assertEquals(5, searchResponse.getHits().getTotalHits().value());
            assertEquals(EQUAL_TO, searchResponse.getHits().getTotalHits().relation());

            searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(numDocs))
                .setTerminateAfter(5)
                .setSize(size)
                .setTrackTotalHitsUpTo(5)
                .get();
            assertTrue(searchResponse.isTerminatedEarly());
            assertEquals(5, searchResponse.getHits().getTotalHits().value());
            assertEquals(EQUAL_TO, searchResponse.getHits().getTotalHits().relation());
        }

        searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(numDocs))
            .setTerminateAfter(5)
            .setSize(size)
            .setTrackTotalHits(true)
            .get();
        assertTrue(searchResponse.isTerminatedEarly());
        if (size == 0) {
            // Since terminate_after < track_total_hits, we need to do a range check.
            assertHitCount(searchResponse, 5, numDocs);
        } else {
            assertEquals(5, searchResponse.getHits().getTotalHits().value());
        }
        assertEquals(EQUAL_TO, searchResponse.getHits().getTotalHits().relation());

        searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(numDocs))
            .setTerminateAfter(numDocs * 2)
            .setSize(size)
            .setTrackTotalHits(true)
            .get();
        assertFalse(searchResponse.isTerminatedEarly());
        assertEquals(numDocs, searchResponse.getHits().getTotalHits().value());
        assertEquals(EQUAL_TO, searchResponse.getHits().getTotalHits().relation());

        searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.rangeQuery("field").gte(1).lte(numDocs))
            .setSize(size)
            .setTrackTotalHitsUpTo(5)
            .get();
        assertEquals(5, searchResponse.getHits().getTotalHits().value());
        assertEquals(GREATER_THAN_OR_EQUAL_TO, searchResponse.getHits().getTotalHits().relation());
    }

    public void testSimpleTerminateAfterTrackTotalHitsUpToRandomSize0() throws Exception {
        doTestSimpleTerminateAfterTrackTotalHitsUpTo(0);
    }

    public void testSimpleTerminateAfterTrackTotalHitsUpToSize() throws Exception {
        doTestSimpleTerminateAfterTrackTotalHitsUpTo(randomIntBetween(1, 29));
    }

    public void testSimpleIndexSortEarlyTerminate() throws Exception {
        prepareCreate("test").setSettings(
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).put("index.sort.field", "rank")
        ).setMapping("rank", "type=integer").get();
        ensureGreen();
        int max = randomIntBetween(3, 29);
        List<IndexRequestBuilder> docbuilders = new ArrayList<>(max);

        for (int i = max - 1; i >= 0; i--) {
            String id = String.valueOf(i);
            docbuilders.add(client().prepareIndex("test").setId(id).setSource("rank", i));
        }

        indexRandom(true, docbuilders);
        ensureGreen();
        refresh();

        SearchResponse searchResponse;
        for (int i = 1; i < max; i++) {
            searchResponse = client().prepareSearch("test")
                .addDocValueField("rank")
                .setTrackTotalHits(false)
                .addSort("rank", SortOrder.ASC)
                .setSize(i)
                .get();
            assertNull(searchResponse.getHits().getTotalHits());
            for (int j = 0; j < i; j++) {
                assertThat(searchResponse.getHits().getAt(j).field("rank").getValue(), equalTo((long) j));
            }
        }
    }

    public void testInsaneFromAndSize() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON));

        assertWindowFails(client().prepareSearch("idx").setFrom(Integer.MAX_VALUE));
        assertWindowFails(client().prepareSearch("idx").setSize(Integer.MAX_VALUE));
    }

    public void testTooLargeFromAndSize() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON));

        assertWindowFails(client().prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)));
        assertWindowFails(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) + 1));
        assertWindowFails(
            client().prepareSearch("idx")
                .setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
        );
    }

    public void testLargeFromAndSizeSucceeds() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON));

        assertHitCount(client().prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) - 10).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)).get(), 1);
        assertHitCount(
            client().prepareSearch("idx")
                .setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) / 2)
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) / 2 - 1)
                .get(),
            1
        );
    }

    public void testTooLargeFromAndSizeOkBySetting() throws Exception {
        prepareCreate("idx").setSettings(
            Settings.builder()
                .put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 2)
        ).get();
        indexRandom(true, client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON));

        assertHitCount(client().prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) + 1).get(), 1);
        assertHitCount(
            client().prepareSearch("idx")
                .setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
                .get(),
            1
        );
    }

    public void testTooLargeFromAndSizeOkByDynamicSetting() throws Exception {
        createIndex("idx");
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("idx")
                .setSettings(
                    Settings.builder()
                        .put(
                            IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(),
                            IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 2
                        )
                )
                .get()
        );
        indexRandom(true, client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON));

        assertHitCount(client().prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) + 1).get(), 1);
        assertHitCount(
            client().prepareSearch("idx")
                .setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY))
                .get(),
            1
        );
    }

    public void testTooLargeFromAndSizeBackwardsCompatibilityRecommendation() throws Exception {
        prepareCreate("idx").setSettings(Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), Integer.MAX_VALUE)).get();
        indexRandom(true, client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON));

        assertHitCount(client().prepareSearch("idx").setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 10).get(), 1);
        assertHitCount(client().prepareSearch("idx").setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 10).get(), 1);
        assertHitCount(
            client().prepareSearch("idx")
                .setSize(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 10)
                .setFrom(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY) * 10)
                .get(),
            1
        );
    }

    public void testTooLargeRescoreWindow() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON));

        assertRescoreWindowFails(Integer.MAX_VALUE);
        assertRescoreWindowFails(IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY) + 1);
    }

    public void testTooLargeRescoreOkBySetting() throws Exception {
        int defaultMaxWindow = IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY);
        prepareCreate("idx").setSettings(Settings.builder().put(IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey(), defaultMaxWindow * 2))
            .get();
        indexRandom(true, client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON));

        assertHitCount(
            client().prepareSearch("idx").addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(defaultMaxWindow + 1)).get(),
            1
        );
    }

    public void testTooLargeRescoreOkByResultWindowSetting() throws Exception {
        int defaultMaxWindow = IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY);
        prepareCreate("idx").setSettings(
            Settings.builder()
                .put(
                    IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), // Note that this is the RESULT window.
                    defaultMaxWindow * 2
                )
        ).get();
        indexRandom(true, client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON));

        assertHitCount(
            client().prepareSearch("idx").addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(defaultMaxWindow + 1)).get(),
            1
        );
    }

    public void testTooLargeRescoreOkByDynamicSetting() throws Exception {
        int defaultMaxWindow = IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY);
        createIndex("idx");
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("idx")
                .setSettings(Settings.builder().put(IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey(), defaultMaxWindow * 2))
                .get()
        );
        indexRandom(true, client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON));

        assertHitCount(
            client().prepareSearch("idx").addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(defaultMaxWindow + 1)).get(),
            1
        );
    }

    public void testTooLargeRescoreOkByDynamicResultWindowSetting() throws Exception {
        int defaultMaxWindow = IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY);
        createIndex("idx");
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("idx")
                .setSettings(
                    // Note that this is the RESULT window
                    Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), defaultMaxWindow * 2)
                )
                .get()
        );
        indexRandom(true, client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON));

        assertHitCount(
            client().prepareSearch("idx").addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(defaultMaxWindow + 1)).get(),
            1
        );
    }

    public void testQueryNumericFieldWithRegex() throws Exception {
        assertAcked(prepareCreate("idx").setMapping("num", "type=integer"));
        ensureGreen("idx");

        try {
            client().prepareSearch("idx").setQuery(QueryBuilders.regexpQuery("num", "34")).get();
            fail("SearchPhaseExecutionException should have been thrown");
        } catch (SearchPhaseExecutionException ex) {
            assertThat(ex.getRootCause().getMessage(), containsString("Can only use regexp queries on keyword and text fields"));
        }
    }

    public void testTermQueryBigInt() throws Exception {
        prepareCreate("idx").setMapping("field", "type=keyword").get();
        ensureGreen("idx");

        client().prepareIndex("idx")
            .setId("1")
            .setSource("{\"field\" : 80315953321748200608 }", MediaTypeRegistry.JSON)
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        String queryJson = "{ \"field\" : { \"value\" : 80315953321748200608 } }";
        XContentParser parser = createParser(JsonXContent.jsonXContent, queryJson);
        parser.nextToken();
        TermQueryBuilder query = TermQueryBuilder.fromXContent(parser);
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(query).get();
        assertEquals(1, searchResponse.getHits().getTotalHits().value());
    }

    public void testIndexOnlyFloatField() throws IOException {
        prepareCreate("idx").setMapping("field", "type=float,doc_values=false").get();
        ensureGreen("idx");

        IndexRequestBuilder indexRequestBuilder = client().prepareIndex("idx");

        for (float i = 9000.0F; i < 20000.0F; i++) {
            indexRequestBuilder.setId(String.valueOf(i)).setSource("{\"field\":" + i + "}", MediaTypeRegistry.JSON).get();
        }
        String queryJson = "{ \"filter\" : { \"terms\" : { \"field\" : [ 10000.0 ] } } }";
        XContentParser parser = createParser(JsonXContent.jsonXContent, queryJson);
        parser.nextToken();
        ConstantScoreQueryBuilder query = ConstantScoreQueryBuilder.fromXContent(parser);
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(query).get();
        assertEquals(1, searchResponse.getHits().getTotalHits().value());
    }

    public void testTooLongRegexInRegexpQuery() throws Exception {
        createIndex("idx");
        indexRandom(true, client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON));

        int defaultMaxRegexLength = IndexSettings.MAX_REGEX_LENGTH_SETTING.get(Settings.EMPTY);
        StringBuilder regexp = new StringBuilder(defaultMaxRegexLength);
        while (regexp.length() <= defaultMaxRegexLength) {
            regexp.append("]\\r\\\\]|\\\\.)*\\](?:(?:\\r\\n)?[\\t])*))*(?:,@(?:(?:\\r\\n)?[ \\t])*(?:[^()<>@,;:\\\\\".\\");
        }
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch("idx").setQuery(QueryBuilders.regexpQuery("num", regexp.toString())).get()
        );
        assertThat(
            e.getRootCause().getMessage(),
            containsString(
                "The length of regex ["
                    + regexp.length()
                    + "] used in the Regexp Query request has exceeded "
                    + "the allowed maximum of ["
                    + defaultMaxRegexLength
                    + "]. "
                    + "This maximum can be set by changing the ["
                    + IndexSettings.MAX_REGEX_LENGTH_SETTING.getKey()
                    + "] index level setting."
            )
        );
    }

    private void assertWindowFails(SearchRequestBuilder search) {
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () -> search.get());
        assertThat(
            e.toString(),
            containsString(
                "Result window is too large, from + size must be less than or equal to: ["
                    + IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY)
            )
        );
        assertThat(e.toString(), containsString("See the scroll api for a more efficient way to request large data sets"));
    }

    private void assertRescoreWindowFails(int windowSize) {
        SearchRequestBuilder search = client().prepareSearch("idx")
            .addRescorer(new QueryRescorerBuilder(matchAllQuery()).windowSize(windowSize));
        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () -> search.get());
        assertThat(
            e.toString(),
            containsString(
                "Rescore window ["
                    + windowSize
                    + "] is too large. It must "
                    + "be less than ["
                    + IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(Settings.EMPTY)
            )
        );
        assertThat(
            e.toString(),
            containsString(
                "This limit can be set by changing the [" + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey() + "] index level setting."
            )
        );
    }
}
