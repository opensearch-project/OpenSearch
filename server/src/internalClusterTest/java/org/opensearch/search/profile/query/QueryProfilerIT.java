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

package org.opensearch.search.profile.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.tests.util.English;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.ParameterizedDynamicSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.profile.query.RandomQueryGenerator.randomQueryBuilder;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class QueryProfilerIT extends ParameterizedDynamicSettingsOpenSearchIntegTestCase {
    private final boolean concurrentSearchEnabled;
    private static final String MAX_PREFIX = "max_";
    private static final String MIN_PREFIX = "min_";
    private static final String AVG_PREFIX = "avg_";
    private static final String TIMING_TYPE_COUNT_SUFFIX = "_count";

    public QueryProfilerIT(Settings settings, boolean concurrentSearchEnabled) {
        super(settings);
        this.concurrentSearchEnabled = concurrentSearchEnabled;
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build(), false },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build(), true }
        );
    }

    /**
     * This test simply checks to make sure nothing crashes.  Test indexes 100-150 documents,
     * constructs 20-100 random queries and tries to profile them
     */
    public void testProfileQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        List<String> stringFields = Arrays.asList("field1");
        List<String> numericFields = Arrays.asList("field2");

        indexRandom(true, docs);

        refresh();
        int iters = between(20, 100);
        for (int i = 0; i < iters; i++) {
            QueryBuilder q = randomQueryBuilder(stringFields, numericFields, numDocs, 3);
            logger.info("Query: {}", q);

            SearchResponse resp = client().prepareSearch()
                .setQuery(q)
                .setTrackTotalHits(true)
                .setProfile(true)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .get();

            assertNotNull("Profile response element should not be null", resp.getProfileResults());
            assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));
            for (Map.Entry<String, ProfileShardResult> shard : resp.getProfileResults().entrySet()) {
                assertThat(shard.getValue().getNetworkTime().getInboundNetworkTime(), greaterThanOrEqualTo(0L));
                assertThat(shard.getValue().getNetworkTime().getOutboundNetworkTime(), greaterThanOrEqualTo(0L));
                for (QueryProfileShardResult searchProfiles : shard.getValue().getQueryProfileResults()) {
                    for (ProfileResult result : searchProfiles.getQueryResults()) {
                        assertNotNull(result.getQueryName());
                        assertNotNull(result.getLuceneDescription());
                        assertThat(result.getTime(), greaterThan(0L));
                    }

                    CollectorResult result = searchProfiles.getCollectorResult();
                    assertThat(result.getName(), is(not(emptyOrNullString())));
                    assertThat(result.getTime(), greaterThan(0L));
                }
            }

        }
    }

    /**
     * This test generates a random query and executes a profiled and non-profiled
     * search for each query.  It then does some basic sanity checking of score and hits
     * to make sure the profiling doesn't interfere with the hits being returned
     */
    public void testProfileMatchesRegular() throws Exception {
        createIndex("test", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test")
                .setId(String.valueOf(i))
                .setSource("id", String.valueOf(i), "field1", English.intToEnglish(i), "field2", i);
        }

        List<String> stringFields = Arrays.asList("field1");
        List<String> numericFields = Arrays.asList("field2");

        indexRandom(true, docs);

        refresh();
        QueryBuilder q = randomQueryBuilder(stringFields, numericFields, numDocs, 3);
        logger.debug("Query: {}", q);

        SearchRequestBuilder vanilla = client().prepareSearch("test")
            .setQuery(q)
            .setProfile(false)
            .addSort("id.keyword", SortOrder.ASC)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setRequestCache(false);

        SearchRequestBuilder profile = client().prepareSearch("test")
            .setQuery(q)
            .setProfile(true)
            .addSort("id.keyword", SortOrder.ASC)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setRequestCache(false);

        MultiSearchResponse.Item[] responses = client().prepareMultiSearch().add(vanilla).add(profile).get().getResponses();

        SearchResponse vanillaResponse = responses[0].getResponse();
        SearchResponse profileResponse = responses[1].getResponse();

        assertThat(vanillaResponse.getFailedShards(), equalTo(0));
        assertThat(profileResponse.getFailedShards(), equalTo(0));
        assertThat(vanillaResponse.getSuccessfulShards(), equalTo(profileResponse.getSuccessfulShards()));

        float vanillaMaxScore = vanillaResponse.getHits().getMaxScore();
        float profileMaxScore = profileResponse.getHits().getMaxScore();
        if (Float.isNaN(vanillaMaxScore)) {
            assertTrue("Vanilla maxScore is NaN but Profile is not [" + profileMaxScore + "]", Float.isNaN(profileMaxScore));
        } else {
            assertEquals(
                "Profile maxScore of [" + profileMaxScore + "] is not close to Vanilla maxScore [" + vanillaMaxScore + "]",
                vanillaMaxScore,
                profileMaxScore,
                0.001
            );
        }

        if (vanillaResponse.getHits().getTotalHits().value != profileResponse.getHits().getTotalHits().value) {
            Set<SearchHit> vanillaSet = new HashSet<>(Arrays.asList(vanillaResponse.getHits().getHits()));
            Set<SearchHit> profileSet = new HashSet<>(Arrays.asList(profileResponse.getHits().getHits()));
            if (vanillaResponse.getHits().getTotalHits().value > profileResponse.getHits().getTotalHits().value) {
                vanillaSet.removeAll(profileSet);
                fail("Vanilla hits were larger than profile hits.  Non-overlapping elements were: " + vanillaSet.toString());
            } else {
                profileSet.removeAll(vanillaSet);
                fail("Profile hits were larger than vanilla hits.  Non-overlapping elements were: " + profileSet.toString());
            }
        }

        SearchHit[] vanillaHits = vanillaResponse.getHits().getHits();
        SearchHit[] profileHits = profileResponse.getHits().getHits();

        for (int j = 0; j < vanillaHits.length; j++) {
            assertThat("Profile hit #" + j + " has a different ID from Vanilla", vanillaHits[j].getId(), equalTo(profileHits[j].getId()));
        }

    }

    /**
     * This test verifies that the output is reasonable for a simple, non-nested query
     */
    public void testSimpleMatch() throws Exception {
        createIndex("test");
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);
        ensureGreen();

        QueryBuilder q = QueryBuilders.matchQuery("field1", "one");

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).get();

        Map<String, ProfileShardResult> p = resp.getProfileResults();
        assertNotNull(p);
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertEquals(result.getQueryName(), "TermQuery");
                    assertEquals(result.getLuceneDescription(), "field1:one");
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                    assertQueryProfileResult(result);
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), is(not(emptyOrNullString())));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
    }

    /**
     * This test verifies that the output is reasonable for a nested query
     */
    public void testBool() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);

        QueryBuilder q = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("field1", "one"))
            .must(QueryBuilders.matchQuery("field1", "two"));

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).get();

        Map<String, ProfileShardResult> p = resp.getProfileResults();
        assertNotNull(p);
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertEquals(result.getQueryName(), "BooleanQuery");
                    assertEquals(result.getLuceneDescription(), "+field1:one +field1:two");
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                    assertEquals(result.getProfiledChildren().size(), 2);
                    assertQueryProfileResult(result);

                    // Check the children
                    List<ProfileResult> children = result.getProfiledChildren();
                    assertEquals(children.size(), 2);

                    ProfileResult childProfile = children.get(0);
                    assertEquals(childProfile.getQueryName(), "TermQuery");
                    assertEquals(childProfile.getLuceneDescription(), "field1:one");
                    assertThat(childProfile.getTime(), greaterThan(0L));
                    assertNotNull(childProfile.getTimeBreakdown());
                    assertEquals(childProfile.getProfiledChildren().size(), 0);
                    assertQueryProfileResult(childProfile);

                    childProfile = children.get(1);
                    assertEquals(childProfile.getQueryName(), "TermQuery");
                    assertEquals(childProfile.getLuceneDescription(), "field1:two");
                    assertThat(childProfile.getTime(), greaterThan(0L));
                    assertNotNull(childProfile.getTimeBreakdown());
                    assertQueryProfileResult(childProfile);
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), is(not(emptyOrNullString())));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }

    }

    /**
     * Tests a boolean query with no children clauses
     */
    public void testEmptyBool() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.boolQuery();
        logger.info("Query: {}", q);

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).get();

        assertNotNull("Profile response element should not be null", resp.getProfileResults());
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertNotNull(result.getQueryName());
                    assertNotNull(result.getLuceneDescription());
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                    assertQueryProfileResult(result);
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), is(not(emptyOrNullString())));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
    }

    /**
     * Tests a series of three nested boolean queries with a single "leaf" match query.
     * The rewrite process will "collapse" this down to a single bool, so this tests to make sure
     * nothing catastrophic happens during that fairly substantial rewrite
     */
    public void testCollapsingBool() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.boolQuery()
            .must(QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("field1", "one"))));

        logger.info("Query: {}", q);

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).get();

        assertNotNull("Profile response element should not be null", resp.getProfileResults());
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertNotNull(result.getQueryName());
                    assertNotNull(result.getLuceneDescription());
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                    assertQueryProfileResult(result);
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), is(not(emptyOrNullString())));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
    }

    public void testBoosting() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.boostingQuery(QueryBuilders.matchQuery("field1", "one"), QueryBuilders.matchQuery("field1", "two"))
            .boost(randomFloat())
            .negativeBoost(randomFloat());
        logger.info("Query: {}", q);

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).get();

        assertNotNull("Profile response element should not be null", resp.getProfileResults());
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertNotNull(result.getQueryName());
                    assertNotNull(result.getLuceneDescription());
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                    assertQueryProfileResult(result);
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), is(not(emptyOrNullString())));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
    }

    public void testSearchLeafForItsLeavesAndRewriteQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = 122;
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        List<String> terms = Arrays.asList("zero", "zero", "one");

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.boostingQuery(
            QueryBuilders.idsQuery().addIds(String.valueOf(randomInt()), String.valueOf(randomInt())),
            QueryBuilders.termsQuery("field1", terms)
        ).boost(randomFloat()).negativeBoost(randomFloat());
        logger.info("Query: {}", q);

        SearchResponse resp = client().prepareSearch()
            .setQuery(q)
            .setTrackTotalHits(true)
            .setProfile(true)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .get();

        assertNotNull("Profile response element should not be null", resp.getProfileResults());
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            assertThat(shardResult.getValue().getNetworkTime().getInboundNetworkTime(), greaterThanOrEqualTo(0L));
            assertThat(shardResult.getValue().getNetworkTime().getOutboundNetworkTime(), greaterThanOrEqualTo(0L));
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                List<ProfileResult> results = searchProfiles.getQueryResults();
                for (ProfileResult result : results) {
                    assertNotNull(result.getQueryName());
                    assertNotNull(result.getLuceneDescription());
                    assertThat(result.getTime(), greaterThan(0L));
                    Map<String, Long> breakdown = result.getTimeBreakdown();
                    Long maxSliceTime = result.getMaxSliceTime();
                    Long minSliceTime = result.getMinSliceTime();
                    Long avgSliceTime = result.getAvgSliceTime();
                    if (concurrentSearchEnabled && results.get(0).equals(result)) {
                        assertNotNull(maxSliceTime);
                        assertNotNull(minSliceTime);
                        assertNotNull(avgSliceTime);
                        assertThat(breakdown.size(), equalTo(66));
                        for (QueryTimingType queryTimingType : QueryTimingType.values()) {
                            if (queryTimingType != QueryTimingType.CREATE_WEIGHT) {
                                String maxTimingType = MAX_PREFIX + queryTimingType;
                                String minTimingType = MIN_PREFIX + queryTimingType;
                                String avgTimingType = AVG_PREFIX + queryTimingType;
                                assertNotNull(breakdown.get(maxTimingType));
                                assertNotNull(breakdown.get(minTimingType));
                                assertNotNull(breakdown.get(avgTimingType));
                                assertNotNull(breakdown.get(maxTimingType + TIMING_TYPE_COUNT_SUFFIX));
                                assertNotNull(breakdown.get(minTimingType + TIMING_TYPE_COUNT_SUFFIX));
                                assertNotNull(breakdown.get(avgTimingType + TIMING_TYPE_COUNT_SUFFIX));
                            }
                        }
                    } else if (concurrentSearchEnabled) {
                        assertThat(maxSliceTime, equalTo(0L));
                        assertThat(minSliceTime, equalTo(0L));
                        assertThat(avgSliceTime, equalTo(0L));
                        assertThat(breakdown.size(), equalTo(27));
                    } else {
                        assertThat(maxSliceTime, is(nullValue()));
                        assertThat(minSliceTime, is(nullValue()));
                        assertThat(avgSliceTime, is(nullValue()));
                        assertThat(breakdown.size(), equalTo(27));
                    }
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), is(not(emptyOrNullString())));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
    }

    public void testDisMaxRange() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.disMaxQuery()
            .boost(0.33703882f)
            .add(QueryBuilders.rangeQuery("field2").from(null).to(73).includeLower(true).includeUpper(true));
        logger.info("Query: {}", q);

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).get();

        assertNotNull("Profile response element should not be null", resp.getProfileResults());
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertNotNull(result.getQueryName());
                    assertNotNull(result.getLuceneDescription());
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                    assertQueryProfileResult(result);
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), is(not(emptyOrNullString())));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
    }

    public void testRange() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.rangeQuery("field2").from(0).to(5);

        logger.info("Query: {}", q.toString());

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).get();

        assertNotNull("Profile response element should not be null", resp.getProfileResults());
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertNotNull(result.getQueryName());
                    assertNotNull(result.getLuceneDescription());
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                    assertQueryProfileResult(result);
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), is(not(emptyOrNullString())));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
    }

    public void testPhrase() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test")
                .setId(String.valueOf(i))
                .setSource("field1", English.intToEnglish(i) + " " + English.intToEnglish(i + 1), "field2", i);
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.matchPhraseQuery("field1", "one two");

        logger.info("Query: {}", q);

        SearchResponse resp = client().prepareSearch()
            .setQuery(q)
            .setIndices("test")
            .setProfile(true)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .get();

        if (resp.getShardFailures().length > 0) {
            for (ShardSearchFailure f : resp.getShardFailures()) {
                logger.error("Shard search failure: {}", f);
            }
            fail();
        }

        assertNotNull("Profile response element should not be null", resp.getProfileResults());
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertNotNull(result.getQueryName());
                    assertNotNull(result.getLuceneDescription());
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                    assertQueryProfileResult(result);
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), is(not(emptyOrNullString())));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
    }

    /**
     * This test makes sure no profile results are returned when profiling is disabled
     */
    public void testNoProfile() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);
        refresh();
        QueryBuilder q = QueryBuilders.rangeQuery("field2").from(0).to(5);

        logger.info("Query: {}", q);

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(false).get();
        assertThat("Profile response element should be an empty map", resp.getProfileResults().size(), equalTo(0));
    }

    private void assertQueryProfileResult(ProfileResult result) {
        Map<String, Long> breakdown = result.getTimeBreakdown();
        Long maxSliceTime = result.getMaxSliceTime();
        Long minSliceTime = result.getMinSliceTime();
        Long avgSliceTime = result.getAvgSliceTime();
        if (concurrentSearchEnabled) {
            assertNotNull(maxSliceTime);
            assertNotNull(minSliceTime);
            assertNotNull(avgSliceTime);
            assertThat(breakdown.size(), equalTo(66));
            for (QueryTimingType queryTimingType : QueryTimingType.values()) {
                if (queryTimingType != QueryTimingType.CREATE_WEIGHT) {
                    String maxTimingType = MAX_PREFIX + queryTimingType;
                    String minTimingType = MIN_PREFIX + queryTimingType;
                    String avgTimingType = AVG_PREFIX + queryTimingType;
                    assertNotNull(breakdown.get(maxTimingType));
                    assertNotNull(breakdown.get(minTimingType));
                    assertNotNull(breakdown.get(avgTimingType));
                    assertNotNull(breakdown.get(maxTimingType + TIMING_TYPE_COUNT_SUFFIX));
                    assertNotNull(breakdown.get(minTimingType + TIMING_TYPE_COUNT_SUFFIX));
                    assertNotNull(breakdown.get(avgTimingType + TIMING_TYPE_COUNT_SUFFIX));
                }
            }
        } else {
            assertThat(maxSliceTime, is(nullValue()));
            assertThat(minSliceTime, is(nullValue()));
            assertThat(avgSliceTime, is(nullValue()));
            assertThat(breakdown.size(), equalTo(27));
        }
    }

}
