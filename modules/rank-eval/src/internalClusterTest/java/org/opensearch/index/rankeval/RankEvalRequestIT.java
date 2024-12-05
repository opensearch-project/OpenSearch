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

package org.opensearch.index.rankeval;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.rankeval.PrecisionAtK.Detail;
import org.opensearch.indices.IndexClosedException;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static org.opensearch.index.rankeval.EvaluationMetric.filterUnratedDocuments;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.instanceOf;

public class RankEvalRequestIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final String TEST_INDEX = "test";
    private static final String INDEX_ALIAS = "alias0";
    private static final int RELEVANT_RATING_1 = 1;

    public RankEvalRequestIT(Settings staticSettings) {
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
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(RankEvalModulePlugin.class);
    }

    @Before
    public void setup() {
        createIndex(TEST_INDEX);
        ensureGreen();

        client().prepareIndex(TEST_INDEX)
            .setId("1")
            .setSource("id", 1, "text", "berlin", "title", "Berlin, Germany", "population", 3670622)
            .get();
        client().prepareIndex(TEST_INDEX).setId("2").setSource("id", 2, "text", "amsterdam", "population", 851573).get();
        client().prepareIndex(TEST_INDEX).setId("3").setSource("id", 3, "text", "amsterdam", "population", 851573).get();
        client().prepareIndex(TEST_INDEX).setId("4").setSource("id", 4, "text", "amsterdam", "population", 851573).get();
        client().prepareIndex(TEST_INDEX).setId("5").setSource("id", 5, "text", "amsterdam", "population", 851573).get();
        client().prepareIndex(TEST_INDEX).setId("6").setSource("id", 6, "text", "amsterdam", "population", 851573).get();

        // add another index for testing closed indices etc...
        client().prepareIndex("test2").setId("7").setSource("id", 7, "text", "amsterdam", "population", 851573).get();
        refresh();

        // set up an alias that can also be used in tests
        assertAcked(client().admin().indices().prepareAliases().addAliasAction(AliasActions.add().index(TEST_INDEX).alias(INDEX_ALIAS)));
    }

    /**
     * Test cases retrieves all six documents indexed above. The first part checks the Prec@10 calculation where
     * all unlabeled docs are treated as "unrelevant". We average Prec@ metric across two search use cases, the
     * first one that labels 4 out of the 6 documents as relevant, the second one with only one relevant document.
     */
    public void testPrecisionAtRequest() {
        List<RatedRequest> specifications = new ArrayList<>();
        SearchSourceBuilder testQuery = new SearchSourceBuilder();
        testQuery.query(new MatchAllQueryBuilder());
        testQuery.sort("id");
        RatedRequest amsterdamRequest = new RatedRequest("amsterdam_query", createRelevant("2", "3", "4", "5"), testQuery);
        amsterdamRequest.addSummaryFields(Arrays.asList(new String[] { "text", "title" }));

        specifications.add(amsterdamRequest);
        RatedRequest berlinRequest = new RatedRequest("berlin_query", createRelevant("1"), testQuery);
        berlinRequest.addSummaryFields(Arrays.asList(new String[] { "text", "title" }));
        specifications.add(berlinRequest);

        PrecisionAtK metric = new PrecisionAtK(1, false, 10);
        RankEvalSpec task = new RankEvalSpec(specifications, metric);

        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(client(), RankEvalAction.INSTANCE, new RankEvalRequest());
        builder.setRankEvalSpec(task);

        String indexToUse = randomBoolean() ? TEST_INDEX : INDEX_ALIAS;
        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request().indices(indexToUse)).actionGet();
        // the expected Prec@ for the first query is 4/6 and the expected Prec@ for the
        // second is 1/6, divided by 2 to get the average
        double expectedPrecision = (1.0 / 6.0 + 4.0 / 6.0) / 2.0;
        assertEquals(expectedPrecision, response.getMetricScore(), Double.MIN_VALUE);
        Set<Entry<String, EvalQueryQuality>> entrySet = response.getPartialResults().entrySet();
        assertEquals(2, entrySet.size());
        for (Entry<String, EvalQueryQuality> entry : entrySet) {
            EvalQueryQuality quality = entry.getValue();
            if (entry.getKey() == "amsterdam_query") {
                assertEquals(2, filterUnratedDocuments(quality.getHitsAndRatings()).size());
                List<RatedSearchHit> hitsAndRatings = quality.getHitsAndRatings();
                assertEquals(6, hitsAndRatings.size());
                for (RatedSearchHit hit : hitsAndRatings) {
                    String id = hit.getSearchHit().getId();
                    if (id.equals("1") || id.equals("6")) {
                        assertFalse(hit.getRating().isPresent());
                    } else {
                        assertEquals(RELEVANT_RATING_1, hit.getRating().getAsInt());
                    }
                }
            }
            if (entry.getKey() == "berlin_query") {
                assertEquals(5, filterUnratedDocuments(quality.getHitsAndRatings()).size());
                List<RatedSearchHit> hitsAndRatings = quality.getHitsAndRatings();
                assertEquals(6, hitsAndRatings.size());
                for (RatedSearchHit hit : hitsAndRatings) {
                    String id = hit.getSearchHit().getId();
                    if (id.equals("1")) {
                        assertEquals(RELEVANT_RATING_1, hit.getRating().getAsInt());
                    } else {
                        assertFalse(hit.getRating().isPresent());
                    }
                }
            }
        }

        // test that a different window size k affects the result
        metric = new PrecisionAtK(1, false, 3);
        task = new RankEvalSpec(specifications, metric);

        builder = new RankEvalRequestBuilder(client(), RankEvalAction.INSTANCE, new RankEvalRequest(task, new String[] { TEST_INDEX }));

        response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        // if we look only at top 3 documente, the expected P@3 for the first query is
        // 2/3 and the expected Prec@ for the second is 1/3, divided by 2 to get the average
        expectedPrecision = (1.0 / 3.0 + 2.0 / 3.0) / 2.0;
        assertEquals(expectedPrecision, response.getMetricScore(), Double.MIN_VALUE);
    }

    /**
     * This test assumes we are using the same ratings as in {@link DiscountedCumulativeGainTests#testDCGAt()}.
     * See details in that test case for how the expected values are calculated
     */
    public void testDCGRequest() {
        SearchSourceBuilder testQuery = new SearchSourceBuilder();
        testQuery.query(new MatchAllQueryBuilder());
        testQuery.sort("id");

        List<RatedRequest> specifications = new ArrayList<>();
        List<RatedDocument> ratedDocs = Arrays.asList(
            new RatedDocument(TEST_INDEX, "1", 3),
            new RatedDocument(TEST_INDEX, "2", 2),
            new RatedDocument(TEST_INDEX, "3", 3),
            new RatedDocument(TEST_INDEX, "4", 0),
            new RatedDocument(TEST_INDEX, "5", 1),
            new RatedDocument(TEST_INDEX, "6", 2)
        );
        specifications.add(new RatedRequest("amsterdam_query", ratedDocs, testQuery));

        DiscountedCumulativeGain metric = new DiscountedCumulativeGain(false, null, 10);
        RankEvalSpec task = new RankEvalSpec(specifications, metric);

        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(
            client(),
            RankEvalAction.INSTANCE,
            new RankEvalRequest(task, new String[] { TEST_INDEX })
        );

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        assertEquals(DiscountedCumulativeGainTests.EXPECTED_DCG, response.getMetricScore(), 10E-14);

        // test that a different window size k affects the result
        metric = new DiscountedCumulativeGain(false, null, 3);
        task = new RankEvalSpec(specifications, metric);

        builder = new RankEvalRequestBuilder(client(), RankEvalAction.INSTANCE, new RankEvalRequest(task, new String[] { TEST_INDEX }));

        response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        assertEquals(12.39278926071437, response.getMetricScore(), 10E-14);
    }

    public void testMRRRequest() {
        SearchSourceBuilder testQuery = new SearchSourceBuilder();
        testQuery.query(new MatchAllQueryBuilder());
        testQuery.sort("id");

        List<RatedRequest> specifications = new ArrayList<>();
        specifications.add(new RatedRequest("amsterdam_query", createRelevant("5"), testQuery));
        specifications.add(new RatedRequest("berlin_query", createRelevant("1"), testQuery));

        MeanReciprocalRank metric = new MeanReciprocalRank(1, 10);
        RankEvalSpec task = new RankEvalSpec(specifications, metric);

        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(
            client(),
            RankEvalAction.INSTANCE,
            new RankEvalRequest(task, new String[] { TEST_INDEX })
        );

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        // the expected reciprocal rank for the amsterdam_query is 1/5
        // the expected reciprocal rank for the berlin_query is 1/1
        // dividing by 2 to get the average
        double expectedMRR = (1.0 + 1.0 / 5.0) / 2.0;
        assertEquals(expectedMRR, response.getMetricScore(), 0.0);

        // test that a different window size k affects the result
        metric = new MeanReciprocalRank(1, 3);
        task = new RankEvalSpec(specifications, metric);

        builder = new RankEvalRequestBuilder(client(), RankEvalAction.INSTANCE, new RankEvalRequest(task, new String[] { TEST_INDEX }));

        response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        // limiting to top 3 results, the amsterdam_query has no relevant document in it
        // the reciprocal rank for the berlin_query is 1/1
        // dividing by 2 to get the average
        expectedMRR = 1.0 / 2.0;
        assertEquals(expectedMRR, response.getMetricScore(), 0.0);
    }

    /**
     * test that running a bad query (e.g. one that will target a non existing
     * field) will produce an error in the response
     */
    public void testBadQuery() {
        List<RatedRequest> specifications = new ArrayList<>();
        SearchSourceBuilder amsterdamQuery = new SearchSourceBuilder();
        amsterdamQuery.query(new MatchAllQueryBuilder());
        RatedRequest amsterdamRequest = new RatedRequest("amsterdam_query", createRelevant("2", "3", "4", "5"), amsterdamQuery);
        specifications.add(amsterdamRequest);

        SearchSourceBuilder brokenQuery = new SearchSourceBuilder();
        brokenQuery.query(QueryBuilders.termQuery("population", "noStringOnNumericFields"));
        RatedRequest brokenRequest = new RatedRequest("broken_query", createRelevant("1"), brokenQuery);
        specifications.add(brokenRequest);

        RankEvalSpec task = new RankEvalSpec(specifications, new PrecisionAtK());

        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(
            client(),
            RankEvalAction.INSTANCE,
            new RankEvalRequest(task, new String[] { TEST_INDEX })
        );
        builder.setRankEvalSpec(task);

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        assertEquals(1, response.getFailures().size());
        OpenSearchException[] rootCauses = OpenSearchException.guessRootCauses(response.getFailures().get("broken_query"));
        assertEquals("java.lang.NumberFormatException: For input string: \"noStringOnNumericFields\"", rootCauses[0].getCause().toString());
    }

    /**
     * test that multiple indices work, setting indices Options is possible and works as expected
     */
    public void testIndicesOptions() {
        SearchSourceBuilder amsterdamQuery = new SearchSourceBuilder().query(new MatchAllQueryBuilder());
        List<RatedDocument> relevantDocs = createRelevant("2", "3", "4", "5", "6");
        relevantDocs.add(new RatedDocument("test2", "7", RELEVANT_RATING_1));
        List<RatedRequest> specifications = new ArrayList<>();
        specifications.add(new RatedRequest("amsterdam_query", relevantDocs, amsterdamQuery));
        RankEvalSpec task = new RankEvalSpec(specifications, new PrecisionAtK());

        RankEvalRequest request = new RankEvalRequest(task, new String[] { TEST_INDEX, "test2" });
        request.setRankEvalSpec(task);

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, request).actionGet();
        Detail details = (PrecisionAtK.Detail) response.getPartialResults().get("amsterdam_query").getMetricDetails();
        assertEquals(7, details.getRetrieved());
        assertEquals(6, details.getRelevantRetrieved());

        // test that ignore_unavailable=true works but returns one result less
        assertTrue(client().admin().indices().prepareClose("test2").get().isAcknowledged());

        request.indicesOptions(IndicesOptions.fromParameters(null, "true", null, "false", SearchRequest.DEFAULT_INDICES_OPTIONS));
        response = client().execute(RankEvalAction.INSTANCE, request).actionGet();
        details = (PrecisionAtK.Detail) response.getPartialResults().get("amsterdam_query").getMetricDetails();
        assertEquals(6, details.getRetrieved());
        assertEquals(5, details.getRelevantRetrieved());

        // test that ignore_unavailable=false or default settings throw an IndexClosedException
        assertTrue(client().admin().indices().prepareClose("test2").get().isAcknowledged());
        request.indicesOptions(IndicesOptions.fromParameters(null, "false", null, "false", SearchRequest.DEFAULT_INDICES_OPTIONS));
        response = client().execute(RankEvalAction.INSTANCE, request).actionGet();
        assertEquals(1, response.getFailures().size());
        assertThat(response.getFailures().get("amsterdam_query"), instanceOf(IndexClosedException.class));

        // test expand_wildcards
        request = new RankEvalRequest(task, new String[] { "tes*" });
        request.indicesOptions(IndicesOptions.fromParameters("none", null, null, "false", SearchRequest.DEFAULT_INDICES_OPTIONS));
        response = client().execute(RankEvalAction.INSTANCE, request).actionGet();
        details = (PrecisionAtK.Detail) response.getPartialResults().get("amsterdam_query").getMetricDetails();
        assertEquals(0, details.getRetrieved());

        request.indicesOptions(IndicesOptions.fromParameters("open", null, null, "false", SearchRequest.DEFAULT_INDICES_OPTIONS));
        response = client().execute(RankEvalAction.INSTANCE, request).actionGet();
        details = (PrecisionAtK.Detail) response.getPartialResults().get("amsterdam_query").getMetricDetails();
        assertEquals(6, details.getRetrieved());
        assertEquals(5, details.getRelevantRetrieved());

        request.indicesOptions(IndicesOptions.fromParameters("closed", null, null, "false", SearchRequest.DEFAULT_INDICES_OPTIONS));
        response = client().execute(RankEvalAction.INSTANCE, request).actionGet();
        assertEquals(1, response.getFailures().size());
        assertThat(response.getFailures().get("amsterdam_query"), instanceOf(IllegalArgumentException.class));

        // test allow_no_indices
        request = new RankEvalRequest(task, new String[] { "bad*" });
        request.indicesOptions(IndicesOptions.fromParameters(null, null, "true", "false", SearchRequest.DEFAULT_INDICES_OPTIONS));
        response = client().execute(RankEvalAction.INSTANCE, request).actionGet();
        details = (PrecisionAtK.Detail) response.getPartialResults().get("amsterdam_query").getMetricDetails();
        assertEquals(0, details.getRetrieved());

        request.indicesOptions(IndicesOptions.fromParameters(null, null, "false", "false", SearchRequest.DEFAULT_INDICES_OPTIONS));
        response = client().execute(RankEvalAction.INSTANCE, request).actionGet();
        assertEquals(1, response.getFailures().size());
        assertThat(response.getFailures().get("amsterdam_query"), instanceOf(IndexNotFoundException.class));
    }

    private static List<RatedDocument> createRelevant(String... docs) {
        List<RatedDocument> relevant = new ArrayList<>();
        for (String doc : docs) {
            relevant.add(new RatedDocument("test", doc, RELEVANT_RATING_1));
        }
        return relevant;
    }
}
