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

package org.opensearch.search.functionscore;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.Version;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.test.VersionUtils;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.constantScoreQuery;
import static org.opensearch.index.query.QueryBuilders.functionScoreQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.exponentialDecayFunction;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.gaussDecayFunction;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.linearDecayFunction;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.builder.SearchSourceBuilder.searchSource;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertOrderedSearchHits;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHits;
import static org.opensearch.transport.client.Requests.indexRequest;
import static org.opensearch.transport.client.Requests.searchRequest;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class DecayFunctionScoreIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public DecayFunctionScoreIT(Settings staticSettings) {
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
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    private final QueryBuilder baseQuery = constantScoreQuery(termQuery("test", "value"));

    public void testDistanceScoreGeoLinGaussExp() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("loc")
                    .field("type", "geo_point")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        indexBuilders.add(
            client().prepareIndex()
                .setId("1")
                .setIndex("test")
                .setSource(
                    jsonBuilder().startObject()
                        .field("test", "value")
                        .startObject("loc")
                        .field("lat", 10)
                        .field("lon", 20)
                        .endObject()
                        .endObject()
                )
        );
        indexBuilders.add(
            client().prepareIndex()
                .setId("2")
                .setIndex("test")
                .setSource(
                    jsonBuilder().startObject()
                        .field("test", "value")
                        .startObject("loc")
                        .field("lat", 11)
                        .field("lon", 22)
                        .endObject()
                        .endObject()
                )
        );

        int numDummyDocs = 20;
        for (int i = 1; i <= numDummyDocs; i++) {
            indexBuilders.add(
                client().prepareIndex()
                    .setId(Integer.toString(i + 3))
                    .setIndex("test")
                    .setSource(
                        jsonBuilder().startObject()
                            .field("test", "value")
                            .startObject("loc")
                            .field("lat", 11 + i)
                            .field("lon", 22 + i)
                            .endObject()
                            .endObject()
                    )
            );
        }

        indexRandom(true, indexBuilders);

        // Test Gauss
        List<Float> lonlat = new ArrayList<>();
        lonlat.add(20f);
        lonlat.add(11f);

        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(searchSource().query(baseQuery))
        );
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (numDummyDocs + 2)));

        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().query(functionScoreQuery(baseQuery, gaussDecayFunction("loc", lonlat, "1000km"))))
        );
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (numDummyDocs + 2)));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        // Test Exp

        response = client().search(searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(searchSource().query(baseQuery)));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (numDummyDocs + 2)));

        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().query(functionScoreQuery(baseQuery, linearDecayFunction("loc", lonlat, "1000km"))))
        );
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (numDummyDocs + 2)));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        // Test Lin

        response = client().search(searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(searchSource().query(baseQuery)));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (numDummyDocs + 2)));

        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().query(functionScoreQuery(baseQuery, exponentialDecayFunction("loc", lonlat, "1000km"))))
        );
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (numDummyDocs + 2)));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
    }

    public void testDistanceScoreGeoLinGaussExpWithOffset() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num")
                    .field("type", "double")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        // add tw docs within offset
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        indexBuilders.add(
            client().prepareIndex()
                .setId("1")
                .setIndex("test")
                .setSource(jsonBuilder().startObject().field("test", "value").field("num", 0.5).endObject())
        );
        indexBuilders.add(
            client().prepareIndex()
                .setId("2")
                .setIndex("test")
                .setSource(jsonBuilder().startObject().field("test", "value").field("num", 1.7).endObject())
        );

        // add docs outside offset
        int numDummyDocs = 20;
        for (int i = 0; i < numDummyDocs; i++) {
            indexBuilders.add(
                client().prepareIndex()
                    .setId(Integer.toString(i + 3))
                    .setIndex("test")
                    .setSource(jsonBuilder().startObject().field("test", "value").field("num", 3.0 + i).endObject())
            );
        }

        indexRandom(true, indexBuilders);

        // Test Gauss

        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().size(numDummyDocs + 2)
                        .query(
                            functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 1.0, 5.0, 1.0)).boostMode(
                                CombineFunction.REPLACE
                            )
                        )
                )
        );
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (numDummyDocs + 2)));
        assertThat(sh.getAt(0).getId(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(sh.getAt(1).getId(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(sh.getAt(1).getScore(), equalTo(sh.getAt(0).getScore()));
        for (int i = 0; i < numDummyDocs; i++) {
            assertThat(sh.getAt(i + 2).getId(), equalTo(Integer.toString(i + 3)));
        }

        // Test Exp

        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().size(numDummyDocs + 2)
                        .query(
                            functionScoreQuery(termQuery("test", "value"), exponentialDecayFunction("num", 1.0, 5.0, 1.0)).boostMode(
                                CombineFunction.REPLACE
                            )
                        )
                )
        );
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (numDummyDocs + 2)));
        assertThat(sh.getAt(0).getId(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(sh.getAt(1).getId(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(sh.getAt(1).getScore(), equalTo(sh.getAt(0).getScore()));
        for (int i = 0; i < numDummyDocs; i++) {
            assertThat(sh.getAt(i + 2).getId(), equalTo(Integer.toString(i + 3)));
        }
        // Test Lin
        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().size(numDummyDocs + 2)
                        .query(
                            functionScoreQuery(termQuery("test", "value"), linearDecayFunction("num", 1.0, 20.0, 1.0)).boostMode(
                                CombineFunction.REPLACE
                            )
                        )
                )
        );
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (numDummyDocs + 2)));
        assertThat(sh.getAt(0).getId(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(sh.getAt(1).getId(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(sh.getAt(1).getScore(), equalTo(sh.getAt(0).getScore()));
    }

    public void testBoostModeSettingWorks() throws Exception {
        Settings settings = Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1).build();
        assertAcked(
            prepareCreate("test").setSettings(settings)
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("test")
                        .field("type", "text")
                        .endObject()
                        .startObject("loc")
                        .field("type", "geo_point")
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        indexBuilders.add(
            client().prepareIndex()
                .setId("1")
                .setIndex("test")
                .setSource(
                    jsonBuilder().startObject()
                        .field("test", "value value")
                        .startObject("loc")
                        .field("lat", 11)
                        .field("lon", 21)
                        .endObject()
                        .endObject()
                )
        );
        indexBuilders.add(
            client().prepareIndex()
                .setId("2")
                .setIndex("test")
                .setSource(
                    jsonBuilder().startObject()
                        .field("test", "value")
                        .startObject("loc")
                        .field("lat", 11)
                        .field("lon", 20)
                        .endObject()
                        .endObject()
                )
        );
        indexRandom(true, false, indexBuilders); // force no dummy docs
        indexRandomForConcurrentSearch("test");

        // Test Gauss
        List<Float> lonlat = new ArrayList<>();
        lonlat.add(20f);
        lonlat.add(11f);

        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("loc", lonlat, "1000km")).boostMode(
                            CombineFunction.MULTIPLY
                        )
                    )
                )
        );
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (2)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));

        // Test Exp
        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(searchSource().query(termQuery("test", "value")))
        );
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (2)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));

        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("loc", lonlat, "1000km")).boostMode(
                            CombineFunction.REPLACE
                        )
                    )
                )
        );
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (2)));
        assertThat(sh.getAt(0).getId(), equalTo("2"));
        assertThat(sh.getAt(1).getId(), equalTo("1"));

    }

    public void testParseGeoPoint() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("loc")
                    .field("type", "geo_point")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        client().prepareIndex()
            .setId("1")
            .setIndex("test")
            .setSource(
                jsonBuilder().startObject()
                    .field("test", "value")
                    .startObject("loc")
                    .field("lat", 20)
                    .field("lon", 11)
                    .endObject()
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();
        FunctionScoreQueryBuilder baseQuery = functionScoreQuery(
            constantScoreQuery(termQuery("test", "value")),
            ScoreFunctionBuilders.weightFactorFunction(randomIntBetween(1, 10))
        );
        indexRandomForConcurrentSearch("test");
        GeoPoint point = new GeoPoint(20, 11);
        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(baseQuery, gaussDecayFunction("loc", point, "1000km")).boostMode(CombineFunction.REPLACE)
                    )
                )
        );
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).getScore(), closeTo(1.0, 1.e-5));
        // this is equivalent to new GeoPoint(20, 11); just flipped so scores must be same
        float[] coords = { 11, 20 };
        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(baseQuery, gaussDecayFunction("loc", coords, "1000km")).boostMode(CombineFunction.REPLACE)
                    )
                )
        );
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).getScore(), closeTo(1.0f, 1.e-5));
    }

    public void testCombineModes() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num")
                    .field("type", "double")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        client().prepareIndex()
            .setId("1")
            .setIndex("test")
            .setRefreshPolicy(IMMEDIATE)
            .setSource(jsonBuilder().startObject().field("test", "value value").field("num", 1.0).endObject())
            .get();
        indexRandomForConcurrentSearch("test");
        FunctionScoreQueryBuilder baseQuery = functionScoreQuery(
            constantScoreQuery(termQuery("test", "value")),
            ScoreFunctionBuilders.weightFactorFunction(2)
        );
        // decay score should return 0.5 for this function and baseQuery should return 2.0f as it's score
        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(baseQuery, gaussDecayFunction("num", 0.0, 1.0, null, 0.5)).boostMode(CombineFunction.MULTIPLY)
                    )
                )
        );
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).getScore(), closeTo(1.0, 1.e-5));

        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(baseQuery, gaussDecayFunction("num", 0.0, 1.0, null, 0.5)).boostMode(CombineFunction.REPLACE)
                    )
                )
        );
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).getScore(), closeTo(0.5, 1.e-5));

        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(baseQuery, gaussDecayFunction("num", 0.0, 1.0, null, 0.5)).boostMode(CombineFunction.SUM)
                    )
                )
        );
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).getScore(), closeTo(2.0 + 0.5, 1.e-5));
        logger.info("--> Hit[0] {} Explanation:\n {}", sr.getHits().getAt(0).getId(), sr.getHits().getAt(0).getExplanation());

        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(baseQuery, gaussDecayFunction("num", 0.0, 1.0, null, 0.5)).boostMode(CombineFunction.AVG)
                    )
                )
        );
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).getScore(), closeTo((2.0 + 0.5) / 2, 1.e-5));

        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(baseQuery, gaussDecayFunction("num", 0.0, 1.0, null, 0.5)).boostMode(CombineFunction.MIN)
                    )
                )
        );
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).getScore(), closeTo(0.5, 1.e-5));

        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(baseQuery, gaussDecayFunction("num", 0.0, 1.0, null, 0.5)).boostMode(CombineFunction.MAX)
                    )
                )
        );
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).getScore(), closeTo(2.0, 1.e-5));

    }

    public void testCombineModesExplain() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num")
                    .field("type", "double")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        client().prepareIndex()
            .setId("1")
            .setIndex("test")
            .setRefreshPolicy(IMMEDIATE)
            .setSource(jsonBuilder().startObject().field("test", "value value").field("num", 1.0).endObject())
            .get();

        FunctionScoreQueryBuilder baseQuery = functionScoreQuery(
            constantScoreQuery(termQuery("test", "value")).queryName("query1"),
            ScoreFunctionBuilders.weightFactorFunction(2, "weight1")
        );
        indexRandomForConcurrentSearch("test");
        // decay score should return 0.5 for this function and baseQuery should return 2.0f as it's score
        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().explain(true)
                        .query(
                            functionScoreQuery(baseQuery, gaussDecayFunction("num", 0.0, 1.0, null, 0.5, "func2")).boostMode(
                                CombineFunction.MULTIPLY
                            )
                        )
                )
        );
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(0).getExplanation().getDetails(), arrayWithSize(2));
        assertThat(sh.getAt(0).getExplanation().getDetails()[0].getDetails(), arrayWithSize(2));
        // "description": "ConstantScore(test:value) (_name: query1)"
        assertThat(
            sh.getAt(0).getExplanation().getDetails()[0].getDetails()[0].getDescription(),
            equalTo("ConstantScore(test:value) (_name: query1)")
        );
        assertThat(sh.getAt(0).getExplanation().getDetails()[0].getDetails()[1].getDetails(), arrayWithSize(2));
        assertThat(sh.getAt(0).getExplanation().getDetails()[0].getDetails()[1].getDetails()[0].getDetails(), arrayWithSize(2));
        // "description": "constant score 1.0(_name: func1) - no function provided"
        assertThat(
            sh.getAt(0).getExplanation().getDetails()[0].getDetails()[1].getDetails()[0].getDetails()[0].getDescription(),
            equalTo("constant score 1.0(_name: weight1) - no function provided")
        );
        // "description": "exp(-0.5*pow(MIN[Math.max(Math.abs(1.0(=doc value) - 0.0(=origin))) - 0.0(=offset), 0)],2.0)/0.7213475204444817,
        // _name: func2)"
        assertThat(sh.getAt(0).getExplanation().getDetails()[1].getDetails(), arrayWithSize(2));
        assertThat(sh.getAt(0).getExplanation().getDetails()[1].getDetails()[0].getDetails(), arrayWithSize(1));
        assertThat(
            sh.getAt(0).getExplanation().getDetails()[1].getDetails()[0].getDetails()[0].getDescription(),
            containsString("_name: func2")
        );
    }

    public void testExceptionThrownIfScaleLE0() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num1")
                    .field("type", "date")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        client().index(
            indexRequest("test").id("1").source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").endObject())
        ).actionGet();
        client().index(
            indexRequest("test").id("2").source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-28").endObject())
        ).actionGet();
        refresh();

        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num1", "2013-05-28", "-1d")))
                )
        );
        try {
            response.actionGet();
            fail("Expected SearchPhaseExecutionException");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), is("all shards failed"));
        }
    }

    public void testParseDateMath() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num1")
                    .field("type", "date")
                    .field("format", "epoch_millis")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        client().index(
            indexRequest("test").id("1")
                .source(jsonBuilder().startObject().field("test", "value").field("num1", System.currentTimeMillis()).endObject())
        ).actionGet();
        client().index(
            indexRequest("test").id("2")
                .source(
                    jsonBuilder().startObject()
                        .field("test", "value")
                        .field("num1", System.currentTimeMillis() - (1000 * 60 * 60 * 24))
                        .endObject()
                )
        ).actionGet();
        refresh();

        indexRandomForConcurrentSearch("test");
        SearchResponse sr = client().search(
            searchRequest().source(
                searchSource().query(functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num1", "now", "2d")))
            )
        ).get();

        assertNoFailures(sr);
        assertOrderedSearchHits(sr, "1", "2");

        sr = client().search(
            searchRequest().source(
                searchSource().query(functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num1", "now-1d", "2d")))
            )
        ).get();

        assertNoFailures(sr);
        assertOrderedSearchHits(sr, "2", "1");

    }

    public void testValueMissingLin() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num1")
                    .field("type", "date")
                    .endObject()
                    .startObject("num2")
                    .field("type", "double")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        client().index(
            indexRequest("test").id("1")
                .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").field("num2", "1.0").endObject())
        ).actionGet();
        client().index(
            indexRequest("test").id("2").source(jsonBuilder().startObject().field("test", "value").field("num2", "1.0").endObject())
        ).actionGet();
        client().index(
            indexRequest("test").id("3")
                .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-30").field("num2", "1.0").endObject())
        ).actionGet();
        client().index(
            indexRequest("test").id("4").source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-30").endObject())
        ).actionGet();

        refresh();
        indexRandomForConcurrentSearch("test");

        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(
                            baseQuery,
                            new FilterFunctionBuilder[] {
                                new FilterFunctionBuilder(linearDecayFunction("num1", "2013-05-28", "+3d")),
                                new FilterFunctionBuilder(linearDecayFunction("num2", "0.0", "1")) }
                        ).scoreMode(FunctionScoreQuery.ScoreMode.MULTIPLY)
                    )
                )
        );

        SearchResponse sr = response.actionGet();

        assertNoFailures(sr);
        SearchHits sh = sr.getHits();
        assertThat(sh.getHits().length, equalTo(4));
        double[] scores = new double[4];
        for (int i = 0; i < sh.getHits().length; i++) {
            scores[Integer.parseInt(sh.getAt(i).getId()) - 1] = sh.getAt(i).getScore();
        }
        assertThat(scores[0], lessThan(scores[1]));
        assertThat(scores[2], lessThan(scores[3]));

    }

    public void testDateWithoutOrigin() throws Exception {
        ZonedDateTime dt = ZonedDateTime.now(ZoneOffset.UTC);

        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num1")
                    .field("type", "date")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        ZonedDateTime docDate = dt.minusDays(1);
        String docDateString = docDate.getYear()
            + "-"
            + String.format(Locale.ROOT, "%02d", docDate.getMonthValue())
            + "-"
            + String.format(Locale.ROOT, "%02d", docDate.getDayOfMonth());
        client().index(
            indexRequest("test").id("1").source(jsonBuilder().startObject().field("test", "value").field("num1", docDateString).endObject())
        ).actionGet();
        docDate = dt.minusDays(2);
        docDateString = docDate.getYear()
            + "-"
            + String.format(Locale.ROOT, "%02d", docDate.getMonthValue())
            + "-"
            + String.format(Locale.ROOT, "%02d", docDate.getDayOfMonth());
        client().index(
            indexRequest("test").id("2").source(jsonBuilder().startObject().field("test", "value").field("num1", docDateString).endObject())
        ).actionGet();
        docDate = dt.minusDays(3);
        docDateString = docDate.getYear()
            + "-"
            + String.format(Locale.ROOT, "%02d", docDate.getMonthValue())
            + "-"
            + String.format(Locale.ROOT, "%02d", docDate.getDayOfMonth());
        client().index(
            indexRequest("test").id("3").source(jsonBuilder().startObject().field("test", "value").field("num1", docDateString).endObject())
        ).actionGet();

        refresh();
        indexRandomForConcurrentSearch("test");

        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(
                            QueryBuilders.matchAllQuery(),
                            new FilterFunctionBuilder[] {
                                new FilterFunctionBuilder(linearDecayFunction("num1", null, "7000d")),
                                new FilterFunctionBuilder(gaussDecayFunction("num1", null, "1d")),
                                new FilterFunctionBuilder(exponentialDecayFunction("num1", null, "7000d")) }
                        ).scoreMode(FunctionScoreQuery.ScoreMode.MULTIPLY)
                    )
                )
        );

        SearchResponse sr = response.actionGet();
        assertNoFailures(sr);
        SearchHits sh = sr.getHits();
        assertThat(sh.getHits().length, equalTo(3));
        double[] scores = new double[4];
        for (int i = 0; i < sh.getHits().length; i++) {
            scores[Integer.parseInt(sh.getAt(i).getId()) - 1] = sh.getAt(i).getScore();
        }
        assertThat(scores[1], lessThan(scores[0]));
        assertThat(scores[2], lessThan(scores[1]));

    }

    public void testManyDocsLin() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("test")
            .field("type", "text")
            .endObject()
            .startObject("date")
            .field("type", "date")
            .field("doc_values", true)
            .endObject()
            .startObject("num")
            .field("type", "double")
            .field("doc_values", true)
            .endObject()
            .startObject("geo")
            .field("type", "geo_point")
            .field("ignore_malformed", true);
        xContentBuilder.endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(xContentBuilder));
        int numDocs = 200;
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            double lat = 100 + (int) (10.0 * (i) / (numDocs));
            double lon = 100;
            int day = (int) (29.0 * (i) / (numDocs)) + 1;
            String dayString = day < 10 ? "0" + Integer.toString(day) : Integer.toString(day);
            String date = "2013-05-" + dayString;

            indexBuilders.add(
                client().prepareIndex()
                    .setId(Integer.toString(i))
                    .setIndex("test")
                    .setSource(
                        jsonBuilder().startObject()
                            .field("test", "value")
                            .field("date", date)
                            .field("num", i)
                            .startObject("geo")
                            .field("lat", lat)
                            .field("lon", lon)
                            .endObject()
                            .endObject()
                    )
            );
        }
        indexRandom(true, indexBuilders);
        List<Float> lonlat = new ArrayList<>();
        lonlat.add(100f);
        lonlat.add(110f);
        indexRandomForConcurrentSearch("test");
        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().size(numDocs)
                        .query(
                            functionScoreQuery(
                                termQuery("test", "value"),
                                new FilterFunctionBuilder[] {
                                    new FilterFunctionBuilder(linearDecayFunction("date", "2013-05-30", "+15d")),
                                    new FilterFunctionBuilder(linearDecayFunction("geo", lonlat, "1000km")),
                                    new FilterFunctionBuilder(linearDecayFunction("num", numDocs, numDocs / 2.0)) }
                            ).scoreMode(ScoreMode.MULTIPLY).boostMode(CombineFunction.REPLACE)
                        )
                )
        );

        SearchResponse sr = response.actionGet();
        assertNoFailures(sr);
        SearchHits sh = sr.getHits();
        assertThat(sh.getHits().length, equalTo(numDocs));
        double[] scores = new double[numDocs];
        for (int i = 0; i < numDocs; i++) {
            scores[Integer.parseInt(sh.getAt(i).getId())] = sh.getAt(i).getScore();
        }
        for (int i = 0; i < numDocs - 1; i++) {
            assertThat(scores[i], lessThan(scores[i + 1]));
        }
    }

    public void testParsingExceptionIfFieldDoesNotExist() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("geo")
                    .field("type", "geo_point")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        int numDocs = 2;
        client().index(
            indexRequest("test").source(
                jsonBuilder().startObject()
                    .field("test", "value")
                    .startObject("geo")
                    .field("lat", 1)
                    .field("lon", 2)
                    .endObject()
                    .endObject()
            )
        ).actionGet();
        refresh();
        List<Float> lonlat = new ArrayList<>();
        lonlat.add(100f);
        lonlat.add(110f);
        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().size(numDocs)
                        .query(
                            functionScoreQuery(termQuery("test", "value"), linearDecayFunction("type.geo", lonlat, "1000km")).scoreMode(
                                FunctionScoreQuery.ScoreMode.MULTIPLY
                            )
                        )
                )
        );
        try {
            response.actionGet();
            fail("Expected SearchPhaseExecutionException");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), is("all shards failed"));
        }
    }

    public void testParsingExceptionIfFieldTypeDoesNotMatch() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        client().index(
            indexRequest("test").source(jsonBuilder().startObject().field("test", "value").field("num", Integer.toString(1)).endObject())
        ).actionGet();
        refresh();
        // so, we indexed a string field, but now we try to score a num field
        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(termQuery("test", "value"), linearDecayFunction("num", 1.0, 0.5)).scoreMode(ScoreMode.MULTIPLY)
                    )
                )
        );
        try {
            response.actionGet();
            fail("Expected SearchPhaseExecutionException");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), is("all shards failed"));
        }
    }

    public void testNoQueryGiven() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num")
                    .field("type", "double")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        client().index(indexRequest("test").source(jsonBuilder().startObject().field("test", "value").field("num", 1.0).endObject()))
            .actionGet();
        refresh();
        indexRandomForConcurrentSearch("test");
        // so, we indexed a string field, but now we try to score a num field
        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().query(
                        functionScoreQuery(linearDecayFunction("num", 1, 0.5)).scoreMode(FunctionScoreQuery.ScoreMode.MULTIPLY)
                    )
                )
        );
        response.actionGet();
    }

    public void testMultiFieldOptions() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("loc")
                    .field("type", "geo_point")
                    .endObject()
                    .startObject("num")
                    .field("type", "float")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        // Index for testing MIN and MAX
        IndexRequestBuilder doc1 = client().prepareIndex()
            .setId("1")
            .setIndex("test")
            .setSource(
                jsonBuilder().startObject()
                    .field("test", "value")
                    .startArray("loc")
                    .startObject()
                    .field("lat", 10)
                    .field("lon", 20)
                    .endObject()
                    .startObject()
                    .field("lat", 12)
                    .field("lon", 23)
                    .endObject()
                    .endArray()
                    .endObject()
            );
        IndexRequestBuilder doc2 = client().prepareIndex()
            .setId("2")
            .setIndex("test")
            .setSource(
                jsonBuilder().startObject()
                    .field("test", "value")
                    .startObject("loc")
                    .field("lat", 11)
                    .field("lon", 22)
                    .endObject()
                    .endObject()
            );

        indexRandom(true, doc1, doc2);
        indexRandomForConcurrentSearch("test");

        ActionFuture<SearchResponse> response = client().search(searchRequest().source(searchSource().query(baseQuery)));
        SearchResponse sr = response.actionGet();
        assertSearchHits(sr, "1", "2");
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) (2)));

        List<Float> lonlat = new ArrayList<>();
        lonlat.add(20f);
        lonlat.add(10f);
        response = client().search(
            searchRequest().source(
                searchSource().query(
                    functionScoreQuery(baseQuery, gaussDecayFunction("loc", lonlat, "1000km").setMultiValueMode(MultiValueMode.MIN))
                )
            )
        );
        sr = response.actionGet();
        assertSearchHits(sr, "1", "2");
        sh = sr.getHits();

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        response = client().search(
            searchRequest().source(
                searchSource().query(
                    functionScoreQuery(baseQuery, gaussDecayFunction("loc", lonlat, "1000km").setMultiValueMode(MultiValueMode.MAX))
                )
            )
        );
        sr = response.actionGet();
        assertSearchHits(sr, "1", "2");
        sh = sr.getHits();

        assertThat(sh.getAt(0).getId(), equalTo("2"));
        assertThat(sh.getAt(1).getId(), equalTo("1"));

        // Now test AVG and SUM

        doc1 = client().prepareIndex()
            .setId("1")
            .setIndex("test")
            .setSource(
                jsonBuilder().startObject().field("test", "value").startArray("num").value(0.0).value(1.0).value(2.0).endArray().endObject()
            );
        doc2 = client().prepareIndex()
            .setId("2")
            .setIndex("test")
            .setSource(jsonBuilder().startObject().field("test", "value").field("num", 1.0).endObject());

        indexRandom(true, doc1, doc2);
        response = client().search(
            searchRequest().source(
                searchSource().query(
                    functionScoreQuery(baseQuery, linearDecayFunction("num", "0", "10").setMultiValueMode(MultiValueMode.SUM))
                )
            )
        );
        sr = response.actionGet();
        assertSearchHits(sr, "1", "2");
        sh = sr.getHits();

        assertThat(sh.getAt(0).getId(), equalTo("2"));
        assertThat(sh.getAt(1).getId(), equalTo("1"));
        assertThat(1.0 - sh.getAt(0).getScore(), closeTo((1.0 - sh.getAt(1).getScore()) / 3.0, 1.e-6d));
        response = client().search(
            searchRequest().source(
                searchSource().query(
                    functionScoreQuery(baseQuery, linearDecayFunction("num", "0", "10").setMultiValueMode(MultiValueMode.AVG))
                )
            )
        );
        sr = response.actionGet();
        assertSearchHits(sr, "1", "2");
        sh = sr.getHits();
        assertThat((double) (sh.getAt(0).getScore()), closeTo((sh.getAt(1).getScore()), 1.e-6d));
    }

    public void testDistanceScoreGeoLinGaussExplain() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("loc")
                    .field("type", "geo_point")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        indexBuilders.add(
            client().prepareIndex()
                .setId("1")
                .setIndex("test")
                .setSource(
                    jsonBuilder().startObject()
                        .field("test", "value")
                        .startObject("loc")
                        .field("lat", 10)
                        .field("lon", 20)
                        .endObject()
                        .endObject()
                )
        );
        indexBuilders.add(
            client().prepareIndex()
                .setId("2")
                .setIndex("test")
                .setSource(
                    jsonBuilder().startObject()
                        .field("test", "value")
                        .startObject("loc")
                        .field("lat", 11)
                        .field("lon", 22)
                        .endObject()
                        .endObject()
                )
        );

        indexRandom(true, indexBuilders);

        // Test Gauss
        List<Float> lonlat = new ArrayList<>();
        lonlat.add(20f);
        lonlat.add(11f);

        final String queryName = "query1";
        final String functionName = "func1";
        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().explain(true)
                        .query(
                            functionScoreQuery(baseQuery.queryName(queryName), gaussDecayFunction("loc", lonlat, "1000km", functionName))
                        )
                )
        );
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo(2L));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        assertExplain(queryName, functionName, sr);

        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().explain(true)
                        .query(
                            functionScoreQuery(baseQuery.queryName(queryName), linearDecayFunction("loc", lonlat, "1000km", functionName))
                        )
                )
        );

        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo(2L));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        assertExplain(queryName, functionName, sr);

        response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().explain(true)
                        .query(
                            functionScoreQuery(
                                baseQuery.queryName(queryName),
                                exponentialDecayFunction("loc", lonlat, "1000km", functionName)
                            )
                        )
                )
        );

        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits().value(), equalTo(2L));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        assertExplain(queryName, functionName, sr);
    }

    private void assertExplain(final String queryName, final String functionName, SearchResponse sr) {
        SearchHit firstHit = sr.getHits().getAt(0);
        assertThat(firstHit.getExplanation().getDetails(), arrayWithSize(2));
        // "description": "*:* (_name: query1)"
        assertThat(firstHit.getExplanation().getDetails()[0].getDescription().toString(), containsString("_name: " + queryName));
        assertThat(firstHit.getExplanation().getDetails()[1].getDetails(), arrayWithSize(2));
        // "description": "random score function (seed: 12345678, field: _seq_no, _name: func1)"
        assertThat(firstHit.getExplanation().getDetails()[1].getDetails()[0].getDetails(), arrayWithSize(1));
        // "description": "exp(-0.5*pow(MIN of: [Math.max(arcDistance(10.999999972991645, 21.99999994598329(=doc value),11.0, 20.0(=origin))
        // - 0.0(=offset), 0)],2.0)/7.213475204444817E11, _name: func1)"
        assertThat(
            firstHit.getExplanation().getDetails()[1].getDetails()[0].getDetails()[0].getDescription().toString(),
            containsString("_name: " + functionName)
        );
    }
}
