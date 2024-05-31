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

package org.opensearch.search.slice;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.Scroll;
import org.opensearch.search.SearchException;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class SearchSliceIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    public SearchSliceIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    private void setupIndex(int numDocs, int numberOfShards) throws IOException, ExecutionException, InterruptedException {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("invalid_random_kw")
            .field("type", "keyword")
            .field("doc_values", "false")
            .endObject()
            .startObject("random_int")
            .field("type", "integer")
            .field("doc_values", "true")
            .endObject()
            .startObject("invalid_random_int")
            .field("type", "integer")
            .field("doc_values", "false")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(
                    Settings.builder()
                        .put("number_of_shards", numberOfShards)
                        .put("index.max_slices_per_scroll", 10000)
                        .put("index.max_slices_per_pit", 10000)
                )
                .setMapping(mapping)
        );
        ensureGreen();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder builder = jsonBuilder().startObject()
                .field("invalid_random_kw", randomAlphaOfLengthBetween(5, 20))
                .field("random_int", randomInt())
                .field("static_int", 0)
                .field("invalid_random_int", randomInt())
                .endObject();
            requests.add(client().prepareIndex("test").setSource(builder));
        }
        indexRandom(true, requests);
    }

    public void testSearchSort() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numDocs = randomIntBetween(100, 1000);
        setupIndex(numDocs, numShards);
        int max = randomIntBetween(2, numShards * 3);
        for (String field : new String[] { "_id", "random_int", "static_int" }) {
            int fetchSize = randomIntBetween(10, 100);
            // test _doc sort
            SearchRequestBuilder request = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .setSize(fetchSize)
                .addSort(SortBuilders.fieldSort("_doc"));
            assertSearchSlicesWithScroll(request, field, max, numDocs);

            // test numeric sort
            request = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .addSort(SortBuilders.fieldSort("random_int"))
                .setSize(fetchSize);
            assertSearchSlicesWithScroll(request, field, max, numDocs);
        }
    }

    public void testSearchSortWithoutPitOrScroll() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numDocs = randomIntBetween(100, 1000);
        setupIndex(numDocs, numShards);
        int fetchSize = randomIntBetween(10, 100);
        SearchRequestBuilder request = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .setSize(fetchSize)
            .addSort(SortBuilders.fieldSort("_doc"));
        SliceBuilder sliceBuilder = new SliceBuilder("_id", 0, 4);
        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class, () -> request.slice(sliceBuilder).get());
        assertTrue(ex.getMessage().contains("all shards failed"));
    }

    public void testSearchSortWithPIT() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numDocs = randomIntBetween(100, 1000);
        setupIndex(numDocs, numShards);
        int max = randomIntBetween(2, numShards * 3);
        CreatePitRequest pitRequest = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        pitRequest.setIndices(new String[] { "test" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, pitRequest);
        CreatePitResponse pitResponse = execute.get();
        for (String field : new String[] { "_id", "random_int", "static_int" }) {
            int fetchSize = randomIntBetween(10, 100);

            // test _doc sort
            SearchRequestBuilder request = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
                .setSize(fetchSize)
                .addSort(SortBuilders.fieldSort("_doc"));
            assertSearchSlicesWithPIT(request, field, max, numDocs);

            // test numeric sort
            request = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
                .setSize(fetchSize)
                .addSort(SortBuilders.fieldSort("random_int"));
            assertSearchSlicesWithPIT(request, field, max, numDocs);
        }
        client().admin().indices().prepareDelete("test").get();
    }

    private void assertSearchSlicesWithPIT(SearchRequestBuilder request, String field, int numSlice, int numDocs) {
        int totalResults = 0;
        List<String> keys = new ArrayList<>();
        for (int id = 0; id < numSlice; id++) {
            SliceBuilder sliceBuilder = new SliceBuilder(field, id, numSlice);
            SearchResponse searchResponse = request.slice(sliceBuilder).setFrom(0).get();
            totalResults += searchResponse.getHits().getHits().length;
            int expectedSliceResults = (int) searchResponse.getHits().getTotalHits().value;
            int numSliceResults = searchResponse.getHits().getHits().length;
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                assertTrue(keys.add(hit.getId()));
            }
            while (searchResponse.getHits().getHits().length > 0) {
                searchResponse = request.setFrom(numSliceResults).slice(sliceBuilder).get();
                totalResults += searchResponse.getHits().getHits().length;
                numSliceResults += searchResponse.getHits().getHits().length;
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    assertTrue(keys.add(hit.getId()));
                }
            }
            assertThat(numSliceResults, equalTo(expectedSliceResults));
        }
        assertThat(totalResults, equalTo(numDocs));
        assertThat(keys.size(), equalTo(numDocs));
        assertThat(new HashSet(keys).size(), equalTo(numDocs));
    }

    public void testWithPreferenceAndRoutings() throws Exception {
        int numShards = 10;
        int totalDocs = randomIntBetween(100, 1000);
        setupIndex(totalDocs, numShards);
        {
            SearchResponse sr = client().prepareSearch("test").setQuery(matchAllQuery()).setPreference("_shards:1,4").setSize(0).get();
            int numDocs = (int) sr.getHits().getTotalHits().value;
            int max = randomIntBetween(2, numShards * 3);
            int fetchSize = randomIntBetween(10, 100);
            SearchRequestBuilder request = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .setSize(fetchSize)
                .setPreference("_shards:1,4")
                .addSort(SortBuilders.fieldSort("_doc"));
            assertSearchSlicesWithScroll(request, "_id", max, numDocs);
        }
        {
            SearchResponse sr = client().prepareSearch("test").setQuery(matchAllQuery()).setRouting("foo", "bar").setSize(0).get();
            int numDocs = (int) sr.getHits().getTotalHits().value;
            int max = randomIntBetween(2, numShards * 3);
            int fetchSize = randomIntBetween(10, 100);
            SearchRequestBuilder request = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .setSize(fetchSize)
                .setRouting("foo", "bar")
                .addSort(SortBuilders.fieldSort("_doc"));
            assertSearchSlicesWithScroll(request, "_id", max, numDocs);
        }
        {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareAliases()
                    .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("test").alias("alias1").routing("foo"))
                    .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("test").alias("alias2").routing("bar"))
                    .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("test").alias("alias3").routing("baz"))
                    .get()
            );
            SearchResponse sr = client().prepareSearch("alias1", "alias3").setQuery(matchAllQuery()).setSize(0).get();
            int numDocs = (int) sr.getHits().getTotalHits().value;
            int max = randomIntBetween(2, numShards * 3);
            int fetchSize = randomIntBetween(10, 100);
            SearchRequestBuilder request = client().prepareSearch("alias1", "alias3")
                .setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .setSize(fetchSize)
                .addSort(SortBuilders.fieldSort("_doc"));
            assertSearchSlicesWithScroll(request, "_id", max, numDocs);
        }
    }

    public void testInvalidFields() throws Exception {
        setupIndex(0, 1);
        SearchPhaseExecutionException exc = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .slice(new SliceBuilder("invalid_random_int", 0, 10))
                .get()
        );
        Throwable rootCause = findRootCause(exc);
        assertThat(rootCause.getClass(), equalTo(IllegalArgumentException.class));
        assertThat(rootCause.getMessage(), startsWith("cannot load numeric doc values"));

        exc = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .slice(new SliceBuilder("invalid_random_kw", 0, 10))
                .get()
        );
        rootCause = findRootCause(exc);
        assertThat(rootCause.getClass(), equalTo(IllegalArgumentException.class));
        assertThat(rootCause.getMessage(), startsWith("cannot load numeric doc values"));
    }

    public void testInvalidQuery() throws Exception {
        setupIndex(0, 1);
        SearchPhaseExecutionException exc = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch().setQuery(matchAllQuery()).slice(new SliceBuilder("invalid_random_int", 0, 10)).get()
        );
        Throwable rootCause = findRootCause(exc);
        assertThat(rootCause.getClass(), equalTo(SearchException.class));
        assertThat(rootCause.getMessage(), equalTo("`slice` cannot be used outside of a scroll context or PIT context"));
    }

    private void assertSearchSlicesWithScroll(SearchRequestBuilder request, String field, int numSlice, int numDocs) {
        int totalResults = 0;
        List<String> keys = new ArrayList<>();
        for (int id = 0; id < numSlice; id++) {
            SliceBuilder sliceBuilder = new SliceBuilder(field, id, numSlice);
            SearchResponse searchResponse = request.slice(sliceBuilder).get();
            totalResults += searchResponse.getHits().getHits().length;
            int expectedSliceResults = (int) searchResponse.getHits().getTotalHits().value;
            int numSliceResults = searchResponse.getHits().getHits().length;
            String scrollId = searchResponse.getScrollId();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                assertTrue(keys.add(hit.getId()));
            }
            while (searchResponse.getHits().getHits().length > 0) {
                searchResponse = client().prepareSearchScroll("test")
                    .setScrollId(scrollId)
                    .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                    .get();
                scrollId = searchResponse.getScrollId();
                totalResults += searchResponse.getHits().getHits().length;
                numSliceResults += searchResponse.getHits().getHits().length;
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    assertTrue(keys.add(hit.getId()));
                }
            }
            assertThat(numSliceResults, equalTo(expectedSliceResults));
            clearScroll(scrollId);
        }
        assertThat(totalResults, equalTo(numDocs));
        assertThat(keys.size(), equalTo(numDocs));
        assertThat(new HashSet(keys).size(), equalTo(numDocs));
    }

    private Throwable findRootCause(Exception e) {
        Throwable ret = e;
        while (ret.getCause() != null) {
            ret = ret.getCause();
        }
        return ret;
    }
}
