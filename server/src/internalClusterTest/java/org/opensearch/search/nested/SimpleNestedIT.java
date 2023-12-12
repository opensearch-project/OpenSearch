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

package org.opensearch.search.nested;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.sort.NestedSortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortMode;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.ParameterizedOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.nestedQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class SimpleNestedIT extends ParameterizedOpenSearchIntegTestCase {

    public SimpleNestedIT(Settings dynamicSettings) {
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
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.CONCURRENT_SEGMENT_SEARCH, "true").build();
    }

    public void testSimpleNested() throws Exception {
        assertAcked(prepareCreate("test").setMapping("nested1", "type=nested"));
        ensureGreen();

        // check on no data, see it works
        SearchResponse searchResponse = client().prepareSearch("test").get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("n_field1", "n_value1_1")).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", "value1")
                    .startArray("nested1")
                    .startObject()
                    .field("n_field1", "n_value1_1")
                    .field("n_field2", "n_value2_1")
                    .endObject()
                    .startObject()
                    .field("n_field1", "n_value1_2")
                    .field("n_field2", "n_value2_2")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        waitForRelocation(ClusterHealthStatus.GREEN);
        indexRandomForConcurrentSearch("test");
        GetResponse getResponse = client().prepareGet("test", "1").get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getSourceAsBytes(), notNullValue());
        refresh();
        // check the numDocs
        assertDocumentCount("test", 3);

        searchResponse = client().prepareSearch("test").setQuery(termQuery("n_field1", "n_value1_1")).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        // search for something that matches the nested doc, and see that we don't find the nested doc
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("n_field1", "n_value1_1")).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        // now, do a nested query
        searchResponse = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1_1"), ScoreMode.Avg))
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1_1"), ScoreMode.Avg))
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        // add another doc, one that would match if it was not nested...

        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", "value1")
                    .startArray("nested1")
                    .startObject()
                    .field("n_field1", "n_value1_1")
                    .field("n_field2", "n_value2_2")
                    .endObject()
                    .startObject()
                    .field("n_field1", "n_value1_2")
                    .field("n_field2", "n_value2_1")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();
        assertDocumentCount("test", 6);

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.n_field1", "n_value1_1")).must(termQuery("nested1.n_field2", "n_value2_1")),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        // filter
        searchResponse = client().prepareSearch("test")
            .setQuery(
                boolQuery().must(matchAllQuery())
                    .mustNot(
                        nestedQuery(
                            "nested1",
                            boolQuery().must(termQuery("nested1.n_field1", "n_value1_1")).must(termQuery("nested1.n_field2", "n_value2_1")),
                            ScoreMode.Avg
                        )
                    )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        // check with type prefix
        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.n_field1", "n_value1_1")).must(termQuery("nested1.n_field2", "n_value2_1")),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        // check delete, so all is gone...
        DeleteResponse deleteResponse = client().prepareDelete("test", "2").get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());

        refresh();
        assertDocumentCount("test", 3);

        searchResponse = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1_1"), ScoreMode.Avg))
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    public void testMultiNested() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("nested1")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("nested2")
                    .field("type", "nested")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        ensureGreen();
        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("field", "value")
                    .startArray("nested1")
                    .startObject()
                    .field("field1", "1")
                    .startArray("nested2")
                    .startObject()
                    .field("field2", "2")
                    .endObject()
                    .startObject()
                    .field("field2", "3")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("field1", "4")
                    .startArray("nested2")
                    .startObject()
                    .field("field2", "5")
                    .endObject()
                    .startObject()
                    .field("field2", "6")
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        GetResponse getResponse = client().prepareGet("test", "1").get();
        assertThat(getResponse.isExists(), equalTo(true));
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();
        // check the numDocs
        assertDocumentCount("test", 7);
        indexRandomForConcurrentSearch("test");

        // do some multi nested queries
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1", termQuery("nested1.field1", "1"), ScoreMode.Avg))
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "2"), ScoreMode.Avg))
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.field1", "1"))
                        .must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "2"), ScoreMode.Avg)),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.field1", "1"))
                        .must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "3"), ScoreMode.Avg)),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.field1", "1"))
                        .must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "4"), ScoreMode.Avg)),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.field1", "1"))
                        .must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "5"), ScoreMode.Avg)),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.field1", "4"))
                        .must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "5"), ScoreMode.Avg)),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.field1", "4"))
                        .must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "2"), ScoreMode.Avg)),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
    }

    // When IncludeNestedDocsQuery is wrapped in a FilteredQuery then a in-finite loop occurs b/c of a bug in
    // IncludeNestedDocsQuery#advance()
    // This IncludeNestedDocsQuery also needs to be aware of the filter from alias
    public void testDeleteNestedDocsWithAlias() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put("index.refresh_interval", -1).build())
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("field1")
                        .field("type", "text")
                        .endObject()
                        .startObject("nested1")
                        .field("type", "nested")
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        client().admin().indices().prepareAliases().addAlias("test", "alias1", QueryBuilders.termQuery("field1", "value1")).get();

        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", "value1")
                    .startArray("nested1")
                    .startObject()
                    .field("n_field1", "n_value1_1")
                    .field("n_field2", "n_value2_1")
                    .endObject()
                    .startObject()
                    .field("n_field1", "n_value1_2")
                    .field("n_field2", "n_value2_2")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", "value2")
                    .startArray("nested1")
                    .startObject()
                    .field("n_field1", "n_value1_1")
                    .field("n_field2", "n_value2_1")
                    .endObject()
                    .startObject()
                    .field("n_field1", "n_value1_2")
                    .field("n_field2", "n_value2_2")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        flush();
        refresh();
        assertDocumentCount("test", 6);
    }

    public void testExplain() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("nested1")
                    .field("type", "nested")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", "value1")
                    .startArray("nested1")
                    .startObject()
                    .field("n_field1", "n_value1")
                    .endObject()
                    .startObject()
                    .field("n_field1", "n_value1")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();
        indexRandomForConcurrentSearch("test");

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1"), ScoreMode.Total))
            .setExplain(true)
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        Explanation explanation = searchResponse.getHits().getHits()[0].getExplanation();
        assertThat(explanation.getValue(), equalTo(searchResponse.getHits().getHits()[0].getScore()));
        assertThat(explanation.toString(), startsWith("0.36464313 = Score based on 2 child docs in range from 0 to 1"));
    }

    public void testSimpleNestedSorting() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put("index.refresh_interval", -1))
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .startObject("properties")
                        .startObject("field1")
                        .field("type", "long")
                        .field("store", true)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );
        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", 1)
                    .startArray("nested1")
                    .startObject()
                    .field("field1", 5)
                    .endObject()
                    .startObject()
                    .field("field1", 4)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", 2)
                    .startArray("nested1")
                    .startObject()
                    .field("field1", 1)
                    .endObject()
                    .startObject()
                    .field("field1", 2)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        client().prepareIndex("test")
            .setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", 3)
                    .startArray("nested1")
                    .startObject()
                    .field("field1", 3)
                    .endObject()
                    .startObject()
                    .field("field1", 4)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        refresh();
        indexRandomForConcurrentSearch("test");

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .addSort(SortBuilders.fieldSort("nested1.field1").order(SortOrder.ASC).setNestedPath("nested1"))
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("4"));

        searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .addSort(SortBuilders.fieldSort("nested1.field1").order(SortOrder.DESC).setNestedPath("nested1"))
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("5"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("4"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("2"));
    }

    public void testSimpleNestedSortingWithNestedFilterMissing() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put("index.refresh_interval", -1))
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .startObject("properties")
                        .startObject("field1")
                        .field("type", "long")
                        .endObject()
                        .startObject("field2")
                        .field("type", "boolean")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );
        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", 1)
                    .startArray("nested1")
                    .startObject()
                    .field("field1", 5)
                    .field("field2", true)
                    .endObject()
                    .startObject()
                    .field("field1", 4)
                    .field("field2", true)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", 2)
                    .startArray("nested1")
                    .startObject()
                    .field("field1", 1)
                    .field("field2", true)
                    .endObject()
                    .startObject()
                    .field("field1", 2)
                    .field("field2", true)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        // Doc with missing nested docs if nested filter is used
        refresh();
        client().prepareIndex("test")
            .setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", 3)
                    .startArray("nested1")
                    .startObject()
                    .field("field1", 3)
                    .field("field2", false)
                    .endObject()
                    .startObject()
                    .field("field1", 4)
                    .field("field2", false)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        refresh();
        indexRandomForConcurrentSearch("test");

        SearchRequestBuilder searchRequestBuilder = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("nested1.field1")
                    .setNestedPath("nested1")
                    .setNestedFilter(termQuery("nested1.field2", true))
                    .missing(10)
                    .order(SortOrder.ASC)
            );

        if (randomBoolean()) {
            searchRequestBuilder.setScroll("10m");
        }

        SearchResponse searchResponse = searchRequestBuilder.get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("4"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("10"));

        searchRequestBuilder = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("nested1.field1")
                    .setNestedPath("nested1")
                    .setNestedFilter(termQuery("nested1.field2", true))
                    .missing(10)
                    .order(SortOrder.DESC)
            );

        if (randomBoolean()) {
            searchRequestBuilder.setScroll("10m");
        }

        searchResponse = searchRequestBuilder.get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("5"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("2"));
        client().prepareClearScroll().addScrollId("_all").get();
    }

    public void testNestedSortWithMultiLevelFiltering() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                "{\n"
                    + "  \"properties\": {\n"
                    + "    \"acl\": {\n"
                    + "      \"type\": \"nested\",\n"
                    + "      \"properties\": {\n"
                    + "        \"access_id\": {\"type\": \"keyword\"},\n"
                    + "        \"operation\": {\n"
                    + "          \"type\": \"nested\",\n"
                    + "          \"properties\": {\n"
                    + "            \"name\": {\"type\": \"keyword\"},\n"
                    + "            \"user\": {\n"
                    + "              \"type\": \"nested\",\n"
                    + "              \"properties\": {\n"
                    + "                \"username\": {\"type\": \"keyword\"},\n"
                    + "                \"id\": {\"type\": \"integer\"}\n"
                    + "              }\n"
                    + "            }\n"
                    + "          }\n"
                    + "        }\n"
                    + "      }\n"
                    + "    }\n"
                    + "  }\n"
                    + "}"
            )
        );
        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                "{\n"
                    + "  \"acl\": [\n"
                    + "    {\n"
                    + "      \"access_id\": 1,\n"
                    + "      \"operation\": [\n"
                    + "        {\n"
                    + "          \"name\": \"read\",\n"
                    + "          \"user\": [\n"
                    + "            {\"username\": \"grault\", \"id\": 1},\n"
                    + "            {\"username\": \"quxx\", \"id\": 2},\n"
                    + "            {\"username\": \"bar\", \"id\": 3}\n"
                    + "          ]\n"
                    + "        },\n"
                    + "        {\n"
                    + "          \"name\": \"write\",\n"
                    + "          \"user\": [\n"
                    + "            {\"username\": \"quxx\", \"id\": 2},\n"
                    + "            {\"username\": \"bar\", \"id\": 3}\n"
                    + "          ]\n"
                    + "        }\n"
                    + "      ]\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"access_id\": 2,\n"
                    + "      \"operation\": [\n"
                    + "        {\n"
                    + "          \"name\": \"read\",\n"
                    + "          \"user\": [\n"
                    + "            {\"username\": \"baz\", \"id\": 4},\n"
                    + "            {\"username\": \"quxx\", \"id\": 2}\n"
                    + "          ]\n"
                    + "        },\n"
                    + "        {\n"
                    + "          \"name\": \"write\",\n"
                    + "          \"user\": [\n"
                    + "            {\"username\": \"quxx\", \"id\": 2}\n"
                    + "          ]\n"
                    + "        },\n"
                    + "        {\n"
                    + "          \"name\": \"execute\",\n"
                    + "          \"user\": [\n"
                    + "            {\"username\": \"quxx\", \"id\": 2}\n"
                    + "          ]\n"
                    + "        }\n"
                    + "      ]\n"
                    + "    }\n"
                    + "  ]\n"
                    + "}",
                MediaTypeRegistry.JSON
            )
            .get();

        client().prepareIndex("test")
            .setId("2")
            .setSource(
                "{\n"
                    + "  \"acl\": [\n"
                    + "    {\n"
                    + "      \"access_id\": 1,\n"
                    + "      \"operation\": [\n"
                    + "        {\n"
                    + "          \"name\": \"read\",\n"
                    + "          \"user\": [\n"
                    + "            {\"username\": \"grault\", \"id\": 1},\n"
                    + "            {\"username\": \"foo\", \"id\": 5}\n"
                    + "          ]\n"
                    + "        },\n"
                    + "        {\n"
                    + "          \"name\": \"execute\",\n"
                    + "          \"user\": [\n"
                    + "            {\"username\": \"foo\", \"id\": 5}\n"
                    + "          ]\n"
                    + "        }\n"
                    + "      ]\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"access_id\": 3,\n"
                    + "      \"operation\": [\n"
                    + "        {\n"
                    + "          \"name\": \"read\",\n"
                    + "          \"user\": [\n"
                    + "            {\"username\": \"grault\", \"id\": 1}\n"
                    + "          ]\n"
                    + "        },\n"
                    + "        {\n"
                    + "          \"name\": \"write\",\n"
                    + "          \"user\": [\n"
                    + "            {\"username\": \"grault\", \"id\": 1}\n"
                    + "          ]\n"
                    + "        },\n"
                    + "        {\n"
                    + "          \"name\": \"execute\",\n"
                    + "          \"user\": [\n"
                    + "            {\"username\": \"grault\", \"id\": 1}\n"
                    + "          ]\n"
                    + "        }\n"
                    + "      ]\n"
                    + "    }\n"
                    + "  ]\n"
                    + "}",
                MediaTypeRegistry.JSON
            )
            .get();
        refresh();
        indexRandomForConcurrentSearch("test");

        // access id = 1, read, max value, asc, should use grault and quxx
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("acl.operation.user.username")
                    .setNestedSort(
                        new NestedSortBuilder("acl").setFilter(QueryBuilders.termQuery("acl.access_id", "1"))
                            .setNestedSort(
                                new NestedSortBuilder("acl.operation").setFilter(QueryBuilders.termQuery("acl.operation.name", "read"))
                                    .setNestedSort(new NestedSortBuilder("acl.operation.user"))
                            )
                    )
                    .sortMode(SortMode.MAX)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("grault"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("quxx"));

        // access id = 1, read, min value, asc, should now use bar and foo
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("acl.operation.user.username")
                    .setNestedSort(
                        new NestedSortBuilder("acl").setFilter(QueryBuilders.termQuery("acl.access_id", "1"))
                            .setNestedSort(
                                new NestedSortBuilder("acl.operation").setFilter(QueryBuilders.termQuery("acl.operation.name", "read"))
                                    .setNestedSort(new NestedSortBuilder("acl.operation.user"))
                            )
                    )
                    .sortMode(SortMode.MIN)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("bar"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("foo"));

        // execute, by grault or foo, by user id, sort missing first
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("acl.operation.user.id")
                    .setNestedSort(
                        new NestedSortBuilder("acl").setNestedSort(
                            new NestedSortBuilder("acl.operation").setFilter(QueryBuilders.termQuery("acl.operation.name", "execute"))
                                .setNestedSort(
                                    new NestedSortBuilder("acl.operation.user").setFilter(
                                        QueryBuilders.termsQuery("acl.operation.user.username", "grault", "foo")
                                    )
                                )
                        )
                    )
                    .missing("_first")
                    .sortMode(SortMode.MIN)
                    .order(SortOrder.DESC)
            )
            .get();

        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1")); // missing first
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("1"));

        // execute, by grault or foo, by username, sort missing last (default)
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("acl.operation.user.username")
                    .setNestedSort(
                        new NestedSortBuilder("acl").setNestedSort(
                            new NestedSortBuilder("acl.operation").setFilter(QueryBuilders.termQuery("acl.operation.name", "execute"))
                                .setNestedSort(
                                    new NestedSortBuilder("acl.operation.user").setFilter(
                                        QueryBuilders.termsQuery("acl.operation.user.username", "grault", "foo")
                                    )
                                )
                        )
                    )
                    .sortMode(SortMode.MIN)
                    .order(SortOrder.DESC)
            )
            .get();

        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("foo"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("1")); // missing last
    }

    // https://github.com/elastic/elasticsearch/issues/31554
    public void testLeakingSortValues() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("number_of_shards", 1))
                .setMapping(
                    "{\n"
                        + "        \"dynamic\": \"strict\",\n"
                        + "        \"properties\": {\n"
                        + "          \"nested1\": {\n"
                        + "            \"type\": \"nested\",\n"
                        + "            \"properties\": {\n"
                        + "              \"nested2\": {\n"
                        + "                \"type\": \"nested\",\n"
                        + "                \"properties\": {\n"
                        + "                  \"nested2_keyword\": {\n"
                        + "                    \"type\": \"keyword\"\n"
                        + "                  },\n"
                        + "                  \"sortVal\": {\n"
                        + "                    \"type\": \"integer\"\n"
                        + "                  }\n"
                        + "                }\n"
                        + "              }\n"
                        + "            }\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                )
        );
        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                "{\n"
                    + "  \"nested1\": [\n"
                    + "    {\n"
                    + "      \"nested2\": [\n"
                    + "        {\n"
                    + "          \"nested2_keyword\": \"nested2_bar\",\n"
                    + "          \"sortVal\": 1\n"
                    + "        }\n"
                    + "      ]\n"
                    + "    }\n"
                    + " ]\n"
                    + "}",
                MediaTypeRegistry.JSON
            )
            .get();

        client().prepareIndex("test")
            .setId("2")
            .setSource(
                "{\n"
                    + "  \"nested1\": [\n"
                    + "    {\n"
                    + "      \"nested2\": [\n"
                    + "        {\n"
                    + "          \"nested2_keyword\": \"nested2_bar\",\n"
                    + "          \"sortVal\": 2\n"
                    + "        }\n"
                    + "      ]\n"
                    + "    } \n"
                    + "  ]\n"
                    + "}",
                MediaTypeRegistry.JSON
            )
            .get();

        refresh();
        indexRandomForConcurrentSearch("test");

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(termQuery("_id", 2))
            .addSort(
                SortBuilders.fieldSort("nested1.nested2.sortVal")
                    .setNestedSort(
                        new NestedSortBuilder("nested1").setNestedSort(
                            new NestedSortBuilder("nested1.nested2").setFilter(termQuery("nested1.nested2.nested2_keyword", "nested2_bar"))
                        )
                    )
            )
            .get();

        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("2"));

    }

    public void testSortNestedWithNestedFilter() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("grand_parent_values")
                    .field("type", "long")
                    .endObject()
                    .startObject("parent")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("parent_values")
                    .field("type", "long")
                    .endObject()
                    .startObject("child")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("child_values")
                    .field("type", "long")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();

        // sum: 11
        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("grand_parent_values", 1L)
                    .startArray("parent")
                    .startObject()
                    .field("filter", false)
                    .field("parent_values", 1L)
                    .startArray("child")
                    .startObject()
                    .field("filter", true)
                    .field("child_values", 1L)
                    .startObject("child_obj")
                    .field("value", 1L)
                    .endObject()
                    .endObject()
                    .startObject()
                    .field("filter", false)
                    .field("child_values", 6L)
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("filter", true)
                    .field("parent_values", 2L)
                    .startArray("child")
                    .startObject()
                    .field("filter", false)
                    .field("child_values", -1L)
                    .endObject()
                    .startObject()
                    .field("filter", false)
                    .field("child_values", 5L)
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        // sum: 7
        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("grand_parent_values", 2L)
                    .startArray("parent")
                    .startObject()
                    .field("filter", false)
                    .field("parent_values", 2L)
                    .startArray("child")
                    .startObject()
                    .field("filter", true)
                    .field("child_values", 2L)
                    .startObject("child_obj")
                    .field("value", 2L)
                    .endObject()
                    .endObject()
                    .startObject()
                    .field("filter", false)
                    .field("child_values", 4L)
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("parent_values", 3L)
                    .field("filter", true)
                    .startArray("child")
                    .startObject()
                    .field("child_values", -2L)
                    .field("filter", false)
                    .endObject()
                    .startObject()
                    .field("filter", false)
                    .field("child_values", 3L)
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        // sum: 2
        client().prepareIndex("test")
            .setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("grand_parent_values", 3L)
                    .startArray("parent")
                    .startObject()
                    .field("parent_values", 3L)
                    .field("filter", false)
                    .startArray("child")
                    .startObject()
                    .field("filter", true)
                    .field("child_values", 3L)
                    .startObject("child_obj")
                    .field("value", 3L)
                    .endObject()
                    .endObject()
                    .startObject()
                    .field("filter", false)
                    .field("child_values", 1L)
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("parent_values", 4L)
                    .field("filter", true)
                    .startArray("child")
                    .startObject()
                    .field("filter", false)
                    .field("child_values", -3L)
                    .endObject()
                    .startObject()
                    .field("filter", false)
                    .field("child_values", 1L)
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        refresh();
        indexRandomForConcurrentSearch("test");

        // Without nested filter
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("parent.child.child_values").setNestedPath("parent.child").order(SortOrder.ASC))
            .get();
        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("-3"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("-2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("-1"));

        // With nested filter
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedPath("parent.child")
                    .setNestedFilter(QueryBuilders.termQuery("parent.child.filter", true))
                    .order(SortOrder.ASC)
            )
            .get();
        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));

        // Nested path should be automatically detected, expect same results as above search request
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedPath("parent.child")
                    .setNestedFilter(QueryBuilders.termQuery("parent.child.filter", true))
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.parent_values")
                    .setNestedPath("parent.child")
                    .setNestedFilter(QueryBuilders.termQuery("parent.filter", false))
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedSort(
                        new NestedSortBuilder("parent").setFilter(QueryBuilders.termQuery("parent.filter", false))
                            .setNestedSort(new NestedSortBuilder("parent.child"))
                    )
                    .sortMode(SortMode.MAX)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("4"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("6"));

        // Check if closest nested type is resolved
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_obj.value")
                    .setNestedPath("parent.child")
                    .setNestedFilter(QueryBuilders.termQuery("parent.child.filter", true))
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));

        // Sort mode: sum
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedPath("parent.child")
                    .sortMode(SortMode.SUM)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("7"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("11"));

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedPath("parent.child")
                    .sortMode(SortMode.SUM)
                    .order(SortOrder.DESC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("11"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("7"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("2"));

        // Sort mode: sum with filter
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedPath("parent.child")
                    .setNestedFilter(QueryBuilders.termQuery("parent.child.filter", true))
                    .sortMode(SortMode.SUM)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));

        // Sort mode: avg
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedPath("parent.child")
                    .sortMode(SortMode.AVG)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedPath("parent.child")
                    .sortMode(SortMode.AVG)
                    .order(SortOrder.DESC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("1"));

        // Sort mode: avg with filter
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedPath("parent.child")
                    .setNestedFilter(QueryBuilders.termQuery("parent.child.filter", true))
                    .sortMode(SortMode.AVG)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));
    }

    // Issue #9305
    public void testNestedSortingWithNestedFilterAsFilter() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("officelocation")
                    .field("type", "text")
                    .endObject()
                    .startObject("users")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("first")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("last")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("workstations")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("stationid")
                    .field("type", "text")
                    .endObject()
                    .startObject("phoneid")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        IndexResponse indexResponse1 = client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("officelocation", "gendale")
                    .startArray("users")
                    .startObject()
                    .field("first", "fname1")
                    .field("last", "lname1")
                    .startArray("workstations")
                    .startObject()
                    .field("stationid", "s1")
                    .field("phoneid", "p1")
                    .endObject()
                    .startObject()
                    .field("stationid", "s2")
                    .field("phoneid", "p2")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("first", "fname2")
                    .field("last", "lname2")
                    .startArray("workstations")
                    .startObject()
                    .field("stationid", "s3")
                    .field("phoneid", "p3")
                    .endObject()
                    .startObject()
                    .field("stationid", "s4")
                    .field("phoneid", "p4")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("first", "fname3")
                    .field("last", "lname3")
                    .startArray("workstations")
                    .startObject()
                    .field("stationid", "s5")
                    .field("phoneid", "p5")
                    .endObject()
                    .startObject()
                    .field("stationid", "s6")
                    .field("phoneid", "p6")
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        assertTrue(indexResponse1.getShardInfo().getSuccessful() > 0);

        IndexResponse indexResponse2 = client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("officelocation", "gendale")
                    .startArray("users")
                    .startObject()
                    .field("first", "fname4")
                    .field("last", "lname4")
                    .startArray("workstations")
                    .startObject()
                    .field("stationid", "s1")
                    .field("phoneid", "p1")
                    .endObject()
                    .startObject()
                    .field("stationid", "s2")
                    .field("phoneid", "p2")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("first", "fname5")
                    .field("last", "lname5")
                    .startArray("workstations")
                    .startObject()
                    .field("stationid", "s3")
                    .field("phoneid", "p3")
                    .endObject()
                    .startObject()
                    .field("stationid", "s4")
                    .field("phoneid", "p4")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("first", "fname1")
                    .field("last", "lname1")
                    .startArray("workstations")
                    .startObject()
                    .field("stationid", "s5")
                    .field("phoneid", "p5")
                    .endObject()
                    .startObject()
                    .field("stationid", "s6")
                    .field("phoneid", "p6")
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        assertTrue(indexResponse2.getShardInfo().getSuccessful() > 0);
        refresh();
        indexRandomForConcurrentSearch("test");

        SearchResponse searchResponse = client().prepareSearch("test")
            .addSort(SortBuilders.fieldSort("users.first").setNestedPath("users").order(SortOrder.ASC))
            .addSort(
                SortBuilders.fieldSort("users.first")
                    .order(SortOrder.ASC)
                    .setNestedPath("users")
                    .setNestedFilter(nestedQuery("users.workstations", termQuery("users.workstations.stationid", "s5"), ScoreMode.Avg))
            )
            .get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0].toString(), equalTo("fname1"));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[1].toString(), equalTo("fname1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0].toString(), equalTo("fname1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[1].toString(), equalTo("fname3"));
    }

    public void testCheckFixedBitSetCache() throws Exception {
        boolean loadFixedBitSeLazily = randomBoolean();
        Settings.Builder settingsBuilder = Settings.builder().put(indexSettings()).put("index.refresh_interval", -1);
        if (loadFixedBitSeLazily) {
            settingsBuilder.put("index.load_fixed_bitset_filters_eagerly", false);
        }
        assertAcked(prepareCreate("test").setSettings(settingsBuilder));

        client().prepareIndex("test").setId("0").setSource("field", "value").get();
        client().prepareIndex("test").setId("1").setSource("field", "value").get();
        refresh();
        ensureSearchable("test");
        indexRandomForConcurrentSearch("test");

        // No nested mapping yet, there shouldn't be anything in the fixed bit set cache
        ClusterStatsResponse clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
        assertThat(clusterStatsResponse.getIndicesStats().getSegments().getBitsetMemoryInBytes(), equalTo(0L));

        // Now add nested mapping
        assertAcked(client().admin().indices().preparePutMapping("test").setSource("array1", "type=nested"));

        XContentBuilder builder = jsonBuilder().startObject()
            .startArray("array1")
            .startObject()
            .field("field1", "value1")
            .endObject()
            .endArray()
            .endObject();
        // index simple data
        client().prepareIndex("test").setId("2").setSource(builder).get();
        client().prepareIndex("test").setId("3").setSource(builder).get();
        client().prepareIndex("test").setId("4").setSource(builder).get();
        client().prepareIndex("test").setId("5").setSource(builder).get();
        client().prepareIndex("test").setId("6").setSource(builder).get();
        refresh();
        ensureSearchable("test");

        if (loadFixedBitSeLazily) {
            clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
            assertThat(clusterStatsResponse.getIndicesStats().getSegments().getBitsetMemoryInBytes(), equalTo(0L));

            // only when querying with nested the fixed bitsets are loaded
            SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(nestedQuery("array1", termQuery("array1.field1", "value1"), ScoreMode.Avg))
                .get();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(5L));
        }
        clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
        assertThat(clusterStatsResponse.getIndicesStats().getSegments().getBitsetMemoryInBytes(), greaterThan(0L));

        assertAcked(client().admin().indices().prepareDelete("test"));
        clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
        assertThat(clusterStatsResponse.getIndicesStats().getSegments().getBitsetMemoryInBytes(), equalTo(0L));
    }

    private void assertDocumentCount(String index, long numdocs) {
        IndicesStatsResponse stats = admin().indices().prepareStats(index).clear().setDocs(true).get();
        assertNoFailures(stats);
        assertThat(stats.getIndex(index).getPrimaries().docs.getCount(), is(numdocs));

    }

}
