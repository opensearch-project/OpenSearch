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

package org.opensearch.search.stats;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.search.stats.SearchStats.Stats;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.opensearch.action.search.SearchRequestStats.SEARCH_REQUEST_STATS_ENABLED_KEY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAllSuccessful;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, minNumDataNodes = 2)
public class SearchStatsIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public SearchStatsIT(Settings staticSettings) {
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
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("_source.field", vars -> {
                Map<?, ?> src = (Map) vars.get("_source");
                return src.get("field");
            });
        }
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testSimpleStats() throws Exception {
        // clear all stats first
        client().admin().indices().prepareStats().clear().get();
        final int numNodes = cluster().numDataNodes();
        assertThat(numNodes, greaterThanOrEqualTo(2));
        final int shardsIdx1 = randomIntBetween(1, 10); // we make sure each node gets at least a single shard...
        final int shardsIdx2 = Math.max(numNodes - shardsIdx1, randomIntBetween(1, 10));
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(SEARCH_REQUEST_STATS_ENABLED_KEY, true).build())
            .get();
        assertThat(numNodes, lessThanOrEqualTo(shardsIdx1 + shardsIdx2));
        assertAcked(
            prepareCreate("test1").setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, shardsIdx1).put(SETTING_NUMBER_OF_REPLICAS, 0)
            )
        );
        int docsTest1 = scaledRandomIntBetween(3 * shardsIdx1, 5 * shardsIdx1);
        for (int i = 0; i < docsTest1; i++) {
            client().prepareIndex("test1").setId(Integer.toString(i)).setSource("field", "value").get();
            if (rarely()) {
                refresh();
            }
        }
        assertAcked(
            prepareCreate("test2").setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, shardsIdx2).put(SETTING_NUMBER_OF_REPLICAS, 0)
            )
        );
        int docsTest2 = scaledRandomIntBetween(3 * shardsIdx2, 5 * shardsIdx2);
        for (int i = 0; i < docsTest2; i++) {
            client().prepareIndex("test2").setId(Integer.toString(i)).setSource("field", "value").get();
            if (rarely()) {
                refresh();
            }
        }
        assertThat(shardsIdx1 + shardsIdx2, equalTo(numAssignedShards("test1", "test2")));
        assertThat(numAssignedShards("test1", "test2"), greaterThanOrEqualTo(2));
        // THERE WILL BE AT LEAST 2 NODES HERE SO WE CAN WAIT FOR GREEN
        ensureGreen();
        refresh();
        int iters = scaledRandomIntBetween(100, 150);
        for (int i = 0; i < iters; i++) {
            SearchResponse searchResponse = internalCluster().coordOnlyNodeClient()
                .prepareSearch()
                .setQuery(QueryBuilders.termQuery("field", "value"))
                .setStats("group1", "group2")
                .highlighter(new HighlightBuilder().field("field"))
                .addScriptField("script1", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_source.field", Collections.emptyMap()))
                .setSize(100)
                .get();
            assertHitCount(searchResponse, docsTest1 + docsTest2);
            assertAllSuccessful(searchResponse);
        }

        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats().get();
        logger.debug("###### indices search stats: {}", indicesStats.getTotal().getSearch());
        assertThat(indicesStats.getTotal().getSearch().getTotal().getQueryCount(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getQueryTimeInMillis(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getFetchCount(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getFetchTimeInMillis(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats(), nullValue());

        indicesStats = client().admin().indices().prepareStats().setGroups("group1").get();
        assertThat(indicesStats.getTotal().getSearch().getGroupStats(), notNullValue());
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getQueryCount(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getQueryTimeInMillis(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getFetchCount(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getFetchTimeInMillis(), greaterThan(0L));
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().get();

        Set<String> nodeIdsWithIndex = nodeIdsWithIndex("test1", "test2");
        int num = 0;
        int numOfCoordinators = 0;

        for (NodeStats stat : nodeStats.getNodes()) {
            Stats total = stat.getIndices().getSearch().getTotal();
            if (total.getRequestStatsLongHolder().getRequestStatsHolder().get(SearchPhaseName.QUERY.getName()).getTimeInMillis() > 0) {
                assertThat(
                    total.getRequestStatsLongHolder().getRequestStatsHolder().get(SearchPhaseName.FETCH.getName()).getTimeInMillis(),
                    greaterThan(0L)
                );
                assertEquals(
                    iters,
                    total.getRequestStatsLongHolder().getRequestStatsHolder().get(SearchPhaseName.FETCH.getName()).getTotal()
                );
                assertEquals(
                    iters,
                    total.getRequestStatsLongHolder().getRequestStatsHolder().get(SearchPhaseName.EXPAND.getName()).getTotal()
                );
                assertEquals(
                    iters,
                    total.getRequestStatsLongHolder().getRequestStatsHolder().get(SearchPhaseName.FETCH.getName()).getTotal()
                );
                numOfCoordinators += 1;
            }
            if (nodeIdsWithIndex.contains(stat.getNode().getId())) {
                assertThat(total.getQueryCount(), greaterThan(0L));
                assertThat(total.getQueryTimeInMillis(), greaterThan(0L));
                num++;
            } else {
                assertThat(total.getQueryCount(), greaterThanOrEqualTo(0L));
                assertThat(total.getQueryTimeInMillis(), equalTo(0L));
            }
        }
        assertThat(numOfCoordinators, greaterThan(0));
        assertThat(num, greaterThan(0));
    }

    private Set<String> nodeIdsWithIndex(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator<ShardIterator> allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        Set<String> nodes = new HashSet<>();
        for (ShardIterator shardIterator : allAssignedShardsGrouped) {
            for (ShardRouting routing : shardIterator) {
                if (routing.active()) {
                    nodes.add(routing.currentNodeId());
                }

            }
        }
        return nodes;
    }

    public void testOpenContexts() {
        String index = "test1";
        createIndex(index);
        ensureGreen(index);

        // create shards * docs number of docs and attempt to distribute them equally
        // this distribution will not be perfect; each shard will have an integer multiple of docs (possibly zero)
        // we do this so we have a lot of pages to scroll through
        final int docs = scaledRandomIntBetween(20, 50);
        for (int s = 0; s < numAssignedShards(index); s++) {
            for (int i = 0; i < docs; i++) {
                client().prepareIndex(index)
                    .setId(Integer.toString(s * docs + i))
                    .setSource("field", "value")
                    .setRouting(Integer.toString(s))
                    .get();
            }
        }
        client().admin().indices().prepareRefresh(index).get();

        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats(index).get();
        assertThat(indicesStats.getTotal().getSearch().getOpenContexts(), equalTo(0L));

        int size = scaledRandomIntBetween(1, docs);
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(size)
            .setScroll(TimeValue.timeValueMinutes(2))
            .get();
        assertSearchResponse(searchResponse);

        // refresh the stats now that scroll contexts are opened
        indicesStats = client().admin().indices().prepareStats(index).get();

        assertThat(indicesStats.getTotal().getSearch().getOpenContexts(), equalTo((long) numAssignedShards(index)));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getScrollCurrent(), equalTo((long) numAssignedShards(index)));

        int hits = 0;
        while (true) {
            if (searchResponse.getHits().getHits().length == 0) {
                break;
            }
            hits += searchResponse.getHits().getHits().length;
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)).get();
        }
        long expected = 0;

        // the number of queries executed is equal to at least the sum of number of pages in shard over all shards
        IndicesStatsResponse r = client().admin().indices().prepareStats(index).get();
        for (int s = 0; s < numAssignedShards(index); s++) {
            expected += (long) Math.ceil(r.getShards()[s].getStats().getDocs().getCount() / size);
        }
        indicesStats = client().admin().indices().prepareStats().get();
        Stats stats = indicesStats.getTotal().getSearch().getTotal();
        assertEquals(hits, docs * numAssignedShards(index));
        assertThat(stats.getQueryCount(), greaterThanOrEqualTo(expected));

        clearScroll(searchResponse.getScrollId());

        indicesStats = client().admin().indices().prepareStats().get();
        stats = indicesStats.getTotal().getSearch().getTotal();
        assertThat(indicesStats.getTotal().getSearch().getOpenContexts(), equalTo(0L));
        assertThat(stats.getScrollCount(), equalTo((long) numAssignedShards(index)));
        assertThat(stats.getScrollTimeInMillis(), greaterThan(0L));
    }

    protected int numAssignedShards(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        return allAssignedShardsGrouped.size();
    }
}
