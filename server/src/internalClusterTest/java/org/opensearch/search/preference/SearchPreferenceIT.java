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

package org.opensearch.search.preference;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.node.Node;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.not;

@OpenSearchIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SearchPreferenceIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public SearchPreferenceIT(Settings staticSettings) {
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
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING.getKey(), false)
            .build();
    }

    // see #2896
    public void testStopOneNodePreferenceWithRedState() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder().put("index.number_of_shards", cluster().numDataNodes() + 2).put("index.number_of_replicas", 0)
            )
        );
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test").setId("" + i).setSource("field1", "value1").get();
        }
        refresh();
        indexRandomForConcurrentSearch("test");
        internalCluster().stopRandomDataNode();
        client().admin().cluster().prepareHealth().setWaitForStatus(ClusterHealthStatus.RED).get();
        String[] preferences = new String[] {
            "_primary",
            "_local",
            "_primary_first",
            "_prefer_nodes:somenode",
            "_prefer_nodes:server2",
            "_prefer_nodes:somenode,server2" };
        for (String pref : preferences) {
            logger.info("--> Testing out preference={}", pref);
            SearchResponse searchResponse = client().prepareSearch().setSize(0).setPreference(pref).get();
            assertThat(RestStatus.OK, equalTo(searchResponse.status()));
            assertThat(pref, searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
            searchResponse = client().prepareSearch().setPreference(pref).get();
            assertThat(RestStatus.OK, equalTo(searchResponse.status()));
            assertThat(pref, searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
        }

        // _only_local is a stricter preference, we need to send the request to a data node
        SearchResponse searchResponse = dataNodeClient().prepareSearch().setSize(0).setPreference("_only_local").get();
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("_only_local", searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
        searchResponse = dataNodeClient().prepareSearch().setPreference("_only_local").get();
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("_only_local", searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
    }

    public void testNoPreferenceRandom() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(
                // this test needs at least a replica to make sure two consecutive searches go to two different copies of the same data
                Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(1, maximumNumberOfReplicas()))
            )
        );
        ensureGreen();

        client().prepareIndex("test").setSource("field1", "value1").get();
        refresh();
        indexRandomForConcurrentSearch("test");

        final Client client = internalCluster().smartClient();
        SearchResponse searchResponse = client.prepareSearch("test").setQuery(matchAllQuery()).get();
        String firstNodeId = searchResponse.getHits().getAt(0).getShard().getNodeId();
        searchResponse = client.prepareSearch("test").setQuery(matchAllQuery()).get();
        String secondNodeId = searchResponse.getHits().getAt(0).getShard().getNodeId();

        assertThat(firstNodeId, not(equalTo(secondNodeId)));
    }

    public void testSimplePreference() throws InterruptedException {
        client().admin().indices().prepareCreate("test").setSettings("{\"number_of_replicas\": 1}", MediaTypeRegistry.JSON).get();
        ensureGreen();

        client().prepareIndex("test").setSource("field1", "value1").get();
        refresh();
        indexRandomForConcurrentSearch("test");

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_local").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_primary").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_primary_first").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_replica").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_replica_first").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("1234").execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));

    }

    public void testThatSpecifyingNonExistingNodesReturnsUsefulError() {
        createIndex("test");
        ensureGreen();

        try {
            client().prepareSearch().setQuery(matchAllQuery()).setPreference("_only_nodes:DOES-NOT-EXIST").get();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e, hasToString(containsString("no data nodes with criteria [DOES-NOT-EXIST] found for shard: [test][")));
        }
    }

    public void testNodesOnlyRandom() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(
                // this test needs at least a replica to make sure two consecutive searches go to two different copies of the same data
                Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(1, maximumNumberOfReplicas()))
            )
        );
        ensureGreen();
        client().prepareIndex("test").setSource("field1", "value1").get();
        refresh();
        indexRandomForConcurrentSearch("test");

        final Client client = internalCluster().smartClient();
        // multiple wildchar to cover multi-param usecase
        SearchRequestBuilder request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference("_only_nodes:*,nodes*");
        assertSearchOnRandomNodes(request);

        request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference("_only_nodes:*");
        assertSearchOnRandomNodes(request);

        ArrayList<String> allNodeIds = new ArrayList<>();
        ArrayList<String> allNodeNames = new ArrayList<>();
        ArrayList<String> allNodeHosts = new ArrayList<>();
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().get();
        for (NodeStats node : nodeStats.getNodes()) {
            allNodeIds.add(node.getNode().getId());
            allNodeNames.add(node.getNode().getName());
            allNodeHosts.add(node.getHostname());
        }

        String node_expr = "_only_nodes:" + Strings.arrayToCommaDelimitedString(allNodeIds.toArray());
        request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference(node_expr);
        assertSearchOnRandomNodes(request);

        node_expr = "_only_nodes:" + Strings.arrayToCommaDelimitedString(allNodeNames.toArray());
        request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference(node_expr);
        assertSearchOnRandomNodes(request);

        node_expr = "_only_nodes:" + Strings.arrayToCommaDelimitedString(allNodeHosts.toArray());
        request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference(node_expr);
        assertSearchOnRandomNodes(request);

        node_expr = "_only_nodes:" + Strings.arrayToCommaDelimitedString(allNodeHosts.toArray());
        request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference(node_expr);
        assertSearchOnRandomNodes(request);

        // Mix of valid and invalid nodes
        node_expr = "_only_nodes:*,invalidnode";
        request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference(node_expr);
        assertSearchOnRandomNodes(request);
    }

    private void assertSearchOnRandomNodes(SearchRequestBuilder request) {
        Set<String> hitNodes = new HashSet<>();
        for (int i = 0; i < 2; i++) {
            SearchResponse searchResponse = request.get();
            assertThat(searchResponse.getHits().getHits().length, greaterThan(0));
            hitNodes.add(searchResponse.getHits().getAt(0).getShard().getNodeId());
        }
        assertThat(hitNodes.size(), greaterThan(1));
    }

    public void testCustomPreferenceUnaffectedByOtherShardMovements() throws InterruptedException {

        /*
         * Custom preferences can be used to encourage searches to go to a consistent set of shard copies, meaning that other copies' data
         * is rarely touched and can be dropped from the filesystem cache. This works best if the set of shards searched doesn't change
         * unnecessarily, so this test verifies a consistent routing even as other shards are created/relocated/removed.
         */

        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put(SETTING_NUMBER_OF_REPLICAS, between(1, maximumNumberOfReplicas()))
                    .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            )
        );
        ensureGreen();
        client().prepareIndex("test").setSource("field1", "value1").get();
        refresh();
        indexRandomForConcurrentSearch("test");

        final String customPreference = randomAlphaOfLength(10);

        final String nodeId = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .setPreference(customPreference)
            .get()
            .getHits()
            .getAt(0)
            .getShard()
            .getNodeId();

        assertSearchesSpecificNode("test", customPreference, nodeId);

        final int replicasInNewIndex = between(1, maximumNumberOfReplicas());
        assertAcked(
            prepareCreate("test2").setSettings(Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, replicasInNewIndex))
        );
        ensureGreen();
        indexRandomForConcurrentSearch("test2");

        assertSearchesSpecificNode("test", customPreference, nodeId);

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test2")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, replicasInNewIndex - 1))
        );

        assertSearchesSpecificNode("test", customPreference, nodeId);

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test2")
                .setSettings(
                    Settings.builder()
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(
                            IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name",
                            internalCluster().getDataNodeInstance(Node.class).settings().get(Node.NODE_NAME_SETTING.getKey())
                        )
                )
        );

        ensureGreen();

        assertSearchesSpecificNode("test", customPreference, nodeId);

        assertAcked(client().admin().indices().prepareDelete("test2"));

        assertSearchesSpecificNode("test", customPreference, nodeId);
    }

    private static void assertSearchesSpecificNode(String index, String customPreference, String nodeId) {
        final SearchResponse searchResponse = client().prepareSearch(index).setQuery(matchAllQuery()).setPreference(customPreference).get();
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getShard().getNodeId(), equalTo(nodeId));
    }
}
