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

package org.opensearch.action.admin.indices.shards;

import org.apache.lucene.index.CorruptIndexException;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.store.MockFSIndexStore;
import org.opensearch.transport.client.Requests;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class IndicesShardStoreRequestIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockFSIndexStore.TestPlugin.class);
    }

    public void testEmpty() {
        ensureGreen();
        IndicesShardStoresResponse rsp = client().admin().indices().prepareShardStores().get();
        assertThat(rsp.getStoreStatuses().size(), equalTo(0));
    }

    public void testBasic() throws Exception {
        String index = "test";
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "2").put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "1")
            )
        );
        indexRandomData(index);
        ensureGreen(index);

        // no unallocated shards
        IndicesShardStoresResponse response = client().admin().indices().prepareShardStores(index).get();
        assertThat(response.getStoreStatuses().size(), equalTo(0));

        // all shards
        response = client().admin().indices().shardStores(Requests.indicesShardStoresRequest(index).shardStatuses("all")).get();
        assertThat(response.getStoreStatuses().containsKey(index), equalTo(true));
        final Map<Integer, List<IndicesShardStoresResponse.StoreStatus>> shardStores = response.getStoreStatuses().get(index);
        assertThat(shardStores.values().size(), equalTo(2));
        for (var shardStoreStatuses : shardStores.values()) {
            for (IndicesShardStoresResponse.StoreStatus storeStatus : shardStoreStatuses) {
                assertThat(storeStatus.getAllocationId(), notNullValue());
                assertThat(storeStatus.getNode(), notNullValue());
                assertThat(storeStatus.getStoreException(), nullValue());
            }
        }

        // default with unassigned shards
        ensureGreen(index);
        logger.info("--> disable allocation");
        disableAllocation(index);
        logger.info("--> stop random node");
        int num = client().admin().cluster().prepareState().get().getState().nodes().getSize();
        internalCluster().stopRandomNode(new IndexNodePredicate(index));
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes("" + (num - 1)));
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        List<ShardRouting> unassignedShards = clusterState.routingTable().index(index).shardsWithState(ShardRoutingState.UNASSIGNED);
        response = client().admin().indices().shardStores(Requests.indicesShardStoresRequest(index)).get();
        assertThat(response.getStoreStatuses().containsKey(index), equalTo(true));
        final Map<Integer, List<IndicesShardStoresResponse.StoreStatus>> shardStoresStatuses = response.getStoreStatuses().get(index);
        assertThat(shardStoresStatuses.size(), equalTo(unassignedShards.size()));
        for (var storesStatus : shardStoresStatuses.values()) {
            assertThat("must report for one store", storesStatus.size(), equalTo(1));
            assertThat(
                "reported store should be primary",
                storesStatus.get(0).getAllocationStatus(),
                equalTo(IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY)
            );
        }
        logger.info("--> enable allocation");
        enableAllocation(index);
    }

    public void testIndices() throws Exception {
        String index1 = "test1";
        String index2 = "test2";
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(prepareCreate(index1).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "2")));
        assertAcked(prepareCreate(index2).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "2")));
        indexRandomData(index1);
        indexRandomData(index2);
        ensureGreen();
        IndicesShardStoresResponse response = client().admin()
            .indices()
            .shardStores(Requests.indicesShardStoresRequest().shardStatuses("all"))
            .get();
        Map<String, Map<Integer, List<IndicesShardStoresResponse.StoreStatus>>> shardStatuses = response.getStoreStatuses();
        assertThat(shardStatuses.containsKey(index1), equalTo(true));
        assertThat(shardStatuses.containsKey(index2), equalTo(true));
        assertThat(shardStatuses.get(index1).size(), equalTo(2));
        assertThat(shardStatuses.get(index2).size(), equalTo(2));

        // ensure index filtering works
        response = client().admin().indices().shardStores(Requests.indicesShardStoresRequest(index1).shardStatuses("all")).get();
        shardStatuses = response.getStoreStatuses();
        assertThat(shardStatuses.containsKey(index1), equalTo(true));
        assertThat(shardStatuses.containsKey(index2), equalTo(false));
        assertThat(shardStatuses.get(index1).size(), equalTo(2));
    }

    public void testCorruptedShards() throws Exception {
        String index = "test";
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "5")
                    .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), false)
            )
        );

        indexRandomData(index);
        ensureGreen(index);

        logger.info("--> disable allocation");
        disableAllocation(index);

        logger.info("--> corrupt random shard copies");
        Map<Integer, Set<String>> corruptedShardIDMap = new HashMap<>();
        Index idx = resolveIndex(index);
        for (String node : internalCluster().nodesInclude(index)) {
            IndicesService indexServices = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexShards = indexServices.indexServiceSafe(idx);
            for (Integer shardId : indexShards.shardIds()) {
                IndexShard shard = indexShards.getShard(shardId);
                if (randomBoolean()) {
                    logger.debug("--> failing shard [{}] on node [{}]", shardId, node);
                    shard.failShard("test", new CorruptIndexException("test corrupted", ""));
                    logger.debug("--> failed shard [{}] on node [{}]", shardId, node);
                    Set<String> nodes = corruptedShardIDMap.get(shardId);
                    if (nodes == null) {
                        nodes = new HashSet<>();
                    }
                    nodes.add(node);
                    corruptedShardIDMap.put(shardId, nodes);
                }
            }
        }

        assertBusy(() -> { // IndicesClusterStateService#failAndRemoveShard() called asynchronously but we need it to have completed here.
            IndicesShardStoresResponse rsp = client().admin().indices().prepareShardStores(index).setShardStatuses("all").get();
            final Map<Integer, List<IndicesShardStoresResponse.StoreStatus>> shardStatuses = rsp.getStoreStatuses().get(index);
            assertNotNull(shardStatuses);
            assertThat(shardStatuses.size(), greaterThan(0));
            for (var shardStatus : shardStatuses.entrySet()) {
                for (IndicesShardStoresResponse.StoreStatus status : shardStatus.getValue()) {
                    if (corruptedShardIDMap.containsKey(shardStatus.getKey())
                        && corruptedShardIDMap.get(shardStatus.getKey()).contains(status.getNode().getName())) {
                        assertThat(
                            "shard [" + shardStatus.getKey() + "] is failed on node [" + status.getNode().getName() + "]",
                            status.getStoreException(),
                            notNullValue()
                        );
                    } else {
                        assertNull(
                            "shard [" + shardStatus.getKey() + "] is not failed on node [" + status.getNode().getName() + "]",
                            status.getStoreException()
                        );
                    }
                }
            }
        });
        logger.info("--> enable allocation");
        enableAllocation(index);
    }

    private void indexRandomData(String index) throws ExecutionException, InterruptedException {
        int numDocs = scaledRandomIntBetween(10, 20);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(index).setSource("field", "value");
        }
        indexRandom(true, builders);
        client().admin().indices().prepareFlush().setForce(true).execute().actionGet();
    }

    private static final class IndexNodePredicate implements Predicate<Settings> {
        private final Set<String> nodesWithShard;

        IndexNodePredicate(String index) {
            this.nodesWithShard = findNodesWithShard(index);
        }

        @Override
        public boolean test(Settings settings) {
            return nodesWithShard.contains(settings.get("node.name"));
        }

        private Set<String> findNodesWithShard(String index) {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexRoutingTable indexRoutingTable = state.routingTable().index(index);
            List<ShardRouting> startedShards = indexRoutingTable.shardsWithState(ShardRoutingState.STARTED);
            Set<String> nodesWithShard = new HashSet<>();
            for (ShardRouting startedShard : startedShards) {
                nodesWithShard.add(state.nodes().get(startedShard.currentNodeId()).getName());
            }
            return nodesWithShard;
        }
    }
}
