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

package org.opensearch.cluster;

import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.coordination.NoClusterManagerBlockService;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class MinimumClusterManagerNodesIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final HashSet<Class<? extends Plugin>> classes = new HashSet<>(super.nodePlugins());
        classes.add(MockTransportService.TestPlugin.class);
        return classes;
    }

    public void testTwoNodesNoClusterManagerBlock() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(1);

        Settings settings = Settings.builder().put("discovery.initial_state_timeout", "500ms").build();

        logger.info("--> start first node");
        String node1Name = internalCluster().startNode(settings);

        logger.info("--> should be blocked, no cluster-manager...");
        ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID), equalTo(true));
        assertThat(state.nodes().getSize(), equalTo(1)); // verify that we still see the local node in the cluster state

        logger.info("--> start second node, cluster should be formed");
        String node2Name = internalCluster().startNode(settings);

        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("2")
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().getSize(), equalTo(2));
        assertThat(state.metadata().indices().containsKey("test"), equalTo(false));

        createIndex("test");
        NumShards numShards = getNumShards("test");
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        // make sure that all shards recovered before trying to flush
        assertThat(
            client().admin()
                .cluster()
                .prepareHealth("test")
                .setWaitForActiveShards(numShards.totalNumShards)
                .execute()
                .actionGet()
                .getActiveShards(),
            equalTo(numShards.totalNumShards)
        );
        // flush for simpler debugging
        flushAndRefresh();

        logger.info("--> verify we get the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(
                client().prepareSearch()
                    .setSize(0)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value(),
                equalTo(100L)
            );
        }

        String clusterManagerNode = internalCluster().getClusterManagerName();
        String otherNode = node1Name.equals(clusterManagerNode) ? node2Name : node1Name;
        logger.info("--> add voting config exclusion for non-cluster-manager node, to be sure it's not elected");
        client().execute(AddVotingConfigExclusionsAction.INSTANCE, new AddVotingConfigExclusionsRequest(otherNode)).get();
        logger.info("--> stop cluster-manager node, no cluster-manager block should appear");
        Settings clusterManagerDataPathSettings = internalCluster().dataPathSettings(clusterManagerNode);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(clusterManagerNode));

        assertBusy(() -> {
            ClusterState clusterState = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertTrue(clusterState.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID));
        });

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID), equalTo(true));
        // verify that both nodes are still in the cluster state but there is no cluster-manager
        assertThat(state.nodes().getSize(), equalTo(2));
        assertThat(state.nodes().getClusterManagerNode(), equalTo(null));

        logger.info("--> starting the previous cluster-manager node again...");
        node2Name = internalCluster().startNode(Settings.builder().put(settings).put(clusterManagerDataPathSettings).build());

        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForYellowStatus()
            .setWaitForNodes("2")
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().getSize(), equalTo(2));
        assertThat(state.metadata().indices().containsKey("test"), equalTo(true));

        ensureGreen();

        logger.info("--> verify we get the data back after cluster reform");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 100);
        }

        logger.info("--> clearing voting config exclusions");
        ClearVotingConfigExclusionsRequest clearRequest = new ClearVotingConfigExclusionsRequest();
        clearRequest.setWaitForRemoval(false);
        client().execute(ClearVotingConfigExclusionsAction.INSTANCE, clearRequest).get();

        clusterManagerNode = internalCluster().getClusterManagerName();
        otherNode = node1Name.equals(clusterManagerNode) ? node2Name : node1Name;
        logger.info("--> add voting config exclusion for cluster-manager node, to be sure it's not elected");
        client().execute(AddVotingConfigExclusionsAction.INSTANCE, new AddVotingConfigExclusionsRequest(clusterManagerNode)).get();
        logger.info("--> stop non-cluster-manager node, no cluster-manager block should appear");
        Settings otherNodeDataPathSettings = internalCluster().dataPathSettings(otherNode);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(otherNode));

        assertBusy(() -> {
            ClusterState state1 = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertThat(state1.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID), equalTo(true));
        });

        logger.info("--> starting the previous cluster-manager node again...");
        internalCluster().startNode(Settings.builder().put(settings).put(otherNodeDataPathSettings).build());

        ensureGreen();
        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("2")
            .setWaitForGreenStatus()
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().getSize(), equalTo(2));
        assertThat(state.metadata().indices().containsKey("test"), equalTo(true));

        logger.info("Running Cluster Health");
        ensureGreen();

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 100);
        }
    }

    public void testThreeNodesNoClusterManagerBlock() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(2);

        Settings settings = Settings.builder().put("discovery.initial_state_timeout", "500ms").build();

        logger.info("--> start first 2 nodes");
        internalCluster().startNodes(2, settings);

        ClusterState state;

        assertBusy(() -> {
            for (Client client : clients()) {
                ClusterState state1 = client.admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                assertThat(state1.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID), equalTo(true));
            }
        });

        logger.info("--> start one more node");
        internalCluster().startNode(settings);

        ensureGreen();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("3")
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().getSize(), equalTo(3));

        createIndex("test");
        NumShards numShards = getNumShards("test");
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        ensureGreen();
        // make sure that all shards recovered before trying to flush
        assertThat(
            client().admin()
                .cluster()
                .prepareHealth("test")
                .setWaitForActiveShards(numShards.totalNumShards)
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );
        // flush for simpler debugging
        client().admin().indices().prepareFlush().execute().actionGet();

        refresh();
        logger.info("--> verify we get the data back");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 100);
        }

        List<String> nonClusterManagerNodes = new ArrayList<>(
            Sets.difference(
                Sets.newHashSet(internalCluster().getNodeNames()),
                Collections.singleton(internalCluster().getClusterManagerName())
            )
        );
        Settings nonClusterManagerDataPathSettings1 = internalCluster().dataPathSettings(nonClusterManagerNodes.get(0));
        Settings nonClusterManagerDataPathSettings2 = internalCluster().dataPathSettings(nonClusterManagerNodes.get(1));
        internalCluster().stopRandomNodeNotCurrentClusterManager();
        internalCluster().stopRandomNodeNotCurrentClusterManager();

        logger.info("--> verify that there is no cluster-manager anymore on remaining node");
        // spin here to wait till the state is set
        assertBusy(() -> {
            ClusterState st = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertThat(st.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID), equalTo(true));
        });

        logger.info("--> start back the 2 nodes ");
        internalCluster().startNodes(nonClusterManagerDataPathSettings1, nonClusterManagerDataPathSettings2);

        internalCluster().validateClusterFormed();
        ensureGreen();

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().getSize(), equalTo(3));

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 100);
        }
    }

    public void testCannotCommitStateThreeNodes() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(2);

        Settings settings = Settings.builder().put("discovery.initial_state_timeout", "500ms").build();

        internalCluster().startNodes(3, settings);
        ensureStableCluster(3);

        final String clusterManager = internalCluster().getClusterManagerName();
        Set<String> otherNodes = new HashSet<>(Arrays.asList(internalCluster().getNodeNames()));
        otherNodes.remove(clusterManager);
        NetworkDisruption partition = isolateClusterManagerDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(partition);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> failure = new AtomicReference<>();
        logger.debug("--> submitting for cluster state to be rejected");
        final ClusterService clusterManagerClusterService = internalCluster().clusterService(clusterManager);
        clusterManagerClusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                logger.debug("--> starting the disruption, preventing cluster state publishing");
                partition.startDisrupting();
                Metadata.Builder metadata = Metadata.builder(currentState.metadata())
                    .persistentSettings(
                        Settings.builder().put(currentState.metadata().persistentSettings()).put("_SHOULD_NOT_BE_THERE_", true).build()
                    );
                return ClusterState.builder(currentState).metadata(metadata).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                failure.set(e);
                latch.countDown();
            }
        });

        logger.debug("--> waiting for cluster state to be processed/rejected");
        latch.await();

        assertThat(failure.get(), instanceOf(FailedToCommitClusterStateException.class));

        logger.debug("--> check that there is no cluster-manager in minor partition");
        assertBusy(() -> assertThat(clusterManagerClusterService.state().nodes().getClusterManagerNode(), nullValue()));

        // let major partition to elect new cluster-manager, to ensure that old cluster-manager is not elected once partition is restored,
        // otherwise persistent setting (which is a part of accepted state on old cluster-manager) will be propagated to other nodes
        logger.debug("--> wait for cluster-manager to be elected in major partition");
        assertBusy(() -> {
            DiscoveryNode clusterManagerNode = internalCluster().client(randomFrom(otherNodes))
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getClusterManagerNode();
            assertThat(clusterManagerNode, notNullValue());
            assertThat(clusterManagerNode.getName(), not(equalTo(clusterManager)));
        });

        partition.stopDisrupting();

        logger.debug("--> waiting for cluster to heal");
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes("3").setWaitForEvents(Priority.LANGUID));

        for (String node : internalCluster().getNodeNames()) {
            Settings nodeSetting = internalCluster().clusterService(node).state().metadata().settings();
            assertThat(
                node + " processed the cluster state despite of a min cluster-manager node violation",
                nodeSetting.get("_SHOULD_NOT_BE_THERE_"),
                nullValue()
            );
        }

    }
}
