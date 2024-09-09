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

package org.opensearch.cluster.coordination;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.MockEngineFactoryPlugin;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.tasks.Task;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.store.MockFSIndexStore;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.test.transport.StubbableTransport;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_ACTION_NAME;
import static org.hamcrest.Matchers.is;

/**
 ITs for reproducing the scenario and validating some new cases for https://github.com/opensearch-project/OpenSearch/issues/4874
 This issue is about an infinite loop of node joining and leaving the cluster.
 This race condition would happen when a node-join task was queued into cluster-manager thread while a node-left task for the same node was still processing.

 Scenario:
 Suppose a node disconnects from the cluster due to some normal reason.
 This queues a node-left task on cluster manager thread.
 Then cluster manager then computes the new cluster state based on the node-left task.
 The cluster manager now tries to send the new state to all the nodes and waits for all nodes to ack back.
 Suppose this takes a long time due to lagging nodes or slow applying of the state or any other reason.
 While this is happening, the node that just left sends a join request to the cluster manager to rejoin the cluster.
 The role of this join request is to re-establish any required connections and do some pre-validations before queuing a new task.
 After join request is validated by cluster manager node, cluster manager queues a node-join task into its thread.
 This node-join task would only start after the node-left task is completed.
 Now suppose the node-left task has completed publication and has started to apply the new state on the cluster manager.
 As part of applying the cluster state of node-left task, cluster manager wipes out the connection info of the leaving node.
 The node-left task then completes and the node-join task begins.
 Now the node-join task starts. This task assumes that because the previous join request succeeded, that all connection info would still be there.
 So then the cluster manager computes the new state.
 Then it tells the followerchecker thread to add this new node.
 Then it tries to publish the new state to all the nodes.
 However, at this point, the followerchecker thread fails because the connection was wiped and triggers a new node-left task.
 If the new node-left task also takes time, we end up in an infinite loop of node-left and node-joins.

Fix:
 As part of the fix for this, we now reject the initial join request from a node that has an ongoing node-left task.
 The join request will only succeed after the node-left task completes, so the connection that gets created as part of the join request does not get wiped out and cause node-join task to fail.
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class NodeJoinLeftIT extends OpenSearchIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            MockTransportService.TestPlugin.class,
            MockFSIndexStore.TestPlugin.class,
            InternalSettingsPlugin.class,
            MockEngineFactoryPlugin.class
        );
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        super.beforeIndexDeletion();
        internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
        internalCluster().assertSeqNos();
        internalCluster().assertSameDocIdsOnShards();
    }

    public void testClusterStabilityWhenJoinRequestHappensDuringNodeLeftTask() throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "10s")
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "200ms")
            .put(FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "100ms")
            .build();
        // start a 3 node cluster with 1 cluster-manager
        final String cm = internalCluster().startNode(nodeSettings);
        internalCluster().startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .get();

        ClusterService cmClsService = internalCluster().getInstance(ClusterService.class, cm);
        // Simulate a slow applier on the cm to delay node-left state application
        cmClsService.addStateApplier(event -> {
            if (event.nodesRemoved()) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        // Toggle to succeed/fail the followerchecker to simulate the initial node leaving.
        AtomicBoolean succeedFollowerChecker = new AtomicBoolean();

        // Simulate followerchecker failure on 1 node when succeedFollowerChecker is false
        FollowerCheckerBehaviour simulatedFailureBehaviour = new FollowerCheckerBehaviour(() -> {
            if (succeedFollowerChecker.get()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            throw new NodeHealthCheckFailureException("fake followerchecker failure simulated by test to repro race condition");
        });
        MockTransportService redTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            redNodeName
        );
        redTransportService.addRequestHandlingBehavior(FOLLOWER_CHECK_ACTION_NAME, simulatedFailureBehaviour);

        // Loop runs 10 times to ensure race condition gets reproduced
        for (int i = 0; i < 10; i++) {
            // Fail followerchecker by force to trigger node disconnect and node left
            logger.info("--> simulating followerchecker failure to trigger node-left");
            succeedFollowerChecker.set(false);
            ClusterHealthResponse response1 = client().admin().cluster().prepareHealth().setWaitForNodes("2").get();
            assertThat(response1.isTimedOut(), is(false));

            // once we know a node has left, we can re-enable followerchecker to work normally and validate node rejoins
            logger.info("--> re-enabling normal followerchecker and validating cluster is stable");
            succeedFollowerChecker.set(true);
            ClusterHealthResponse response2 = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
            assertThat(response2.isTimedOut(), is(false));

            Thread.sleep(1000);
            // checking again to validate stability and ensure node did not leave
            ClusterHealthResponse response3 = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
            assertThat(response3.isTimedOut(), is(false));
        }

        succeedFollowerChecker.set(true);
        response = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        assertThat(response.isTimedOut(), is(false));
    }

    public void testClusterStabilityWhenDisconnectDuringSlowNodeLeftTask() throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "10s")
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "200ms")
            .put(FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "100ms")
            .build();
        // start a 3 node cluster with 1 cluster-manager
        final String cm = internalCluster().startNode(nodeSettings);
        internalCluster().startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .get();

        ClusterService cmClsService = internalCluster().getInstance(ClusterService.class, cm);
        // Simulate a slow applier on the cm to delay node-left state application
        cmClsService.addStateApplier(event -> {
            if (event.nodesRemoved()) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        // Toggle to succeed/fail the followerchecker to simulate the initial node leaving.
        AtomicBoolean succeedFollowerChecker = new AtomicBoolean();

        // Simulate followerchecker failure on 1 node when succeedFollowerChecker is false
        FollowerCheckerBehaviour simulatedFailureBehaviour = new FollowerCheckerBehaviour(() -> {
            if (succeedFollowerChecker.get()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            throw new NodeHealthCheckFailureException("fake followerchecker failure simulated by test to repro race condition");
        });
        MockTransportService cmTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, cm);
        MockTransportService redTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            redNodeName
        );
        redTransportService.addRequestHandlingBehavior(FOLLOWER_CHECK_ACTION_NAME, simulatedFailureBehaviour);

        // Loop runs 10 times to ensure race condition gets reproduced
        for (int i = 0; i < 10; i++) {
            // Fail followerchecker by force to trigger node disconnect and node left
            logger.info("--> simulating followerchecker failure to trigger node-left");
            succeedFollowerChecker.set(false);
            Thread.sleep(1000);

            // Trigger a node disconnect while node-left task is still processing
            logger.info(
                "--> triggering a simulated disconnect on red node, after the follower checker failed to see how node-left task deals with this"
            );
            cmTransportService.disconnectFromNode(redTransportService.getLocalDiscoNode());

            ClusterHealthResponse response1 = client().admin().cluster().prepareHealth().setWaitForNodes("2").get();
            assertThat(response1.isTimedOut(), is(false));

            // once we know a node has left, we can re-enable followerchecker to work normally and validate node rejoins
            logger.info("--> re-enabling normal followerchecker and validating cluster is stable");
            succeedFollowerChecker.set(true);
            ClusterHealthResponse response2 = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
            assertThat(response2.isTimedOut(), is(false));

            Thread.sleep(1000);
            // checking again to validate stability and ensure node did not leave
            ClusterHealthResponse response3 = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
            assertThat(response3.isTimedOut(), is(false));
        }

        succeedFollowerChecker.set(true);
        response = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        assertThat(response.isTimedOut(), is(false));
    }

    public void testRestartDataNode() throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "10s")
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "200ms")
            .put(FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .build();
        // start a 3 node cluster
        internalCluster().startNode(nodeSettings);
        internalCluster().startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .get();

        Settings redNodeDataPathSettings = internalCluster().dataPathSettings(redNodeName);
        logger.info("-> stopping data node");
        internalCluster().stopRandomNode(settings -> settings.get("node.name").equals(redNodeName));
        response = client().admin().cluster().prepareHealth().setWaitForNodes("2").get();
        assertThat(response.isTimedOut(), is(false));

        logger.info("-> restarting stopped node");
        internalCluster().startNode(Settings.builder().put("node.name", redNodeName).put(redNodeDataPathSettings).build());
        response = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        assertThat(response.isTimedOut(), is(false));
    }

    public void testRestartCmNode() throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "10s")
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "200ms")
            .put(FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .build();
        // start a 3 node cluster
        final String cm = internalCluster().startNode(Settings.builder().put("node.attr.color", "yellow").put(nodeSettings).build());
        internalCluster().startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .get();

        Settings cmNodeSettings = internalCluster().dataPathSettings(cm);

        logger.info("-> stopping cluster-manager node");
        internalCluster().stopRandomNode(settings -> settings.get("node.name").equals(cm));
        response = client().admin().cluster().prepareHealth().setWaitForNodes("2").get();
        assertThat(response.isTimedOut(), is(false));

        logger.info("-> restarting stopped node");
        internalCluster().startNode(Settings.builder().put("node.name", cm).put(cmNodeSettings).build());
        response = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        assertThat(response.isTimedOut(), is(false));
    }

    private class FollowerCheckerBehaviour implements StubbableTransport.RequestHandlingBehavior<TransportRequest> {
        private final Runnable connectionBreaker;

        private FollowerCheckerBehaviour(Runnable connectionBreaker) {
            this.connectionBreaker = connectionBreaker;
        }

        @Override
        public void messageReceived(
            TransportRequestHandler<TransportRequest> handler,
            TransportRequest request,
            TransportChannel channel,
            Task task
        ) throws Exception {

            connectionBreaker.run();
            handler.messageReceived(request, channel, task);
        }
    }
}
