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
 Check https://github.com/opensearch-project/OpenSearch/issues/4874 and
 https://github.com/opensearch-project/OpenSearch/pull/15521 for context
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
