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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.cluster.service.ClusterApplierService;
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
import org.opensearch.test.TestLogsAppender;
import org.opensearch.test.store.MockFSIndexStore;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.test.transport.StubbableTransport;
import org.opensearch.transport.ClusterConnectionManager;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.opensearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_ACTION_NAME;
import static org.hamcrest.Matchers.is;

/**
 Check https://github.com/opensearch-project/OpenSearch/issues/4874 and
 https://github.com/opensearch-project/OpenSearch/pull/15521 for context
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class NodeJoinLeftIT extends OpenSearchIntegTestCase {

    private TestLogsAppender testLogsAppender;
    private String clusterManager;
    private String redNodeName;
    private Settings nodeSettings;
    private LoggerContext loggerContext;

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

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Add any other specific messages you want to capture
        List<String> messagesToCapture = Arrays.asList(
            "failed to join",
            "IllegalStateException",
            "NodeRemovalClusterStateTaskExecutor",
            ""
        );
        testLogsAppender = new TestLogsAppender(messagesToCapture);
        loggerContext = (LoggerContext) LogManager.getContext(false);
        Configuration config = loggerContext.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(ClusterConnectionManager.class.getName());
        loggerConfig.addAppender(testLogsAppender, null, null);
        loggerContext.updateLoggers();

        String indexName = "test";
        this.nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "10s")
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "200ms")
            .put(FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "100ms")
            .build();
        // start a 3 node cluster with 1 cluster-manager
        this.clusterManager = internalCluster().startNode(nodeSettings);
        internalCluster().startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        this.redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        // validate the 3 node cluster is up
        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        // create an index only if it doesn't exist
        if (!client().admin().indices().prepareExists(indexName).get().isExists()) {
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
        }
    }

    @After
    public void tearDown() throws Exception {
        testLogsAppender.clearCapturedLogs();
        loggerContext = (LoggerContext) LogManager.getContext(false);
        Configuration config = loggerContext.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(ClusterConnectionManager.class.getName());
        loggerConfig.removeAppender(testLogsAppender.getName());
        loggerContext.updateLoggers();
        super.tearDown();
    }

    public void testClusterStabilityWhenJoinRequestHappensDuringNodeLeftTask() throws Exception {

        ClusterService clusterManagerClsService = internalCluster().getInstance(ClusterService.class, clusterManager);
        // Simulate a slow applier on the cm to delay node-left state application
        clusterManagerClsService.addStateApplier(event -> {
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

        // Loop runs 5 times to ensure race condition gets reproduced
        testLogsAppender.clearCapturedLogs();
        for (int i = 0; i < 5; i++) {
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
        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        assertThat(response.isTimedOut(), is(false));

        // assert that join requests fail with the right exception
        boolean logFound = testLogsAppender.waitForLog("failed to join", 30, TimeUnit.SECONDS)
            && testLogsAppender.waitForLog(
                "IllegalStateException[cannot make a new connection as disconnect to node",
                30,
                TimeUnit.SECONDS
            );
        assertTrue("Expected log was not found within the timeout period", logFound);
    }

    public void testClusterStabilityWhenDisconnectDuringSlowNodeLeftTask() throws Exception {
        ClusterService clusterManagerClsService = internalCluster().getInstance(ClusterService.class, clusterManager);
        // Simulate a slow applier on the cm to delay node-left state application
        clusterManagerClsService.addStateApplier(event -> {
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
        MockTransportService cmTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            clusterManager
        );
        MockTransportService redTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            redNodeName
        );
        redTransportService.addRequestHandlingBehavior(FOLLOWER_CHECK_ACTION_NAME, simulatedFailureBehaviour);

        // Loop runs 5 times to ensure race condition gets reproduced
        testLogsAppender.clearCapturedLogs();
        for (int i = 0; i < 5; i++) {
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
        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        assertThat(response.isTimedOut(), is(false));

        // assert that join requests fail with the right exception
        boolean logFound = testLogsAppender.waitForLog("failed to join", 30, TimeUnit.SECONDS);
        assertTrue("Expected log was not found within the timeout period", logFound);
        logFound = testLogsAppender.waitForLog(
            "IllegalStateException[cannot make a new connection as disconnect to node",
            30,
            TimeUnit.SECONDS
        );
        assertTrue("Expected log was not found within the timeout period", logFound);
    }

    public void testClusterStabilityWhenClusterStatePublicationLagsWithLongRunningTaskOnApplierThread() throws Exception {
        internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());
        internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());
        internalCluster().startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        internalCluster().startNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        internalCluster().client().admin().cluster().prepareHealth().setWaitForNodes("7").get();
        for (int i = 0; i < 2; i++) {
            String indexName = "index-" + i;
            internalCluster().client()
                .admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                )
                .get();
        }
        internalCluster().client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
        Settings settings = Settings.builder().put("cluster.follower_lag.timeout", "5s").put("cluster.publish.timeout", "15s").build();
        settingsRequest.transientSettings(settings);
        internalCluster().client().admin().cluster().updateSettings(settingsRequest).actionGet();

        Iterable<ClusterService> dataNodeClusterServices = internalCluster().getDataNodeInstances(ClusterService.class);
        List<ClusterApplierService> dataNodeClusterApplierService = new ArrayList<>();
        dataNodeClusterServices.forEach(clusterService -> dataNodeClusterApplierService.add(clusterService.getClusterApplierService()));
        Map<ClusterApplierService, ClusterStateListener> clusterApplierServiceClusterListenerMap = new HashMap<>();
        AtomicLong previousVersion = new AtomicLong(-1);
        AtomicInteger sleepCounter = new AtomicInteger(0);
        dataNodeClusterApplierService.forEach(dataNodeClusterApplierServiceInstance -> {
            ClusterStateListener clusterStateListener = new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {

                    long currentVersion = event.state().version();

                    // Reset counter for new cluster state version
                    if (previousVersion.compareAndSet(-1, currentVersion) || previousVersion.get() != currentVersion) {
                        sleepCounter.set(0);
                        previousVersion.set(currentVersion);
                    }

                    Random random = new Random();
                    if (random.nextBoolean() && sleepCounter.getAndIncrement() < 2) {
                        dataNodeClusterApplierServiceInstance.runOnApplierThread("NodeJoinLeftIT", clusterState -> {
                            try {
                                logger.info("Sleeping for 30 seconds");
                                Thread.sleep(30 * 1000);
                            } catch (InterruptedException e) {
                                logger.info("Interrupted while waiting for cluster state applier");
                            }
                        }, (source, e) -> logger.error(() -> new ParameterizedMessage("{} unexpected error in listener wait", e), e));
                    }
                }
            };
            clusterApplierServiceClusterListenerMap.put(dataNodeClusterApplierServiceInstance, clusterStateListener);
            dataNodeClusterApplierServiceInstance.addListener(clusterStateListener);
        });

        testLogsAppender.clearCapturedLogs();
        ShardMover shardMover = new ShardMover();
        Thread shufflerThread = new Thread(shardMover);
        shufflerThread.start();
        Thread.sleep(45 * 1000);
        shardMover.setMoveShards(false);
        shufflerThread.join();
        dataNodeClusterApplierService.forEach(dataNodeClusterApplierServiceInstance -> {
            dataNodeClusterApplierServiceInstance.removeListener(
                clusterApplierServiceClusterListenerMap.get(dataNodeClusterApplierServiceInstance)
            );
        });
        internalCluster().client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("7").get();

        testLogsAppender.waitForLog(
            "Tasks batched with key: org.opensearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor",
            30,
            TimeUnit.SECONDS
        );
        boolean logFound = testLogsAppender.waitForLog(
            "Tasks batched with key: org.opensearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor",
            30,
            TimeUnit.SECONDS
        ) && testLogsAppender.waitForLog("reason: lagging", 30, TimeUnit.SECONDS);
        assertTrue("Expected log was not found within the timeout period", logFound);
        // assert that join requests fail with the right exception
        logFound = testLogsAppender.waitForLog("failed to join", 30, TimeUnit.SECONDS)
            && testLogsAppender.waitForLog(
                "IllegalStateException[cannot make a new connection as disconnect to node",
                30,
                TimeUnit.SECONDS
            );
        assertTrue("Expected log was not found within the timeout period", logFound);
    }

    public void testRestartDataNode() throws Exception {

        Settings redNodeDataPathSettings = internalCluster().dataPathSettings(redNodeName);
        logger.info("-> stopping data node");
        internalCluster().stopRandomNode(settings -> settings.get("node.name").equals(redNodeName));
        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes("2").get();
        assertThat(response.isTimedOut(), is(false));

        logger.info("-> restarting stopped node");
        internalCluster().startNode(Settings.builder().put("node.name", redNodeName).put(redNodeDataPathSettings).build());
        response = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        assertThat(response.isTimedOut(), is(false));
    }

    public void testRestartCmNode() throws Exception {

        Settings cmNodeSettings = internalCluster().dataPathSettings(clusterManager);

        logger.info("-> stopping cluster-manager node");
        internalCluster().stopRandomNode(settings -> settings.get("node.name").equals(clusterManager));
        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes("2").get();
        assertThat(response.isTimedOut(), is(false));

        logger.info("-> restarting stopped node");
        internalCluster().startNode(Settings.builder().put("node.name", clusterManager).put(cmNodeSettings).build());

        // Wait for cluster to stabilize before checking node count
        ensureGreen();

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

    private class ShardMover implements Runnable {
        private boolean moveShards = true;

        public void setMoveShards(boolean moveShards) {
            this.moveShards = moveShards;
        }

        @Override
        public void run() {
            ClusterState clusterState = internalCluster().client().admin().cluster().prepareState().get().getState();
            List<String> dataNodes = clusterState.getNodes()
                .getDataNodes()
                .values()
                .stream()
                .map(DiscoveryNode::getId)
                .collect(Collectors.toList());
            Random random = new Random();
            while (moveShards) {
                for (ShardRouting shard : clusterState.getRoutingTable().allShards()) {
                    if (!moveShards) {
                        break;
                    }
                    if (shard.assignedToNode() && shard.primary()) {
                        String currentNode = shard.currentNodeId();
                        // Filter nodes that have the same color attribute as the current node
                        DiscoveryNode currentDiscoveryNode = clusterState.getNodes().get(currentNode);
                        String currentColor = currentDiscoveryNode.getAttributes().get("color");

                        List<String> compatibleNodes = dataNodes.stream().filter(nodeId -> !nodeId.equals(currentNode)).filter(nodeId -> {
                            DiscoveryNode node = clusterState.getNodes().get(nodeId);
                            String nodeColor = node.getAttributes().get("color");
                            return Objects.equals(currentColor, nodeColor);
                        }).collect(Collectors.toList());

                        if (!compatibleNodes.isEmpty()) {
                            String targetNode = compatibleNodes.get(random.nextInt(compatibleNodes.size()));
                            try {
                                internalCluster().client()
                                    .admin()
                                    .cluster()
                                    .prepareReroute()
                                    .add(new MoveAllocationCommand(shard.getIndexName(), shard.getId(), currentNode, targetNode))
                                    .get();
                            } catch (Exception e) {
                                // Ignore allocation failures and continue
                                logger.debug("Failed to move shard {}, continuing", shard, e);
                            }
                        }
                    }
                }
            }
        }
    }
}
