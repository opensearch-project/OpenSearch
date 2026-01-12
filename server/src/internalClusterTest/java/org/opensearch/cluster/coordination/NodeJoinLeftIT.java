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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MockEngineFactoryPlugin;
import org.opensearch.index.store.IndexStoreListener;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.IndexStorePlugin;
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_ACTION_NAME;
import static org.hamcrest.Matchers.is;

/**
 * Check https://github.com/opensearch-project/OpenSearch/issues/4874 and
 * https://github.com/opensearch-project/OpenSearch/pull/15521 for context
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
@SuppressForbidden(reason = "Pending fix: https://github.com/opensearch-project/OpenSearch/issues/18972")
public class NodeJoinLeftIT extends OpenSearchIntegTestCase {

    private TestLogsAppender testLogsAppender;
    private String clusterManager;
    private String redNodeName;
    private Settings nodeSettings;
    private LoggerContext loggerContext;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = Arrays.asList(
            MockTransportService.TestPlugin.class,
            MockFSIndexStore.TestPlugin.class,
            InternalSettingsPlugin.class,
            MockEngineFactoryPlugin.class
        );

        if (requiresTestIndexStoreListener()) {
            plugins = new ArrayList<>(plugins);
            plugins.add(TestIndexStoreListenerPlugin.class);
        }
        return plugins;
    }

    private boolean requiresTestIndexStoreListener() {
        try {
            Method testMethod = getClass().getMethod(getTestName());
            return testMethod.isAnnotationPresent(RequiresTestIndexStoreListener.class);
        } catch (NoSuchMethodException e) {
            return false;
        }
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
        List<String> messagesToCapture = new ArrayList<String>() {
            {
                add("failed to join");
                add("IllegalStateException");
            }
        };
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
        this.clusterManager = internalCluster().startClusterManagerOnlyNode(nodeSettings);
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

    @RequiresTestIndexStoreListener
    public void testClusterStabilityWhenClusterStatePublicationLagsOnShardCleanup() throws Exception {
        additionalSetupForLagDuringDataMigration();
        Map<ClusterApplierService, ClusterStateListener> blueNodeListeners = new HashMap<>();
        try {
            TestIndexStoreListener.DELAY_SHARD_ASSIGNMENT = true;

            // Also add delay listeners on blue nodes (source nodes) to ensure publication lag
            for (String nodeName : internalCluster().getNodeNames()) {
                Settings nodeSettings = internalCluster().getInstance(Settings.class, nodeName);
                if ("blue".equals(nodeSettings.get("node.attr.color")) && nodeSettings.getAsBoolean("node.data", true)) {
                    ClusterApplierService applierService = internalCluster().getInstance(ClusterService.class, nodeName)
                        .getClusterApplierService();
                    ClusterStateListener listener = createDelayListener(applierService);
                    blueNodeListeners.put(applierService, listener);
                    applierService.addListener(listener);
                }
            }

            logger.info("Moving all shards to red nodes");
            client().admin()
                .indices()
                .prepareUpdateSettings("*")
                .setSettings(Settings.builder().put("index.routing.allocation.include.color", "red"))
                .get();
            validateNodeDropDueToPublicationLag();
        } finally {
            TestIndexStoreListener.DELAY_SHARD_ASSIGNMENT = false;
            blueNodeListeners.forEach(ClusterApplierService::removeListener);
        }
        validateClusterRecovery();
    }

    public void testClusterStabilityWhenClusterStatePublicationLagsWithLongRunningListenerOnApplierThread() throws Exception {
        additionalSetupForLagDuringDataMigration();
        Map<ClusterApplierService, ClusterStateListener> redNodeListeners = new HashMap<>();
        try {
            // Setup listeners only on red data nodes
            for (String nodeName : internalCluster().getNodeNames()) {
                Settings nodeSettings = internalCluster().getInstance(Settings.class, nodeName);
                if ("red".equals(nodeSettings.get("node.attr.color")) && nodeSettings.getAsBoolean("node.data", true)) {
                    ClusterApplierService applierService = internalCluster().getInstance(ClusterService.class, nodeName)
                        .getClusterApplierService();
                    ClusterStateListener listener = createDelayListener(applierService);
                    redNodeListeners.put(applierService, listener);
                    applierService.addListener(listener);
                }
            }
            logger.info("Moving all shards to red nodes");
            client().admin()
                .indices()
                .prepareUpdateSettings("*")
                .setSettings(Settings.builder().put("index.routing.allocation.include.color", "red"))
                .get();
            validateNodeDropDueToPublicationLag();
        } finally {
            // Cleanup listeners
            redNodeListeners.forEach(ClusterApplierService::removeListener);
        }
        validateClusterRecovery();
    }

    private void additionalSetupForLagDuringDataMigration() {
        internalCluster().startClusterManagerOnlyNodes(2, nodeSettings);
        internalCluster().startDataOnlyNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());
        internalCluster().startDataOnlyNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());
        internalCluster().startDataOnlyNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        internalCluster().startDataOnlyNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        internalCluster().client().admin().cluster().prepareHealth().setWaitForNodes("9").get();
        internalCluster().client()
            .admin()
            .indices()
            .prepareCreate("index-1")
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
            .get();
        internalCluster().client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
        Settings settings = Settings.builder()
            .put("cluster.follower_lag.timeout", "5s")
            .put("cluster.publish.timeout", "15s")
            .put("cluster.routing.allocation.cluster_concurrent_recoveries", 4)
            .build();
        settingsRequest.transientSettings(settings);
        internalCluster().client().admin().cluster().updateSettings(settingsRequest).actionGet();
        // Introducing a delay of 3sec on cluster manager applier thread to ensure join request from peer finder is received during
        // node-left
        ClusterService clusterManagerClsService = internalCluster().getInstance(ClusterService.class, clusterManager);
        clusterManagerClsService.addStateApplier(event -> {
            if (event.nodesRemoved()) {
                logger.info("Adding a 3 sec delay on cluster manager applier thread");
                CountDownLatch latch = new CountDownLatch(1);
                ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
                executor.schedule(() -> { latch.countDown(); }, 3, TimeUnit.SECONDS);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    logger.info("Interrupted while waiting for cluster manager applier delay");
                    Thread.currentThread().interrupt();
                }
                executor.shutdown();
            }
        });
        testLogsAppender.addMessagesToCapture(Set.of("Sleeping for 30 seconds", "NodeRemovalClusterStateTaskExecutor", "reason: lagging"));
        testLogsAppender.clearCapturedLogs();
    }

    private void validateNodeDropDueToPublicationLag() throws Exception {
        // Wait for the delay log to appear first, confirming the delay mechanism is active
        boolean delayLogFound = testLogsAppender.waitForLog("Sleeping for 30 seconds", 60, TimeUnit.SECONDS);
        assertTrue("Expected log for delay in shard cleanup was not found within the timeout period", delayLogFound);

        // Use assertBusy to wait for node drop with retries
        assertBusy(() -> {
            ClusterHealthResponse clusterHealthResponse = internalCluster().client()
                .admin()
                .cluster()
                .prepareHealth()
                .setWaitForNodes("<9")
                .setTimeout(TimeValue.timeValueSeconds(5))
                .get();
            assertFalse("Cluster didn't have a node drop yet", clusterHealthResponse.isTimedOut());
        }, 120, TimeUnit.SECONDS);

        logger.info("Node drop detected, validating logs");
        boolean logFound = testLogsAppender.waitForLog(
            "Tasks batched with key: org.opensearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor",
            30,
            TimeUnit.SECONDS
        ) && testLogsAppender.waitForLog("reason: lagging", 30, TimeUnit.SECONDS);
        assertTrue("Expected log for node removal due to publication lag was not found within the timeout period", logFound);
        // assert that join requests fail with the right exception
        logFound = testLogsAppender.waitForLog("failed to join", 30, TimeUnit.SECONDS)
            && testLogsAppender.waitForLog(
                "IllegalStateException[cannot make a new connection as disconnect to node",
                30,
                TimeUnit.SECONDS
            );
        assertTrue("Expected log for join request failure was not found within the timeout period", logFound);
    }

    private void validateClusterRecovery() {
        logger.info("Checking if cluster is stable after long running thread");

        ClusterHealthResponse response = internalCluster().client()
            .admin()
            .cluster()
            .prepareHealth()
            .setWaitForGreenStatus()
            .setWaitForNodes("9")
            .setTimeout(TimeValue.timeValueSeconds(60))
            .get();
        logger.info("Cluster health response after removing delay: {}", response);
        assertFalse("Cluster health response: " + response.toString(), response.isTimedOut());
        assertEquals("Not all shards are active after moving shards from blue to red nodes", 3, response.getActiveShards());
        // Assert that all shards are migrated to new node-type (red nodes)
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertTrue(
            "All shards should be migrated to red nodes",
            clusterState.getRoutingTable()
                .allShards()
                .stream()
                .allMatch(shard -> clusterState.nodes().get(shard.currentNodeId()).getAttributes().get("color").equals("red"))
        );
    }

    private ClusterStateListener createDelayListener(ClusterApplierService applierService) {
        return event -> applierService.runOnApplierThread("NodeJoinLeftIT", clusterState -> {
            logger.info("Sleeping for 30 seconds");
            CountDownLatch latch = new CountDownLatch(1);
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            executor.schedule(() -> { latch.countDown(); }, 30, TimeUnit.SECONDS);
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.info("Interrupted while waiting for cluster state applier");
                Thread.currentThread().interrupt();
            }
            executor.shutdown();
        }, (source, e) -> logger.error(() -> new ParameterizedMessage("{} unexpected error in listener wait", source), e));
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

    public static class TestIndexStoreListenerPlugin extends Plugin implements IndexStorePlugin {
        @Override
        public Optional<IndexStoreListener> getIndexStoreListener() {
            return Optional.of(new TestIndexStoreListener());
        }
    }

    public static class TestIndexStoreListener implements IndexStoreListener {

        private static final Logger logger = LogManager.getLogger(TestIndexStoreListener.class);
        private static volatile boolean DELAY_SHARD_ASSIGNMENT = false;
        private static final int SHARD_DELETE_DELAY_SECONDS = 30;

        public TestIndexStoreListener() {}

        @Override
        public void beforeShardPathDeleted(ShardId shardId, IndexSettings indexSettings, NodeEnvironment env) {
            if (DELAY_SHARD_ASSIGNMENT) {
                logger.info(
                    "{}: Sleeping for {} seconds before deleting data for shard: {}",
                    Thread.currentThread().getName(),
                    SHARD_DELETE_DELAY_SECONDS,
                    shardId
                );
                // Add slow operation to simulate delay
                CountDownLatch latch = new CountDownLatch(1);
                ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
                executor.schedule(() -> {
                    logger.info(
                        "{}: Done sleeping for {} sec before deleting data for shard: {}",
                        Thread.currentThread().getName(),
                        SHARD_DELETE_DELAY_SECONDS,
                        shardId
                    );
                    latch.countDown();
                }, SHARD_DELETE_DELAY_SECONDS, TimeUnit.SECONDS);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    logger.info("Interrupted while waiting for shard deletion delay");
                    Thread.currentThread().interrupt();
                }
                executor.shutdown();
            }
        }

        @Override
        public void beforeIndexPathDeleted(Index index, IndexSettings indexSettings, NodeEnvironment env) {}
    }
}
