/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.discovery.PeerFinder;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.opensearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_ACTION_NAME;
import static org.opensearch.cluster.coordination.LeaderChecker.LEADER_CHECK_ACTION_NAME;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

/**
 * Reproduces a node-join/node-left loop that is still possible after
 * https://github.com/opensearch-project/OpenSearch/pull/15521.
 * <p>
 * That fix marks removed nodes as pending-disconnect when the node-left publication starts, so a join that arrives
 * after that point fails to connect. It does not cover a join that was validated <em>before</em> the publication
 * starts, i.e. while the node-left state is still being computed. That join stays queued in the master service.
 * When the node-left state is applied, the cluster-manager closes its connection to the node. The queued join then
 * runs, adds the node back, and starts a follower check, which fails at once with NodeNotConnectedException. The
 * node is removed again with reason "disconnected". The node never receives the join publication, so it stays a
 * candidate and sends another join while the next node-left state is computed, and the loop repeats.
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class NodeJoinLeftQueuedJoinIT extends OpenSearchIntegTestCase {

    /** Upper bound on how long one node-left compute waits for a join from the red node. */
    private static final long MAX_WAIT_FOR_JOIN_MILLIS = 5000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class, SlowRemovalComputePlugin.class);
    }

    @Before
    public void resetPlugin() {
        // Reset before each test, once the previous test's cluster (and any compute blocked in the decider) is gone.
        SlowRemovalComputePlugin.reset();
    }

    public void testQueuedJoinDuringSlowNodeLeftComputeCausesJoinLeftLoop() throws Exception {
        final Settings nodeSettings = Settings.builder()
            .put(FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .put(LeaderChecker.LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .put(PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "200ms")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "100ms")
            .build();
        final String clusterManager = internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build());
        final String redNodeName = internalCluster().startDataOnlyNode(
            Settings.builder().put("node.attr.color", "red").put(nodeSettings).build()
        );
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        assertThat(health.isTimedOut(), is(false));

        // One started shard on the blue node, so every reroute asks the deciders whether it can remain.
        client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .get();
        ensureGreen("test");

        final String redNodeId = internalCluster().clusterService(redNodeName).localNode().getId();
        SlowRemovalComputePlugin.redNodeId = redNodeId;

        // Record every removal and addition of the red node, as seen on the cluster-manager. A removal that happens more
        // than settleMillis after the disruption ended means the node is being removed again on a healthy network.
        final long settleMillis = MAX_WAIT_FOR_JOIN_MILLIS;
        final long observeMillis = 10_000;
        final AtomicLong disruptionEnd = new AtomicLong(-1);
        final CountDownLatch loopDetected = new CountDownLatch(1);
        final List<Long> redRemovalTimesMillis = new CopyOnWriteArrayList<>();
        final List<Long> redJoinTimesMillis = new CopyOnWriteArrayList<>();
        final ClusterService cmClusterService = internalCluster().getInstance(ClusterService.class, clusterManager);
        SlowRemovalComputePlugin.clusterManagerClusterService = cmClusterService;
        cmClusterService.addListener(event -> {
            for (DiscoveryNode n : event.nodesDelta().removedNodes()) {
                if (n.getId().equals(redNodeId)) {
                    final long now = System.currentTimeMillis();
                    redRemovalTimesMillis.add(now);
                    final long end = disruptionEnd.get();
                    if (end >= 0 && now > end + settleMillis) {
                        loopDetected.countDown();
                    }
                }
            }
            for (DiscoveryNode n : event.nodesDelta().addedNodes()) {
                if (n.getId().equals(redNodeId)) {
                    redJoinTimesMillis.add(System.currentTimeMillis());
                }
            }
        });

        final MockTransportService cmTransport = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            clusterManager
        );
        // Short disruption between the cluster-manager and the red node only: the red node fails the cluster-manager's
        // follower checks, and the cluster-manager rejects the red node's leader checks. The red node becomes a
        // candidate (and starts sending joins) while it is still in the cluster-manager's state, as a node on a lossy
        // link does.
        final AtomicBoolean disrupt = new AtomicBoolean();
        final MockTransportService redTransport = (MockTransportService) internalCluster().getInstance(TransportService.class, redNodeName);
        redTransport.addRequestHandlingBehavior(FOLLOWER_CHECK_ACTION_NAME, (handler, request, channel, task) -> {
            if (disrupt.get()) {
                throw new NodeHealthCheckFailureException("simulated follower check failure");
            }
            handler.messageReceived(request, channel, task);
        });
        cmTransport.<LeaderChecker.LeaderCheckRequest>addRequestHandlingBehavior(
            LEADER_CHECK_ACTION_NAME,
            (handler, request, channel, task) -> {
                if (disrupt.get() && request.getSender().getId().equals(redNodeId)) {
                    throw new NodeHealthCheckFailureException("simulated leader check failure");
                }
                handler.messageReceived(request, channel, task);
            }
        );

        SlowRemovalComputePlugin.slow.set(true);
        logger.info("--> starting disruption");
        disrupt.set(true);

        // End the disruption once a join from the red node has reached the cluster-manager while the first node-left
        // state is still being computed. From here on the network is healthy and nothing fails any check on purpose.
        final boolean joinDuringFirstCompute;
        try {
            joinDuringFirstCompute = SlowRemovalComputePlugin.firstJoinDuringCompute.await(30, TimeUnit.SECONDS);
        } finally {
            disrupt.set(false);
        }
        final long disruptionEndMillis = System.currentTimeMillis();
        disruptionEnd.set(disruptionEndMillis);
        assertTrue("red node never sent a join while the node-left state was being computed", joinDuringFirstCompute);
        logger.info("--> disruption ended; a join from the red node is queued behind the node-left task");

        // Let the first removal finish, then watch for further removals on a healthy network. Stop early as soon as
        // one is seen; otherwise the whole observation window passes without a removal.
        loopDetected.await(settleMillis + observeMillis, TimeUnit.MILLISECONDS);
        SlowRemovalComputePlugin.slow.set(false);

        final List<Long> removalsAfterSettle = redRemovalTimesMillis.stream()
            .filter(t -> t > disruptionEndMillis + settleMillis)
            .map(t -> t - disruptionEndMillis)
            .collect(Collectors.toList());
        logger.info(
            "--> red removals [{}] joins [{}] computes [{}] joins-during-compute [{}]; removals at ms after disruption end"
                + " (post-settle): {}",
            redRemovalTimesMillis.size(),
            redJoinTimesMillis.size(),
            SlowRemovalComputePlugin.computes.get(),
            SlowRemovalComputePlugin.joinsDuringCompute.get(),
            removalsAfterSettle
        );

        // The race needs at least one join queued behind a node-left task; make sure the test set that up.
        assertThat(SlowRemovalComputePlugin.joinsDuringCompute.get(), greaterThanOrEqualTo(1));

        // A healthy network must not keep removing the node. With the race, it is removed once per node-left compute,
        // with reason "disconnected", for as long as computing the node-left state outlasts the node's join interval.
        assertThat(
            "red node kept being removed after the disruption ended (join/leave loop); removals ["
                + redRemovalTimesMillis.size()
                + "] joins ["
                + redJoinTimesMillis.size()
                + "]",
            removalsAfterSettle,
            empty()
        );
        ensureStableCluster(3);
    }

    /**
     * Makes the cluster-manager slow to compute any cluster state in which the red node is absent, as in production
     * where the node-left reroute took ~1.1s, longer than the node's 1s join interval. Each such compute lasts until
     * a join from the red node has been validated and is queued in the cluster-manager service, so the condition the
     * race needs holds on every cycle instead of by chance. Unlike the upstream tests, which slow the node-left
     * <em>apply</em> step, this delays the step <em>before</em> publication starts, which #15521 does not cover.
     */
    public static class SlowRemovalComputePlugin extends Plugin implements ClusterPlugin {
        static final AtomicBoolean slow = new AtomicBoolean();
        static final AtomicInteger computes = new AtomicInteger();
        static final AtomicInteger joinsDuringCompute = new AtomicInteger();
        static volatile String redNodeId;
        static volatile ClusterService clusterManagerClusterService;
        static volatile CountDownLatch firstJoinDuringCompute = new CountDownLatch(1);

        static void reset() {
            slow.set(false);
            computes.set(0);
            joinsDuringCompute.set(0);
            redNodeId = null;
            clusterManagerClusterService = null;
            firstJoinDuringCompute = new CountDownLatch(1);
        }

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return List.of(new AllocationDecider() {
                @Override
                public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    final String red = redNodeId;
                    if (slow.get() && red != null && allocation.nodes().nodeExists(red) == false && isClusterManagerUpdateThread()) {
                        computes.incrementAndGet();
                        waitForJoinToBeQueued();
                    }
                    return Decision.ALWAYS;
                }
            });
        }

        private static void waitForJoinToBeQueued() {
            final ClusterService clusterService = clusterManagerClusterService;
            if (clusterService == null) {
                return;
            }
            try {
                assertBusy(() -> {
                    if (isJoinQueued(clusterService) == false) {
                        throw new AssertionError("no join is queued behind the node-left task");
                    }
                }, MAX_WAIT_FOR_JOIN_MILLIS, TimeUnit.MILLISECONDS);
                joinsDuringCompute.incrementAndGet();
                firstJoinDuringCompute.countDown();
            } catch (AssertionError e) {
                // No join reached the cluster-manager within the bound; the compute continues without one.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        private static boolean isJoinQueued(ClusterService clusterService) {
            return clusterService.getClusterManagerService()
                .pendingTasks()
                .stream()
                .anyMatch(task -> task.isExecuting() == false && task.getSource().string().startsWith("node-join"));
        }

        private static boolean isClusterManagerUpdateThread() {
            // Only delay state computation on the cluster-manager, not reroute simulations elsewhere.
            final String threadName = Thread.currentThread().getName();
            return threadName.contains("clusterManagerService#updateTask") || threadName.contains("masterService#updateTask");
        }
    }
}
