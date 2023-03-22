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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.opensearch.cluster.decommission.DecommissionStatus;
import org.opensearch.cluster.decommission.NodeDecommissionedException;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.FakeThreadPoolClusterManagerService;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.cluster.service.MasterServiceTests;
import org.opensearch.common.Randomness;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.BaseFuture;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.monitor.NodeHealthService;
import org.opensearch.monitor.StatusInfo;
import org.opensearch.node.Node;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RequestHandlerRegistry;
import org.opensearch.transport.TestTransportChannel;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.opensearch.monitor.StatusInfo.Status.HEALTHY;
import static org.opensearch.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class NodeJoinTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;

    private ClusterManagerService clusterManagerService;
    private Coordinator coordinator;
    private DeterministicTaskQueue deterministicTaskQueue;
    private Transport transport;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool(NodeJoinTests.getTestClass().getName());
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterManagerService.close();
    }

    private static ClusterState initialState(DiscoveryNode localNode, long term, long version, VotingConfiguration config) {
        return ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .term(term)
                            .lastAcceptedConfiguration(config)
                            .lastCommittedConfiguration(config)
                            .build()
                    )
            )
            .version(version)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
    }

    private void setupFakeClusterManagerServiceAndCoordinator(long term, ClusterState initialState, NodeHealthService nodeHealthService) {
        deterministicTaskQueue = new DeterministicTaskQueue(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(),
            random()
        );
        final ThreadPool fakeThreadPool = deterministicTaskQueue.getThreadPool();
        FakeThreadPoolClusterManagerService fakeClusterManagerService = new FakeThreadPoolClusterManagerService(
            "test_node",
            "test",
            fakeThreadPool,
            deterministicTaskQueue::scheduleNow
        );
        setupClusterManagerServiceAndCoordinator(
            term,
            initialState,
            fakeClusterManagerService,
            fakeThreadPool,
            Randomness.get(),
            nodeHealthService
        );
        fakeClusterManagerService.setClusterStatePublisher((event, publishListener, ackListener) -> {
            coordinator.handlePublishRequest(new PublishRequest(event.state()));
            publishListener.onResponse(null);
        });
        fakeClusterManagerService.start();
    }

    private void setupRealClusterManagerServiceAndCoordinator(long term, ClusterState initialState) {
        ClusterManagerService clusterManagerService = new ClusterManagerService(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test_node").build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initialState);
        clusterManagerService.setClusterStatePublisher((event, publishListener, ackListener) -> {
            clusterStateRef.set(event.state());
            publishListener.onResponse(null);
        });
        setupClusterManagerServiceAndCoordinator(
            term,
            initialState,
            clusterManagerService,
            threadPool,
            new Random(Randomness.get().nextLong()),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        clusterManagerService.setClusterStateSupplier(clusterStateRef::get);
        clusterManagerService.start();
    }

    private void setupClusterManagerServiceAndCoordinator(
        long term,
        ClusterState initialState,
        ClusterManagerService clusterManagerService,
        ThreadPool threadPool,
        Random random,
        NodeHealthService nodeHealthService
    ) {
        if (this.clusterManagerService != null || coordinator != null) {
            throw new IllegalStateException("method setupClusterManagerServiceAndCoordinator can only be called once");
        }
        this.clusterManagerService = clusterManagerService;
        CapturingTransport capturingTransport = new CapturingTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode destination) {
                if (action.equals(HANDSHAKE_ACTION_NAME)) {
                    handleResponse(
                        requestId,
                        new TransportService.HandshakeResponse(destination, initialState.getClusterName(), destination.getVersion())
                    );
                } else if (action.equals(JoinHelper.VALIDATE_JOIN_ACTION_NAME)) {
                    handleResponse(requestId, new TransportResponse.Empty());
                } else {
                    super.onSendRequest(requestId, action, request, destination);
                }
            }

            @Override
            protected void onSendRequest(
                long requestId,
                String action,
                TransportRequest request,
                DiscoveryNode destination,
                TransportRequestOptions options
            ) {
                onSendRequest(requestId, action, request, destination);
            }
        };
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TransportService transportService = capturingTransport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> initialState.nodes().getLocalNode(),
            clusterSettings,
            Collections.emptySet()
        );
        coordinator = new Coordinator(
            "test_node",
            Settings.EMPTY,
            clusterSettings,
            transportService,
            writableRegistry(),
            OpenSearchAllocationTestCase.createAllocationService(Settings.EMPTY),
            clusterManagerService,
            () -> new InMemoryPersistedState(term, initialState),
            r -> emptyList(),
            new NoOpClusterApplier(),
            Collections.emptyList(),
            random,
            (s, p, r) -> {},
            ElectionStrategy.DEFAULT_INSTANCE,
            nodeHealthService
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        transport = capturingTransport;
        coordinator.start();
        coordinator.startInitialJoin();
    }

    protected DiscoveryNode newNode(int i) {
        return newNode(i, randomBoolean());
    }

    protected DiscoveryNode newNode(int i, boolean clusterManager) {
        final Set<DiscoveryNodeRole> roles;
        if (clusterManager) {
            roles = singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE);
        } else {
            roles = Collections.emptySet();
        }
        final String prefix = clusterManager ? "cluster_manager_" : "data_";
        return new DiscoveryNode(prefix + i, i + "", buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }

    static class SimpleFuture extends BaseFuture<Void> {
        final String description;

        SimpleFuture(String description) {
            this.description = description;
        }

        public void markAsDone() {
            set(null);
        }

        public void markAsFailed(Throwable t) {
            setException(t);
        }

        @Override
        public String toString() {
            return "future [" + description + "]";
        }
    }

    private SimpleFuture joinNodeAsync(final JoinRequest joinRequest) {
        final SimpleFuture future = new SimpleFuture("join of " + joinRequest + "]");
        logger.debug("starting {}", future);
        // clone the node before submitting to simulate an incoming join, which is guaranteed to have a new
        // disco node object serialized off the network
        try {
            final RequestHandlerRegistry<JoinRequest> joinHandler = transport.getRequestHandlers().getHandler(JoinHelper.JOIN_ACTION_NAME);
            final ActionListener<TransportResponse> listener = new ActionListener<TransportResponse>() {

                @Override
                public void onResponse(TransportResponse transportResponse) {
                    logger.debug("{} completed", future);
                    future.markAsDone();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(() -> new ParameterizedMessage("unexpected error for {}", future), e);
                    future.markAsFailed(e);
                }
            };

            joinHandler.processMessageReceived(joinRequest, new TestTransportChannel(listener));
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected error for {}", future), e);
            future.markAsFailed(e);
        }
        return future;
    }

    private void joinNode(final JoinRequest joinRequest) {
        FutureUtils.get(joinNodeAsync(joinRequest));
    }

    private void joinNodeAndRun(final JoinRequest joinRequest) {
        SimpleFuture fut = joinNodeAsync(joinRequest);
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(fut.isDone());
        FutureUtils.get(fut);
    }

    public void testJoinWithHigherTermElectsLeader() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeClusterManagerServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(randomFrom(node0, node1))),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        assertFalse(isLocalNodeElectedMaster());
        assertNull(coordinator.getStateForClusterManagerService().nodes().getClusterManagerNodeId());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        SimpleFuture fut = joinNodeAsync(
            new JoinRequest(node1, newTerm, Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion)))
        );
        assertEquals(Coordinator.Mode.LEADER, coordinator.getMode());
        assertNull(coordinator.getStateForClusterManagerService().nodes().getClusterManagerNodeId());
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(fut.isDone());
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(coordinator.getStateForClusterManagerService().nodes().isLocalNodeElectedClusterManager());
    }

    public void testJoinWithHigherTermButBetterStateGetsRejected() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeClusterManagerServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node1)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long higherVersion = initialVersion + randomLongBetween(1, 10);
        expectThrows(
            CoordinationStateRejectedException.class,
            () -> joinNodeAndRun(new JoinRequest(node1, newTerm, Optional.of(new Join(node1, node0, newTerm, initialTerm, higherVersion))))
        );
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testJoinWithHigherTermButBetterStateStillElectsClusterManagerThroughSelfJoin() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeClusterManagerServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node0)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long higherVersion = initialVersion + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node1, newTerm, Optional.of(new Join(node1, node0, newTerm, initialTerm, higherVersion))));
        assertTrue(isLocalNodeElectedMaster());
    }

    public void testJoinElectedLeader() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeClusterManagerServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node0)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node0, newTerm, Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
        assertFalse(clusterStateHasNode(node1));
        joinNodeAndRun(new JoinRequest(node1, newTerm, Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(clusterStateHasNode(node1));
    }

    public void testJoinElectedLeaderWithHigherTerm() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeClusterManagerServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node0)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        long newTerm = initialTerm + randomLongBetween(1, 10);

        joinNodeAndRun(new JoinRequest(node0, newTerm, Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());

        long newerTerm = newTerm + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node1, newerTerm, Optional.empty()));
        assertThat(coordinator.getCurrentTerm(), greaterThanOrEqualTo(newerTerm));
        assertTrue(isLocalNodeElectedMaster());
    }

    public void testJoinAccumulation() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        DiscoveryNode node2 = newNode(2, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeClusterManagerServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node2)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        SimpleFuture futNode0 = joinNodeAsync(
            new JoinRequest(node0, newTerm, Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion)))
        );
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(futNode0.isDone());
        assertFalse(isLocalNodeElectedMaster());
        SimpleFuture futNode1 = joinNodeAsync(
            new JoinRequest(node1, newTerm, Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion)))
        );
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(futNode1.isDone());
        assertFalse(isLocalNodeElectedMaster());
        joinNodeAndRun(new JoinRequest(node2, newTerm, Optional.of(new Join(node2, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(clusterStateHasNode(node1));
        assertTrue(clusterStateHasNode(node2));
        FutureUtils.get(futNode0);
        FutureUtils.get(futNode1);
    }

    public void testJoinFollowerWithHigherTerm() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeClusterManagerServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node0)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        long newTerm = initialTerm + randomLongBetween(1, 10);
        handleStartJoinFrom(node1, newTerm);
        handleFollowerCheckFrom(node1, newTerm);
        long newerTerm = newTerm + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node1, newerTerm, Optional.of(new Join(node1, node0, newerTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
    }

    public void testJoinUpdateVotingConfigExclusion() throws Exception {
        DiscoveryNode initialNode = newNode(0, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);

        CoordinationMetadata.VotingConfigExclusion votingConfigExclusion = new CoordinationMetadata.VotingConfigExclusion(
            CoordinationMetadata.VotingConfigExclusion.MISSING_VALUE_MARKER,
            "knownNodeName"
        );

        setupFakeClusterManagerServiceAndCoordinator(
            initialTerm,
            buildStateWithVotingConfigExclusion(initialNode, initialTerm, initialVersion, votingConfigExclusion),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );

        DiscoveryNode knownJoiningNode = new DiscoveryNode(
            "knownNodeName",
            "newNodeId",
            buildNewFakeTransportAddress(),
            emptyMap(),
            singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long newerTerm = newTerm + randomLongBetween(1, 10);

        joinNodeAndRun(
            new JoinRequest(
                knownJoiningNode,
                initialTerm,
                Optional.of(new Join(knownJoiningNode, initialNode, newerTerm, initialTerm, initialVersion))
            )
        );

        assertTrue(MasterServiceTests.discoveryState(clusterManagerService).getVotingConfigExclusions().stream().anyMatch(exclusion -> {
            return "knownNodeName".equals(exclusion.getNodeName()) && "newNodeId".equals(exclusion.getNodeId());
        }));
    }

    private ClusterState buildStateWithVotingConfigExclusion(
        DiscoveryNode initialNode,
        long initialTerm,
        long initialVersion,
        CoordinationMetadata.VotingConfigExclusion votingConfigExclusion
    ) {
        ClusterState initialState = initialState(
            initialNode,
            initialTerm,
            initialVersion,
            new VotingConfiguration(singleton(initialNode.getId()))
        );
        Metadata newMetadata = Metadata.builder(initialState.metadata())
            .coordinationMetadata(
                CoordinationMetadata.builder(initialState.coordinationMetadata()).addVotingConfigExclusion(votingConfigExclusion).build()
            )
            .build();

        return ClusterState.builder(initialState).metadata(newMetadata).build();
    }

    private void handleStartJoinFrom(DiscoveryNode node, long term) throws Exception {
        final RequestHandlerRegistry<StartJoinRequest> startJoinHandler = transport.getRequestHandlers()
            .getHandler(JoinHelper.START_JOIN_ACTION_NAME);
        startJoinHandler.processMessageReceived(
            new StartJoinRequest(node, term),
            new TestTransportChannel(new ActionListener<TransportResponse>() {
                @Override
                public void onResponse(TransportResponse transportResponse) {}

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            })
        );
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(isLocalNodeElectedMaster());
        assertThat(coordinator.getMode(), equalTo(Coordinator.Mode.CANDIDATE));
    }

    private void handleFollowerCheckFrom(DiscoveryNode node, long term) throws Exception {
        final RequestHandlerRegistry<FollowersChecker.FollowerCheckRequest> followerCheckHandler = transport.getRequestHandlers()
            .getHandler(FollowersChecker.FOLLOWER_CHECK_ACTION_NAME);
        final TestTransportChannel channel = new TestTransportChannel(new ActionListener<TransportResponse>() {
            @Override
            public void onResponse(TransportResponse transportResponse) {

            }

            @Override
            public void onFailure(Exception e) {
                fail();
            }
        });
        followerCheckHandler.processMessageReceived(new FollowersChecker.FollowerCheckRequest(term, node), channel);
        // Will throw exception if failed
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(isLocalNodeElectedMaster());
        assertThat(coordinator.getMode(), equalTo(Coordinator.Mode.FOLLOWER));
    }

    public void testJoinFollowerFails() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeClusterManagerServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node0)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        long newTerm = initialTerm + randomLongBetween(1, 10);
        handleStartJoinFrom(node1, newTerm);
        handleFollowerCheckFrom(node1, newTerm);
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> joinNodeAndRun(new JoinRequest(node1, newTerm, Optional.empty())))
                .getMessage(),
            containsString("join target is a follower")
        );
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testBecomeFollowerFailsPendingJoin() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeClusterManagerServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node1)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        long newTerm = initialTerm + randomLongBetween(1, 10);
        SimpleFuture fut = joinNodeAsync(
            new JoinRequest(node0, newTerm, Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion)))
        );
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(fut.isDone());
        assertFalse(isLocalNodeElectedMaster());
        handleFollowerCheckFrom(node1, newTerm);
        assertFalse(isLocalNodeElectedMaster());
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> FutureUtils.get(fut)).getMessage(),
            containsString("became follower")
        );
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testConcurrentJoining() {
        List<DiscoveryNode> clusterManagerNodes = IntStream.rangeClosed(1, randomIntBetween(2, 5))
            .mapToObj(nodeId -> newNode(nodeId, true))
            .collect(Collectors.toList());
        List<DiscoveryNode> otherNodes = IntStream.rangeClosed(
            clusterManagerNodes.size() + 1,
            clusterManagerNodes.size() + 1 + randomIntBetween(0, 5)
        ).mapToObj(nodeId -> newNode(nodeId, false)).collect(Collectors.toList());
        List<DiscoveryNode> allNodes = Stream.concat(clusterManagerNodes.stream(), otherNodes.stream()).collect(Collectors.toList());

        DiscoveryNode localNode = clusterManagerNodes.get(0);
        VotingConfiguration votingConfiguration = new VotingConfiguration(
            randomValueOtherThan(
                singletonList(localNode),
                () -> randomSubsetOf(randomIntBetween(1, clusterManagerNodes.size()), clusterManagerNodes)
            ).stream().map(DiscoveryNode::getId).collect(Collectors.toSet())
        );

        logger.info("Voting configuration: {}", votingConfiguration);

        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupRealClusterManagerServiceAndCoordinator(
            initialTerm,
            initialState(localNode, initialTerm, initialVersion, votingConfiguration)
        );
        long newTerm = initialTerm + randomLongBetween(1, 10);

        // we need at least a quorum of voting nodes with a correct term and worse state
        List<DiscoveryNode> successfulNodes;
        do {
            successfulNodes = randomSubsetOf(allNodes);
        } while (votingConfiguration.hasQuorum(successfulNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toList())) == false);

        logger.info("Successful voting nodes: {}", successfulNodes);

        List<JoinRequest> correctJoinRequests = successfulNodes.stream()
            .map(node -> new JoinRequest(node, newTerm, Optional.of(new Join(node, localNode, newTerm, initialTerm, initialVersion))))
            .collect(Collectors.toList());

        List<DiscoveryNode> possiblyUnsuccessfulNodes = new ArrayList<>(allNodes);
        possiblyUnsuccessfulNodes.removeAll(successfulNodes);

        logger.info("Possibly unsuccessful voting nodes: {}", possiblyUnsuccessfulNodes);

        List<JoinRequest> possiblyFailingJoinRequests = possiblyUnsuccessfulNodes.stream().map(node -> {
            if (randomBoolean()) {
                // a correct request
                return new JoinRequest(node, newTerm, Optional.of(new Join(node, localNode, newTerm, initialTerm, initialVersion)));
            } else if (randomBoolean()) {
                // term too low
                return new JoinRequest(
                    node,
                    newTerm,
                    Optional.of(new Join(node, localNode, randomLongBetween(0, initialTerm), initialTerm, initialVersion))
                );
            } else {
                // better state
                return new JoinRequest(
                    node,
                    newTerm,
                    Optional.of(new Join(node, localNode, newTerm, initialTerm, initialVersion + randomLongBetween(1, 10)))
                );
            }
        }).collect(Collectors.toList());

        // duplicate some requests, which will be unsuccessful
        possiblyFailingJoinRequests.addAll(randomSubsetOf(possiblyFailingJoinRequests));

        CyclicBarrier barrier = new CyclicBarrier(correctJoinRequests.size() + possiblyFailingJoinRequests.size() + 1);
        final Runnable awaitBarrier = () -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        };

        final AtomicBoolean stopAsserting = new AtomicBoolean();
        final Thread assertionThread = new Thread(() -> {
            awaitBarrier.run();
            while (stopAsserting.get() == false) {
                coordinator.invariant();
            }
        }, "assert invariants");

        final List<Thread> joinThreads = Stream.concat(correctJoinRequests.stream().map(joinRequest -> new Thread(() -> {
            awaitBarrier.run();
            joinNode(joinRequest);
        }, "process " + joinRequest)), possiblyFailingJoinRequests.stream().map(joinRequest -> new Thread(() -> {
            awaitBarrier.run();
            try {
                joinNode(joinRequest);
            } catch (CoordinationStateRejectedException e) {
                // ignore - these requests are expected to fail
            }
        }, "process " + joinRequest))).collect(Collectors.toList());

        assertionThread.start();
        joinThreads.forEach(Thread::start);
        joinThreads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        stopAsserting.set(true);
        try {
            assertionThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertTrue(MasterServiceTests.discoveryState(clusterManagerService).nodes().isLocalNodeElectedMaster());
        for (DiscoveryNode successfulNode : successfulNodes) {
            assertTrue(successfulNode + " joined cluster", clusterStateHasNode(successfulNode));
            assertFalse(successfulNode + " voted for cluster-manager", coordinator.missingJoinVoteFrom(successfulNode));
        }
    }

    // Validate the deprecated MASTER_ROLE can join another node and elected as leader.
    public void testJoinElectedLeaderWithDeprecatedMasterRole() {
        final Set<DiscoveryNodeRole> roles = singleton(DiscoveryNodeRole.MASTER_ROLE);
        DiscoveryNode node0 = new DiscoveryNode("master0", "0", buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
        DiscoveryNode node1 = new DiscoveryNode("master1", "1", buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
        long initialTerm = 1;
        long initialVersion = 1;
        setupFakeClusterManagerServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node0)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = 2;
        joinNodeAndRun(new JoinRequest(node0, newTerm, Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
        assertFalse(clusterStateHasNode(node1));
        joinNodeAndRun(new JoinRequest(node1, newTerm, Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(clusterStateHasNode(node1));
    }

    public void testJoinFailsWhenDecommissioned() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeClusterManagerServiceAndCoordinator(
            initialTerm,
            initialStateWithDecommissionedAttribute(
                initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node0)),
                new DecommissionAttribute("zone", "zone1")
            ),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node0, newTerm, Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
        assertFalse(clusterStateHasNode(node1));
        joinNodeAndRun(new JoinRequest(node1, newTerm, Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(clusterStateHasNode(node1));
        DiscoveryNode decommissionedNode = new DiscoveryNode(
            "data_2",
            2 + "",
            buildNewFakeTransportAddress(),
            Collections.singletonMap("zone", "zone1"),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        long anotherTerm = newTerm + randomLongBetween(1, 10);

        assertThat(
            expectThrows(
                NodeDecommissionedException.class,
                () -> joinNodeAndRun(new JoinRequest(decommissionedNode, anotherTerm, Optional.empty()))
            ).getMessage(),
            containsString("with current status of decommissioning")
        );
        assertFalse(clusterStateHasNode(decommissionedNode));

        DiscoveryNode node3 = new DiscoveryNode(
            "data_3",
            3 + "",
            buildNewFakeTransportAddress(),
            Collections.singletonMap("zone", "zone2"),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        long termForNode3 = anotherTerm + randomLongBetween(1, 10);

        joinNodeAndRun(new JoinRequest(node3, termForNode3, Optional.empty()));
        assertTrue(clusterStateHasNode(node3));
    }

    private boolean isLocalNodeElectedMaster() {
        return MasterServiceTests.discoveryState(clusterManagerService).nodes().isLocalNodeElectedMaster();
    }

    private boolean clusterStateHasNode(DiscoveryNode node) {
        return node.equals(MasterServiceTests.discoveryState(clusterManagerService).nodes().get(node.getId()));
    }

    private static ClusterState initialStateWithDecommissionedAttribute(
        ClusterState clusterState,
        DecommissionAttribute decommissionAttribute
    ) {
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(
            decommissionAttribute,
            DecommissionStatus.SUCCESSFUL,
            randomAlphaOfLength(10)
        );
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).decommissionAttributeMetadata(decommissionAttributeMetadata))
            .build();
    }
}
