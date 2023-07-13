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

package org.opensearch.action.admin.cluster.configuration;

import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.ClusterStateObserver.Listener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes.Builder;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.Strings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.Names;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.opensearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction.MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING;
import static org.opensearch.cluster.ClusterState.builder;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class TransportAddVotingConfigExclusionsActionTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;
    private static ClusterService clusterService;
    private static DiscoveryNode localNode, otherNode1, otherNode2, otherDataNode;
    private static VotingConfigExclusion localNodeExclusion, otherNode1Exclusion, otherNode2Exclusion;

    private TransportService transportService;
    private ClusterStateObserver clusterStateObserver;
    private ClusterSettings clusterSettings;
    private int staticMaximum;

    @BeforeClass
    public static void createThreadPoolAndClusterService() {
        threadPool = new TestThreadPool("test", Settings.EMPTY);
        localNode = makeDiscoveryNode("local");
        localNodeExclusion = new VotingConfigExclusion(localNode);
        otherNode1 = makeDiscoveryNode("other1");
        otherNode1Exclusion = new VotingConfigExclusion(otherNode1);
        otherNode2 = makeDiscoveryNode("other2");
        otherNode2Exclusion = new VotingConfigExclusion(otherNode2);
        otherDataNode = new DiscoveryNode("data", "data", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        clusterService = createClusterService(threadPool, localNode);
    }

    private static DiscoveryNode makeDiscoveryNode(String name) {
        return new DiscoveryNode(
            name,
            name,
            buildNewFakeTransportAddress(),
            emptyMap(),
            singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
    }

    @AfterClass
    public static void shutdownThreadPoolAndClusterService() {
        clusterService.stop();
        threadPool.shutdown();
    }

    @Before
    public void setupForTest() {
        final MockTransport transport = new MockTransport();
        transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode,
            null,
            emptySet()
        );

        final Settings.Builder nodeSettingsBuilder = Settings.builder();
        if (randomBoolean()) {
            staticMaximum = between(5, 15);
            nodeSettingsBuilder.put(MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.getKey(), staticMaximum);
        } else {
            staticMaximum = MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.get(Settings.EMPTY);
        }
        final Settings nodeSettings = nodeSettingsBuilder.build();
        clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        new TransportAddVotingConfigExclusionsAction(
            nodeSettings,
            clusterSettings,
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY))
        ); // registers action

        transportService.start();
        transportService.acceptIncomingRequests();

        final VotingConfiguration allNodesConfig = VotingConfiguration.of(localNode, otherNode1, otherNode2);

        setState(
            clusterService,
            builder(new ClusterName("cluster")).nodes(
                new Builder().add(localNode)
                    .add(otherNode1)
                    .add(otherNode2)
                    .add(otherDataNode)
                    .localNodeId(localNode.getId())
                    .clusterManagerNodeId(localNode.getId())
            )
                .metadata(
                    Metadata.builder()
                        .coordinationMetadata(
                            CoordinationMetadata.builder()
                                .lastAcceptedConfiguration(allNodesConfig)
                                .lastCommittedConfiguration(allNodesConfig)
                                .build()
                        )
                )
        );

        clusterStateObserver = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());
    }

    public void testWithdrawsVoteFromANode() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), contains(otherNode1Exclusion));
    }

    public void testWithdrawsVotesFromMultipleNodes() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1", "other2"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(
            clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
            containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion)
        );
    }

    public void testWithdrawsVotesFromNodesMatchingWildcard() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            makeRequestWithNodeDescriptions("other*"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(
            clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
            containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion)
        );
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    public void testWithdrawsVotesFromAllClusterManagerEligibleNodes() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            makeRequestWithNodeDescriptions("_all"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(
            clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
            containsInAnyOrder(localNodeExclusion, otherNode1Exclusion, otherNode2Exclusion)
        );
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    public void testWithdrawsVoteFromLocalNode() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            makeRequestWithNodeDescriptions("_local"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), contains(localNodeExclusion));
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    public void testReturnsImmediatelyIfVoteAlreadyWithdrawn() throws InterruptedException {
        final ClusterState state = clusterService.state();
        setState(
            clusterService,
            builder(state).metadata(
                Metadata.builder(state.metadata())
                    .coordinationMetadata(
                        CoordinationMetadata.builder(state.coordinationMetadata())
                            .lastCommittedConfiguration(VotingConfiguration.of(localNode, otherNode2))
                            .lastAcceptedConfiguration(VotingConfiguration.of(localNode, otherNode2))
                            .build()
                    )
            )
        );

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        // no observer to reconfigure
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), contains(otherNode1Exclusion));
    }

    public void testReturnsErrorIfNoMatchingNodeDescriptions() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final SetOnce<TransportException> exceptionHolder = new SetOnce<>();

        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            makeRequestWithNodeDescriptions("not-a-node"),
            expectError(e -> {
                exceptionHolder.set(e);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        final Throwable rootCause = exceptionHolder.get().getRootCause();
        assertThat(rootCause, instanceOf(IllegalArgumentException.class));
        assertThat(
            rootCause.getMessage(),
            equalTo("add voting config exclusions request for [not-a-node] matched no cluster-manager-eligible nodes")
        );
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    public void testOnlyMatchesClusterManagerEligibleNodes() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final SetOnce<TransportException> exceptionHolder = new SetOnce<>();

        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            makeRequestWithNodeDescriptions("_all", "cluster_manager:false"),
            expectError(e -> {
                exceptionHolder.set(e);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        final Throwable rootCause = exceptionHolder.get().getRootCause();
        assertThat(rootCause, instanceOf(IllegalArgumentException.class));
        assertThat(
            rootCause.getMessage(),
            equalTo("add voting config exclusions request for [_all, cluster_manager:false] matched no cluster-manager-eligible nodes")
        );
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    public void testExcludeAbsentNodesByNodeIds() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(
                Strings.EMPTY_ARRAY,
                new String[] { "absent_id" },
                Strings.EMPTY_ARRAY,
                TimeValue.timeValueSeconds(30)
            ),
            expectSuccess(e -> {
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertEquals(
            singleton(new VotingConfigExclusion("absent_id", VotingConfigExclusion.MISSING_VALUE_MARKER)),
            clusterService.getClusterApplierService().state().getVotingConfigExclusions()
        );
    }

    public void testExcludeExistingNodesByNodeIds() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(
                Strings.EMPTY_ARRAY,
                new String[] { "other1", "other2" },
                Strings.EMPTY_ARRAY,
                TimeValue.timeValueSeconds(30)
            ),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(
            clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
            containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion)
        );
    }

    public void testExcludeAbsentNodesByNodeNames() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("absent_node"),
            expectSuccess(e -> {
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertEquals(
            singleton(new VotingConfigExclusion(VotingConfigExclusion.MISSING_VALUE_MARKER, "absent_node")),
            clusterService.getClusterApplierService().state().getVotingConfigExclusions()
        );
    }

    public void testExcludeExistingNodesByNodeNames() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1", "other2"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(
            clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
            containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion)
        );
    }

    public void testSucceedsEvenIfAllExclusionsAlreadyAdded() throws InterruptedException {
        final ClusterState state = clusterService.state();
        final ClusterState.Builder builder = builder(state);
        builder.metadata(
            Metadata.builder(state.metadata())
                .coordinationMetadata(
                    CoordinationMetadata.builder(state.coordinationMetadata()).addVotingConfigExclusion(otherNode1Exclusion).build()
                )
        );
        setState(clusterService, builder);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), contains(otherNode1Exclusion));
    }

    public void testExcludeByNodeIdSucceedsEvenIfAllExclusionsAlreadyAdded() throws InterruptedException {
        final ClusterState state = clusterService.state();
        final ClusterState.Builder builder = builder(state);
        builder.metadata(
            Metadata.builder(state.metadata())
                .coordinationMetadata(
                    CoordinationMetadata.builder(state.coordinationMetadata()).addVotingConfigExclusion(otherNode1Exclusion).build()
                )
        );
        setState(clusterService, builder);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(
                Strings.EMPTY_ARRAY,
                new String[] { "other1" },
                Strings.EMPTY_ARRAY,
                TimeValue.timeValueSeconds(30)
            ),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), contains(otherNode1Exclusion));
    }

    public void testExcludeByNodeNameSucceedsEvenIfAllExclusionsAlreadyAdded() throws InterruptedException {
        final ClusterState state = clusterService.state();
        final ClusterState.Builder builder = builder(state);
        builder.metadata(
            Metadata.builder(state.metadata())
                .coordinationMetadata(
                    CoordinationMetadata.builder(state.coordinationMetadata()).addVotingConfigExclusion(otherNode1Exclusion).build()
                )
        );
        setState(clusterService, builder);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), contains(otherNode1Exclusion));
    }

    public void testReturnsErrorIfMaximumExclusionCountExceeded() throws InterruptedException {
        final Metadata.Builder metadataBuilder = Metadata.builder(clusterService.state().metadata());
        CoordinationMetadata.Builder coordinationMetadataBuilder = CoordinationMetadata.builder(
            clusterService.state().coordinationMetadata()
        ).addVotingConfigExclusion(localNodeExclusion);

        final int actualMaximum;
        if (randomBoolean()) {
            actualMaximum = staticMaximum;
        } else {
            actualMaximum = between(2, 15);
            clusterSettings.applySettings(
                Settings.builder()
                    .put(clusterService.state().metadata().persistentSettings())
                    .put(MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.getKey(), actualMaximum)
                    .build()
            );
        }

        for (int i = 2; i < actualMaximum; i++) {
            coordinationMetadataBuilder.addVotingConfigExclusion(
                new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10))
            );
        }

        final int existingCount, newCount;
        if (randomBoolean()) {
            coordinationMetadataBuilder.addVotingConfigExclusion(otherNode1Exclusion);
            existingCount = actualMaximum;
            newCount = 1;
        } else {
            existingCount = actualMaximum - 1;
            newCount = 2;
        }

        metadataBuilder.coordinationMetadata(coordinationMetadataBuilder.build());

        final ClusterState.Builder builder = builder(clusterService.state()).metadata(metadataBuilder);
        setState(clusterService, builder);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final SetOnce<TransportException> exceptionHolder = new SetOnce<>();

        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            makeRequestWithNodeDescriptions("other*"),
            expectError(e -> {
                exceptionHolder.set(e);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        final Throwable rootCause = exceptionHolder.get().getRootCause();
        assertThat(rootCause, instanceOf(IllegalArgumentException.class));
        assertThat(
            rootCause.getMessage(),
            equalTo(
                "add voting config exclusions request for [other*] would add ["
                    + newCount
                    + "] exclusions to the existing ["
                    + existingCount
                    + "] which would exceed the maximum of ["
                    + actualMaximum
                    + "] set by [cluster.max_voting_config_exclusions]"
            )
        );
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    public void testTimesOut() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final SetOnce<TransportException> exceptionHolder = new SetOnce<>();

        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(
                new String[] { "other1" },
                Strings.EMPTY_ARRAY,
                Strings.EMPTY_ARRAY,
                TimeValue.timeValueMillis(100)
            ),
            expectError(e -> {
                exceptionHolder.set(e);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        final Throwable rootCause = exceptionHolder.get().getRootCause();
        assertThat(rootCause, instanceOf(OpenSearchTimeoutException.class));
        assertThat(rootCause.getMessage(), startsWith("timed out waiting for voting config exclusions [{other1}"));
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);

    }

    private TransportResponseHandler<AddVotingConfigExclusionsResponse> expectSuccess(
        Consumer<AddVotingConfigExclusionsResponse> onResponse
    ) {
        return responseHandler(onResponse, e -> { throw new AssertionError("unexpected", e); });
    }

    private TransportResponseHandler<AddVotingConfigExclusionsResponse> expectError(Consumer<TransportException> onException) {
        return responseHandler(r -> { assert false : r; }, onException);
    }

    private TransportResponseHandler<AddVotingConfigExclusionsResponse> responseHandler(
        Consumer<AddVotingConfigExclusionsResponse> onResponse,
        Consumer<TransportException> onException
    ) {
        return new TransportResponseHandler<AddVotingConfigExclusionsResponse>() {
            @Override
            public void handleResponse(AddVotingConfigExclusionsResponse response) {
                onResponse.accept(response);
            }

            @Override
            public void handleException(TransportException exp) {
                onException.accept(exp);
            }

            @Override
            public String executor() {
                return Names.SAME;
            }

            @Override
            public AddVotingConfigExclusionsResponse read(StreamInput in) throws IOException {
                return new AddVotingConfigExclusionsResponse(in);
            }
        };
    }

    private static class AdjustConfigurationForExclusions implements Listener {

        final CountDownLatch doneLatch;

        AdjustConfigurationForExclusions(CountDownLatch latch) {
            this.doneLatch = latch;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            clusterService.getClusterManagerService().submitStateUpdateTask("reconfiguration", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    assertThat(currentState, sameInstance(state));
                    final Set<String> votingNodeIds = new HashSet<>();
                    currentState.nodes().forEach(n -> votingNodeIds.add(n.getId()));
                    currentState.getVotingConfigExclusions().forEach(t -> votingNodeIds.remove(t.getNodeId()));
                    final VotingConfiguration votingConfiguration = new VotingConfiguration(votingNodeIds);
                    return builder(currentState).metadata(
                        Metadata.builder(currentState.metadata())
                            .coordinationMetadata(
                                CoordinationMetadata.builder(currentState.coordinationMetadata())
                                    .lastAcceptedConfiguration(votingConfiguration)
                                    .lastCommittedConfiguration(votingConfiguration)
                                    .build()
                            )
                    ).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    throw new AssertionError("unexpected failure", e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    doneLatch.countDown();
                }
            });
        }

        @Override
        public void onClusterServiceClose() {
            throw new AssertionError("unexpected close");
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            throw new AssertionError("unexpected timeout");
        }
    }

    private AddVotingConfigExclusionsRequest makeRequestWithNodeDescriptions(String... nodeDescriptions) {
        return new AddVotingConfigExclusionsRequest(
            nodeDescriptions,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            TimeValue.timeValueSeconds(30)
        );
    }

}
