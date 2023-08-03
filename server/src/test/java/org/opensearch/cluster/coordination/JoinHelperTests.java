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

import org.apache.logging.log4j.Level;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.monitor.StatusInfo;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.test.transport.CapturingTransport.CapturedRequest;
import org.opensearch.test.transport.MockTransport;
import org.opensearch.transport.BytesTransportRequest;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.cluster.coordination.JoinHelper.VALIDATE_COMPRESSED_JOIN_ACTION_NAME;
import static org.opensearch.cluster.coordination.JoinHelper.VALIDATE_JOIN_ACTION_NAME;
import static org.opensearch.monitor.StatusInfo.Status.HEALTHY;
import static org.opensearch.monitor.StatusInfo.Status.UNHEALTHY;
import static org.opensearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class JoinHelperTests extends OpenSearchTestCase {
    private final NamedWriteableRegistry namedWriteableRegistry = DEFAULT_NAMED_WRITABLE_REGISTRY;

    public void testJoinDeduplication() {
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "node0").build(),
            random()
        );
        CapturingTransport capturingTransport = new CapturingTransport();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        TransportService transportService = capturingTransport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode,
            null,
            Collections.emptySet()
        );
        JoinHelper joinHelper = new JoinHelper(
            Settings.EMPTY,
            null,
            null,
            transportService,
            () -> 0L,
            () -> null,
            (joinRequest, joinCallback) -> {
                throw new AssertionError();
            },
            startJoinRequest -> { throw new AssertionError(); },
            Collections.emptyList(),
            (s, p, r) -> {},
            () -> new StatusInfo(HEALTHY, "info"),
            nodeCommissioned -> {},
            namedWriteableRegistry
        );
        transportService.start();

        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);

        assertFalse(joinHelper.isJoinPending());

        // check that sending a join to node1 works
        Optional<Join> optionalJoin1 = randomBoolean()
            ? Optional.empty()
            : Optional.of(new Join(localNode, node1, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        CapturedRequest[] capturedRequests1 = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1.length, equalTo(1));
        CapturedRequest capturedRequest1 = capturedRequests1[0];
        assertEquals(node1, capturedRequest1.node);

        assertTrue(joinHelper.isJoinPending());

        // check that sending a join to node2 works
        Optional<Join> optionalJoin2 = randomBoolean()
            ? Optional.empty()
            : Optional.of(new Join(localNode, node2, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node2, 0L, optionalJoin2);
        CapturedRequest[] capturedRequests2 = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests2.length, equalTo(1));
        CapturedRequest capturedRequest2 = capturedRequests2[0];
        assertEquals(node2, capturedRequest2.node);

        // check that sending another join to node1 is a noop as the previous join is still in progress
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        assertThat(capturingTransport.getCapturedRequestsAndClear().length, equalTo(0));

        // complete the previous join to node1
        if (randomBoolean()) {
            capturingTransport.handleResponse(capturedRequest1.requestId, TransportResponse.Empty.INSTANCE);
        } else {
            capturingTransport.handleRemoteError(capturedRequest1.requestId, new CoordinationStateRejectedException("dummy"));
        }

        // check that sending another join to node1 now works again
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        CapturedRequest[] capturedRequests1a = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1a.length, equalTo(1));
        CapturedRequest capturedRequest1a = capturedRequests1a[0];
        assertEquals(node1, capturedRequest1a.node);

        // check that sending another join to node2 works if the optionalJoin is different
        Optional<Join> optionalJoin2a = optionalJoin2.isPresent() && randomBoolean()
            ? Optional.empty()
            : Optional.of(new Join(localNode, node2, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node2, 0L, optionalJoin2a);
        CapturedRequest[] capturedRequests2a = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests2a.length, equalTo(1));
        CapturedRequest capturedRequest2a = capturedRequests2a[0];
        assertEquals(node2, capturedRequest2a.node);

        // complete all the joins and check that isJoinPending is updated
        assertTrue(joinHelper.isJoinPending());
        capturingTransport.handleRemoteError(capturedRequest2.requestId, new CoordinationStateRejectedException("dummy"));
        capturingTransport.handleRemoteError(capturedRequest1a.requestId, new CoordinationStateRejectedException("dummy"));
        capturingTransport.handleRemoteError(capturedRequest2a.requestId, new CoordinationStateRejectedException("dummy"));
        assertFalse(joinHelper.isJoinPending());
    }

    public void testFailedJoinAttemptLogLevel() {
        assertThat(JoinHelper.FailedJoinAttempt.getLogLevel(new TransportException("generic transport exception")), is(Level.INFO));

        assertThat(
            JoinHelper.FailedJoinAttempt.getLogLevel(
                new RemoteTransportException("remote transport exception with generic cause", new Exception())
            ),
            is(Level.INFO)
        );

        assertThat(
            JoinHelper.FailedJoinAttempt.getLogLevel(
                new RemoteTransportException("caused by CoordinationStateRejectedException", new CoordinationStateRejectedException("test"))
            ),
            is(Level.DEBUG)
        );

        assertThat(
            JoinHelper.FailedJoinAttempt.getLogLevel(
                new RemoteTransportException(
                    "caused by FailedToCommitClusterStateException",
                    new FailedToCommitClusterStateException("test")
                )
            ),
            is(Level.DEBUG)
        );

        assertThat(
            JoinHelper.FailedJoinAttempt.getLogLevel(
                new RemoteTransportException("caused by NotClusterManagerException", new NotClusterManagerException("test"))
            ),
            is(Level.DEBUG)
        );
    }

    public void testJoinValidationRejectsMismatchedClusterUUID() throws IOException {
        assertJoinValidationRejectsMismatchedClusterUUID(
            VALIDATE_COMPRESSED_JOIN_ACTION_NAME,
            "join validation on cluster state with a different cluster uuid"
        );
        assertJoinValidationRejectsMismatchedClusterUUID(
            VALIDATE_JOIN_ACTION_NAME,
            "join validation on cluster state with a different cluster uuid"
        );
    }

    private void assertJoinValidationRejectsMismatchedClusterUUID(String actionName, String expectedMessage) throws IOException {
        TestClusterSetup testCluster = getTestClusterSetup(null, false);

        final ClusterState otherClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().generateClusterUuidIfNeeded())
            .build();
        TransportRequest request;
        final PlainActionFuture<TransportResponse.Empty> future = new PlainActionFuture<>();
        if (actionName.equals(VALIDATE_COMPRESSED_JOIN_ACTION_NAME)) {
            BytesReference bytes = CompressedStreamUtils.createCompressedStream(
                testCluster.localNode.getVersion(),
                otherClusterState::writeTo
            );
            request = new BytesTransportRequest(bytes, testCluster.localNode.getVersion());
            testCluster.transportService.sendRequest(
                testCluster.localNode,
                actionName,
                request,
                new ActionListenerResponseHandler<>(future, in -> TransportResponse.Empty.INSTANCE)
            );
        } else if (actionName.equals(VALIDATE_JOIN_ACTION_NAME)) {
            request = new ValidateJoinRequest(otherClusterState);
            testCluster.transportService.sendRequest(
                testCluster.localNode,
                actionName,
                request,
                new ActionListenerResponseHandler<>(future, in -> TransportResponse.Empty.INSTANCE)
            );
        }

        testCluster.deterministicTaskQueue.runAllTasks();

        final CoordinationStateRejectedException coordinationStateRejectedException = expectThrows(
            CoordinationStateRejectedException.class,
            future::actionGet
        );
        assertThat(coordinationStateRejectedException.getMessage(), containsString(expectedMessage));
        assertThat(coordinationStateRejectedException.getMessage(), containsString(testCluster.localClusterState.metadata().clusterUUID()));
        assertThat(coordinationStateRejectedException.getMessage(), containsString(otherClusterState.metadata().clusterUUID()));
    }

    public void testJoinFailureOnUnhealthyNodes() {
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "node0").build(),
            random()
        );
        CapturingTransport capturingTransport = new CapturingTransport();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        TransportService transportService = capturingTransport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode,
            null,
            Collections.emptySet()
        );
        AtomicReference<StatusInfo> nodeHealthServiceStatus = new AtomicReference<>(new StatusInfo(UNHEALTHY, "unhealthy-info"));
        JoinHelper joinHelper = new JoinHelper(
            Settings.EMPTY,
            null,
            null,
            transportService,
            () -> 0L,
            () -> null,
            (joinRequest, joinCallback) -> {
                throw new AssertionError();
            },
            startJoinRequest -> { throw new AssertionError(); },
            Collections.emptyList(),
            (s, p, r) -> {},
            () -> nodeHealthServiceStatus.get(),
            nodeCommissioned -> {},
            namedWriteableRegistry
        );
        transportService.start();

        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);

        assertFalse(joinHelper.isJoinPending());

        // check that sending a join to node1 doesn't work
        Optional<Join> optionalJoin1 = randomBoolean()
            ? Optional.empty()
            : Optional.of(new Join(localNode, node1, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node1, randomNonNegativeLong(), optionalJoin1);
        CapturedRequest[] capturedRequests1 = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1.length, equalTo(0));

        assertFalse(joinHelper.isJoinPending());

        // check that sending a join to node2 doesn't work
        Optional<Join> optionalJoin2 = randomBoolean()
            ? Optional.empty()
            : Optional.of(new Join(localNode, node2, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));

        transportService.start();
        joinHelper.sendJoinRequest(node2, randomNonNegativeLong(), optionalJoin2);

        CapturedRequest[] capturedRequests2 = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests2.length, equalTo(0));

        assertFalse(joinHelper.isJoinPending());

        nodeHealthServiceStatus.getAndSet(new StatusInfo(HEALTHY, "healthy-info"));
        // check that sending another join to node1 now works again
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        CapturedRequest[] capturedRequests1a = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1a.length, equalTo(1));
        CapturedRequest capturedRequest1a = capturedRequests1a[0];
        assertEquals(node1, capturedRequest1a.node);
    }

    public void testSendCompressedValidateJoinFailOnSerializeFailure() throws ExecutionException, InterruptedException, TimeoutException {
        TestClusterSetup testCluster = getTestClusterSetup(Version.CURRENT, false);
        final CompletableFuture<Throwable> future = new CompletableFuture<>();
        testCluster.joinHelper.sendValidateJoinRequest(testCluster.localNode, null, new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse.Empty empty) {
                future.completeExceptionally(new AssertionError("validate join should have failed"));
            }

            @Override
            public void onFailure(Exception e) {
                future.complete(e);
            }
        });
        Throwable t = future.get(10, TimeUnit.SECONDS);
        assertTrue(t instanceof NullPointerException);
    }

    public void testValidateJoinSentWithCorrectActionForVersions() {
        verifyValidateJoinActionSent(VALIDATE_JOIN_ACTION_NAME, Version.V_2_1_0);
        verifyValidateJoinActionSent(VALIDATE_JOIN_ACTION_NAME, Version.V_2_7_0);
        verifyValidateJoinActionSent(VALIDATE_JOIN_ACTION_NAME, Version.V_2_8_0);
        verifyValidateJoinActionSent(VALIDATE_COMPRESSED_JOIN_ACTION_NAME, Version.CURRENT);
    }

    private void verifyValidateJoinActionSent(String expectedActionName, Version version) {
        TestClusterSetup testCluster = getTestClusterSetup(version, true);
        final CompletableFuture<Throwable> future = new CompletableFuture<>();
        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), version);
        testCluster.joinHelper.sendValidateJoinRequest(node1, testCluster.localClusterState, new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse.Empty empty) {
                throw new AssertionError("capturing transport shouldn't run");
            }

            @Override
            public void onFailure(Exception e) {
                future.complete(e);
            }
        });

        CapturedRequest[] validateRequests = testCluster.capturingTransport.getCapturedRequestsAndClear();
        assertEquals(1, validateRequests.length);
        assertEquals(expectedActionName, validateRequests[0].action);
    }

    public void testJoinValidationFailsOnDecompressionFailure() {
        TestClusterSetup testCluster = getTestClusterSetup(Version.CURRENT, false);
        TransportRequest request;
        final PlainActionFuture<TransportResponse.Empty> future = new PlainActionFuture<>();
        request = new BytesTransportRequest(null, testCluster.localNode.getVersion());
        testCluster.transportService.sendRequest(
            testCluster.localNode,
            VALIDATE_COMPRESSED_JOIN_ACTION_NAME,
            request,
            new ActionListenerResponseHandler<>(future, in -> TransportResponse.Empty.INSTANCE)
        );
        testCluster.deterministicTaskQueue.runAllTasks();
        expectThrows(NullPointerException.class, future::actionGet);
    }

    public void testJoinHelperCachingOnClusterState() throws ExecutionException, InterruptedException, TimeoutException {
        TestClusterSetup testCluster = getTestClusterSetup(Version.CURRENT, false);
        final CompletableFuture<Throwable> future = new CompletableFuture<>();
        ActionListener<TransportResponse.Empty> listener = new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse.Empty empty) {
                logger.info("validation successful");
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(new AssertionError("validate join should not fail here"));
            }
        };
        testCluster.joinHelper.sendValidateJoinRequest(testCluster.localNode, testCluster.localClusterState, listener);
        // validation will pass due to cached cluster state
        ClusterState randomState = ClusterState.builder(new ClusterName("random"))
            .stateUUID("random2")
            .version(testCluster.localClusterState.version())
            .build();
        testCluster.joinHelper.sendValidateJoinRequest(testCluster.localNode, randomState, listener);

        final CompletableFuture<Throwable> future2 = new CompletableFuture<>();
        ActionListener<TransportResponse.Empty> listener2 = new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse.Empty empty) {
                future2.completeExceptionally(new AssertionError("validation should fail now"));
            }

            @Override
            public void onFailure(Exception e) {
                future2.complete(e);
            }
        };
        ClusterState randomState2 = ClusterState.builder(new ClusterName("random"))
            .stateUUID("random2")
            .version(testCluster.localClusterState.version() + 1)
            .build();
        // now sending the validate join request will fail due to random cluster uuid because version is changed
        // and cache will be invalidated
        testCluster.joinHelper.sendValidateJoinRequest(testCluster.localNode, randomState2, listener2);
        testCluster.deterministicTaskQueue.runAllTasks();

        Throwable t = future2.get(10, TimeUnit.SECONDS);
        assertTrue(t instanceof RemoteTransportException);
        assertTrue(t.getCause() instanceof CoordinationStateRejectedException);
        assertTrue(t.getCause().getMessage().contains("different cluster uuid"));
    }

    private TestClusterSetup getTestClusterSetup(Version version, boolean isCapturingTransport) {
        version = version == null ? Version.CURRENT : version;
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "node0").build(),
            random()
        );
        MockTransport mockTransport = new MockTransport();
        CapturingTransport capturingTransport = new CapturingTransport();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), version);

        final ClusterState localClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().generateClusterUuidIfNeeded().clusterUUIDCommitted(true))
            .build();
        TransportService transportService;
        if (isCapturingTransport) {
            transportService = capturingTransport.createTransportService(
                Settings.EMPTY,
                deterministicTaskQueue.getThreadPool(),
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> localNode,
                null,
                Collections.emptySet()
            );
        } else {
            transportService = mockTransport.createTransportService(
                Settings.EMPTY,
                deterministicTaskQueue.getThreadPool(),
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> localNode,
                null,
                Collections.emptySet()
            );
        }
        JoinHelper joinHelper = new JoinHelper(
            Settings.EMPTY,
            null,
            null,
            transportService,
            () -> 0L,
            () -> localClusterState,
            (joinRequest, joinCallback) -> {
                throw new AssertionError();
            },
            startJoinRequest -> { throw new AssertionError(); },
            Collections.emptyList(),
            (s, p, r) -> {},
            null,
            nodeCommissioned -> {},
            namedWriteableRegistry
        ); // registers
           // request
           // handler
        transportService.start();
        transportService.acceptIncomingRequests();
        return new TestClusterSetup(deterministicTaskQueue, localNode, transportService, localClusterState, joinHelper, capturingTransport);
    }

    private static class TestClusterSetup {
        public final DeterministicTaskQueue deterministicTaskQueue;
        public final DiscoveryNode localNode;
        public TransportService transportService;
        public final ClusterState localClusterState;
        public final JoinHelper joinHelper;
        public final CapturingTransport capturingTransport;

        public TestClusterSetup(
            DeterministicTaskQueue deterministicTaskQueue,
            DiscoveryNode localNode,
            TransportService transportService,
            ClusterState localClusterState,
            JoinHelper joinHelper,
            CapturingTransport capturingTransport
        ) {
            this.deterministicTaskQueue = deterministicTaskQueue;
            this.localNode = localNode;
            this.transportService = transportService;
            this.localClusterState = localClusterState;
            this.joinHelper = joinHelper;
            this.capturingTransport = capturingTransport;
        }
    }
}
