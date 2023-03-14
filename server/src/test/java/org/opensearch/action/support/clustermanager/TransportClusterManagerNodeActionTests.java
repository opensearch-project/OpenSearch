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

package org.opensearch.action.support.clustermanager;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterManagerThrottlingException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.discovery.ClusterManagerNotDiscoveredException;
import org.opensearch.node.NodeClosedException;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TransportClusterManagerNodeActionTests extends OpenSearchTestCase {
    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;
    private CapturingTransport transport;
    private DiscoveryNode localNode;
    private DiscoveryNode remoteNode;
    private DiscoveryNode[] allNodes;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportMasterNodeActionTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        localNode = new DiscoveryNode(
            "local_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        remoteNode = new DiscoveryNode(
            "remote_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        allNodes = new DiscoveryNode[] { localNode, remoteNode };
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    void assertListenerThrows(String msg, ActionFuture<?> listener, Class<?> klass) throws InterruptedException {
        try {
            listener.get();
            fail(msg);
        } catch (ExecutionException ex) {
            assertThat(ex.getCause(), instanceOf(klass));
        }
    }

    public static class Request extends ClusterManagerNodeRequest<Request> {
        Request() {}

        Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    class Response extends ActionResponse {
        private long identity = randomLong();

        Response() {}

        Response(StreamInput in) throws IOException {
            super(in);
            identity = in.readLong();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return identity == response.identity;
        }

        @Override
        public int hashCode() {
            return Objects.hash(identity);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(identity);
        }
    }

    class Action extends TransportClusterManagerNodeAction<Request, Response> {
        Action(String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
            super(
                actionName,
                transportService,
                clusterService,
                threadPool,
                new ActionFilters(new HashSet<>()),
                Request::new,
                new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY))
            );
        }

        @Override
        protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
            // remove unneeded threading by wrapping listener with SAME to prevent super.doExecute from wrapping it with LISTENER
            super.doExecute(task, request, new ThreadedActionListener<>(logger, threadPool, ThreadPool.Names.SAME, listener, false));
        }

        @Override
        protected String executor() {
            // very lightweight operation in memory, no need to fork to a thread
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void clusterManagerOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            listener.onResponse(new Response()); // default implementation, overridden in specific tests
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return null; // default implementation, overridden in specific tests
        }
    }

    public void testLocalOperationWithoutBlocks() throws ExecutionException, InterruptedException {
        final boolean clusterManagerOperationFailure = randomBoolean();

        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        final Exception exception = new Exception();
        final Response response = new Response();

        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));

        new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected void clusterManagerOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
                if (clusterManagerOperationFailure) {
                    listener.onFailure(exception);
                } else {
                    listener.onResponse(response);
                }
            }
        }.execute(request, listener);
        assertTrue(listener.isDone());

        if (clusterManagerOperationFailure) {
            try {
                listener.get();
                fail("Expected exception but returned proper result");
            } catch (ExecutionException ex) {
                assertThat(ex.getCause(), equalTo(exception));
            }
        } else {
            assertThat(listener.get(), equalTo(response));
        }
    }

    /* The test is copied from testLocalOperationWithoutBlocks()
    to validate the backwards compatibility for the deprecated method masterOperation(with task parameter). */
    public void testDeprecatedMasterOperationWithTaskParameterCanBeCalled() throws ExecutionException, InterruptedException {
        final boolean clusterManagerOperationFailure = randomBoolean();

        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        final Exception exception = new Exception();
        final Response response = new Response();

        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));

        new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
                if (clusterManagerOperationFailure) {
                    listener.onFailure(exception);
                } else {
                    listener.onResponse(response);
                }
            }
        }.execute(request, listener);
        assertTrue(listener.isDone());

        if (clusterManagerOperationFailure) {
            try {
                listener.get();
                fail("Expected exception but returned proper result");
            } catch (ExecutionException ex) {
                assertThat(ex.getCause(), equalTo(exception));
            }
        } else {
            assertThat(listener.get(), equalTo(response));
        }
    }

    public void testLocalOperationWithBlocks() throws ExecutionException, InterruptedException {
        final boolean retryableBlock = randomBoolean();
        final boolean unblockBeforeTimeout = randomBoolean();

        Request request = new Request().clusterManagerNodeTimeout(TimeValue.timeValueSeconds(unblockBeforeTimeout ? 60 : 0));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        ClusterBlock block = new ClusterBlock(1, "", retryableBlock, true, false, randomFrom(RestStatus.values()), ClusterBlockLevel.ALL);
        ClusterState stateWithBlock = ClusterState.builder(ClusterStateCreationUtils.state(localNode, localNode, allNodes))
            .blocks(ClusterBlocks.builder().addGlobalBlock(block))
            .build();
        setState(clusterService, stateWithBlock);

        new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected ClusterBlockException checkBlock(Request request, ClusterState state) {
                Set<ClusterBlock> blocks = state.blocks().global();
                return blocks.isEmpty() ? null : new ClusterBlockException(blocks);
            }
        }.execute(request, listener);

        if (retryableBlock && unblockBeforeTimeout) {
            assertFalse(listener.isDone());
            setState(
                clusterService,
                ClusterState.builder(ClusterStateCreationUtils.state(localNode, localNode, allNodes))
                    .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
                    .build()
            );
            assertTrue(listener.isDone());
            listener.get();
            return;
        }

        assertTrue(listener.isDone());
        if (retryableBlock) {
            try {
                listener.get();
                fail("Expected exception but returned proper result");
            } catch (ExecutionException ex) {
                assertThat(ex.getCause(), instanceOf(ClusterManagerNotDiscoveredException.class));
                assertThat(ex.getCause().getCause(), instanceOf(ClusterBlockException.class));
            }
        } else {
            assertListenerThrows("ClusterBlockException should be thrown", listener, ClusterBlockException.class);
        }
    }

    public void testCheckBlockThrowsException() throws InterruptedException {
        boolean throwExceptionOnRetry = randomBoolean();
        Request request = new Request().clusterManagerNodeTimeout(TimeValue.timeValueSeconds(60));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        ClusterBlock block = new ClusterBlock(1, "", true, true, false, randomFrom(RestStatus.values()), ClusterBlockLevel.ALL);
        ClusterState stateWithBlock = ClusterState.builder(ClusterStateCreationUtils.state(localNode, localNode, allNodes))
            .blocks(ClusterBlocks.builder().addGlobalBlock(block))
            .build();
        setState(clusterService, stateWithBlock);

        new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected ClusterBlockException checkBlock(Request request, ClusterState state) {
                Set<ClusterBlock> blocks = state.blocks().global();
                if (throwExceptionOnRetry == false || blocks.isEmpty()) {
                    throw new RuntimeException("checkBlock has thrown exception");
                }
                return new ClusterBlockException(blocks);

            }
        }.execute(request, listener);

        if (throwExceptionOnRetry == false) {
            assertListenerThrows("checkBlock has thrown exception", listener, RuntimeException.class);
        } else {
            assertFalse(listener.isDone());
            setState(
                clusterService,
                ClusterState.builder(ClusterStateCreationUtils.state(localNode, localNode, allNodes))
                    .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
                    .build()
            );
            assertListenerThrows("checkBlock has thrown exception", listener, RuntimeException.class);
        }
    }

    public void testForceLocalOperation() throws ExecutionException, InterruptedException {
        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        setState(clusterService, ClusterStateCreationUtils.state(localNode, randomFrom(localNode, remoteNode, null), allNodes));

        new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected boolean localExecute(Request request) {
                return true;
            }
        }.execute(request, listener);

        assertTrue(listener.isDone());
        listener.get();
    }

    public void testClusterManagerNotAvailable() throws ExecutionException, InterruptedException {
        Request request = new Request().clusterManagerNodeTimeout(TimeValue.timeValueSeconds(0));
        setState(clusterService, ClusterStateCreationUtils.state(localNode, null, allNodes));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        new Action("internal:testAction", transportService, clusterService, threadPool).execute(request, listener);
        assertTrue(listener.isDone());
        assertListenerThrows("ClusterManagerNotDiscoveredException should be thrown", listener, ClusterManagerNotDiscoveredException.class);
    }

    public void testClusterManagerBecomesAvailable() throws ExecutionException, InterruptedException {
        Request request = new Request();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, null, allNodes));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        new Action("internal:testAction", transportService, clusterService, threadPool).execute(request, listener);
        assertFalse(listener.isDone());
        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));
        assertTrue(listener.isDone());
        listener.get();
    }

    public void testDelegateToClusterManager() throws ExecutionException, InterruptedException {
        Request request = new Request();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes));

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        new Action("internal:testAction", transportService, clusterService, threadPool).execute(request, listener);

        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[0];
        assertTrue(capturedRequest.node.isClusterManagerNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("internal:testAction"));

        Response response = new Response();
        transport.handleResponse(capturedRequest.requestId, response);
        assertTrue(listener.isDone());
        assertThat(listener.get(), equalTo(response));
    }

    public void testDelegateToFailingClusterManager() throws ExecutionException, InterruptedException {
        boolean failsWithConnectTransportException = randomBoolean();
        boolean rejoinSameClusterManager = failsWithConnectTransportException && randomBoolean();
        Request request = new Request().clusterManagerNodeTimeout(TimeValue.timeValueSeconds(failsWithConnectTransportException ? 60 : 0));
        DiscoveryNode clusterManagerNode = this.remoteNode;
        setState(
            clusterService,
            // use a random base version so it can go down when simulating a restart.
            ClusterState.builder(ClusterStateCreationUtils.state(localNode, clusterManagerNode, allNodes)).version(randomIntBetween(0, 10))
        );

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        new Action("internal:testAction", transportService, clusterService, threadPool).execute(request, listener);

        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = capturedRequests[0];
        assertTrue(capturedRequest.node.isClusterManagerNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("internal:testAction"));

        if (rejoinSameClusterManager) {
            transport.handleRemoteError(
                capturedRequest.requestId,
                randomBoolean()
                    ? new ConnectTransportException(clusterManagerNode, "Fake error")
                    : new NodeClosedException(clusterManagerNode)
            );
            assertFalse(listener.isDone());
            if (randomBoolean()) {
                // simulate cluster-manager node removal
                final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterService.state().nodes());
                nodesBuilder.clusterManagerNodeId(null);
                setState(clusterService, ClusterState.builder(clusterService.state()).nodes(nodesBuilder));
            }
            if (randomBoolean()) {
                // reset the same state to increment a version simulating a join of an existing node
                // simulating use being disconnected
                final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterService.state().nodes());
                nodesBuilder.clusterManagerNodeId(clusterManagerNode.getId());
                setState(clusterService, ClusterState.builder(clusterService.state()).nodes(nodesBuilder));
            } else {
                // simulate cluster-manager restart followed by a state recovery - this will reset the cluster state version
                final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterService.state().nodes());
                nodesBuilder.remove(clusterManagerNode);
                clusterManagerNode = new DiscoveryNode(
                    clusterManagerNode.getId(),
                    clusterManagerNode.getAddress(),
                    clusterManagerNode.getVersion()
                );
                nodesBuilder.add(clusterManagerNode);
                nodesBuilder.clusterManagerNodeId(clusterManagerNode.getId());
                final ClusterState.Builder builder = ClusterState.builder(clusterService.state()).nodes(nodesBuilder);
                setState(clusterService, builder.version(0));
            }
            assertFalse(listener.isDone());
            capturedRequests = transport.getCapturedRequestsAndClear();
            assertThat(capturedRequests.length, equalTo(1));
            capturedRequest = capturedRequests[0];
            assertTrue(capturedRequest.node.isClusterManagerNode());
            assertThat(capturedRequest.request, equalTo(request));
            assertThat(capturedRequest.action, equalTo("internal:testAction"));
        } else if (failsWithConnectTransportException) {
            transport.handleRemoteError(capturedRequest.requestId, new ConnectTransportException(clusterManagerNode, "Fake error"));
            assertFalse(listener.isDone());
            setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));
            assertTrue(listener.isDone());
            listener.get();
        } else {
            OpenSearchException t = new OpenSearchException("test");
            t.addHeader("header", "is here");
            transport.handleRemoteError(capturedRequest.requestId, t);
            assertTrue(listener.isDone());
            try {
                listener.get();
                fail("Expected exception but returned proper result");
            } catch (ExecutionException ex) {
                final Throwable cause = ex.getCause().getCause();
                assertThat(cause, instanceOf(OpenSearchException.class));
                final OpenSearchException es = (OpenSearchException) cause;
                assertThat(es.getMessage(), equalTo(t.getMessage()));
                assertThat(es.getHeader("header"), equalTo(t.getHeader("header")));
            }
        }
    }

    public void testClusterManagerFailoverAfterStepDown() throws ExecutionException, InterruptedException {
        Request request = new Request().clusterManagerNodeTimeout(TimeValue.timeValueHours(1));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        final Response response = new Response();

        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));

        new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected void clusterManagerOperation(Request request, ClusterState state, ActionListener<Response> listener)
                throws Exception {
                // The other node has become cluster-manager, simulate failures of this node while publishing cluster state through
                // ZenDiscovery
                setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes));
                Exception failure = randomBoolean()
                    ? new FailedToCommitClusterStateException("Fake error")
                    : new NotClusterManagerException("Fake error");
                listener.onFailure(failure);
            }
        }.execute(request, listener);

        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[0];
        assertTrue(capturedRequest.node.isClusterManagerNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("internal:testAction"));

        transport.handleResponse(capturedRequest.requestId, response);
        assertTrue(listener.isDone());
        assertThat(listener.get(), equalTo(response));
    }

    // Validate TransportMasterNodeAction.testDelegateToClusterManager() works correctly on node with the deprecated MASTER_ROLE.
    public void testDelegateToClusterManagerOnNodeWithDeprecatedMasterRole() throws ExecutionException, InterruptedException {
        DiscoveryNode localNode = new DiscoveryNode(
            "local_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.MASTER_ROLE),
            Version.CURRENT
        );
        DiscoveryNode remoteNode = new DiscoveryNode(
            "remote_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.MASTER_ROLE),
            Version.CURRENT
        );
        DiscoveryNode[] allNodes = new DiscoveryNode[] { localNode, remoteNode };

        Request request = new Request();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes));

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        new Action("internal:testAction", transportService, clusterService, threadPool).execute(request, listener);

        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[0];
        assertTrue(capturedRequest.node.isClusterManagerNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("internal:testAction"));

        Response response = new Response();
        transport.handleResponse(capturedRequest.requestId, response);
        assertTrue(listener.isDone());
        assertThat(listener.get(), equalTo(response));
    }

    public void testThrottlingRetryLocalMaster() throws InterruptedException, BrokenBarrierException {
        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        AtomicBoolean exception = new AtomicBoolean(true);
        AtomicBoolean retried = new AtomicBoolean(false);
        CyclicBarrier barrier = new CyclicBarrier(2);
        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, new DiscoveryNode[] { localNode }));

        TransportClusterManagerNodeAction action = new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
                if (exception.getAndSet(false)) {
                    throw new ClusterManagerThrottlingException("Throttling Exception : Limit exceeded for test");
                } else {
                    try {
                        retried.set(true);
                        barrier.await();
                    } catch (Exception e) {
                        throw new AssertionError();
                    }
                }
            }
        };
        action.execute(request, listener);

        barrier.await();
        assertTrue(retried.get());
        assertFalse(exception.get());
    }

    public void testThrottlingRetryRemoteMaster() throws ExecutionException, InterruptedException {
        Request request = new Request().clusterManagerNodeTimeout(TimeValue.timeValueSeconds(60));
        DiscoveryNode masterNode = this.remoteNode;
        setState(
            clusterService,
            // use a random base version so it can go down when simulating a restart.
            ClusterState.builder(ClusterStateCreationUtils.state(localNode, masterNode, new DiscoveryNode[] { localNode, masterNode }))
                .version(randomIntBetween(0, 10))
        );

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        TransportClusterManagerNodeAction action = new Action("internal:testAction", transportService, clusterService, threadPool);
        action.execute(request, listener);

        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = capturedRequests[0];
        assertTrue(capturedRequest.node.isMasterNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("internal:testAction"));
        transport.handleRemoteError(
            capturedRequest.requestId,
            new ClusterManagerThrottlingException("Throttling Exception : Limit exceeded for test")
        );

        assertFalse(listener.isDone());

        // waiting for retry to trigger
        Thread.sleep(100);

        // Retry for above throttling exception
        capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(1));
        capturedRequest = capturedRequests[0];
        Response response = new Response();
        transport.handleResponse(capturedRequest.requestId, response);

        assertTrue(listener.isDone());
        listener.get();
    }

    public void testRetryForDifferentException() throws InterruptedException, BrokenBarrierException {
        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        AtomicBoolean exception = new AtomicBoolean(true);
        AtomicBoolean retried = new AtomicBoolean(false);
        CyclicBarrier barrier = new CyclicBarrier(2);
        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, new DiscoveryNode[] { localNode }));

        TransportClusterManagerNodeAction action = new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener)
                throws Exception {
                if (exception.getAndSet(false)) {
                    throw new Exception("Different exception");
                } else {
                    // If called second time due to retry, throw exception
                    retried.set(true);
                    throw new AssertionError("Should not retry for other exception");
                }
            }
        };
        action.execute(request, listener);

        assertFalse(retried.get());
        assertFalse(exception.get());
    }
}
