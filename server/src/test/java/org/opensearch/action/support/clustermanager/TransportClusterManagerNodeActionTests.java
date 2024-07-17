/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.support.clustermanager;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.EmptyClusterInfoService;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.opensearch.cluster.service.ClusterManagerThrottlingException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.discovery.ClusterManagerNotDiscoveredException;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.NodeClosedException;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.gateway.TestGatewayAllocator;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.index.remote.RemoteMigrationIndexMetadataUpdaterTests.createIndexMetadataWithDocrepSettings;
import static org.opensearch.index.remote.RemoteMigrationIndexMetadataUpdaterTests.createIndexMetadataWithRemoteStoreSettings;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;
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
            Collections.emptySet(),
            NoopTracer.INSTANCE
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
        Thread.sleep(10000);

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

    public void testDontAllowSwitchingToStrictCompatibilityModeForMixedCluster() {
        // request to change cluster compatibility mode to STRICT
        Settings currentCompatibilityModeSettings = Settings.builder()
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.MIXED)
            .build();
        Settings intendedCompatibilityModeSettings = Settings.builder()
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.STRICT)
            .build();
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(intendedCompatibilityModeSettings);

        // mixed cluster (containing both remote and non-remote nodes)
        DiscoveryNode nonRemoteNode1 = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode remoteNode1 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            getRemoteStoreNodeAttributes(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode1)
            .localNodeId(nonRemoteNode1.getId())
            .add(remoteNode1)
            .localNodeId(remoteNode1.getId())
            .build();

        Metadata metadata = Metadata.builder().persistentSettings(currentCompatibilityModeSettings).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).nodes(discoveryNodes).build();
        AllocationService allocationService = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
        TransportClusterUpdateSettingsAction transportClusterUpdateSettingsAction = new TransportClusterUpdateSettingsAction(
            transportService,
            clusterService,
            threadPool,
            allocationService,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            clusterService.getClusterSettings()
        );

        final SettingsException exception = expectThrows(
            SettingsException.class,
            () -> transportClusterUpdateSettingsAction.validateCompatibilityModeSettingRequest(request, clusterState)
        );
        assertEquals(
            "can not switch to STRICT compatibility mode when the cluster contains both remote and non-remote nodes",
            exception.getMessage()
        );

        DiscoveryNode nonRemoteNode2 = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode remoteNode2 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            getRemoteStoreNodeAttributes(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        // cluster with only non-remote nodes
        discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteNode1)
            .localNodeId(nonRemoteNode1.getId())
            .add(nonRemoteNode2)
            .localNodeId(nonRemoteNode2.getId())
            .build();

        metadata = createIndexMetadataWithRemoteStoreSettings("test-index");
        ClusterState sameTypeClusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).metadata(metadata).build();
        transportClusterUpdateSettingsAction.validateCompatibilityModeSettingRequest(request, sameTypeClusterState);

        // cluster with only non-remote nodes
        discoveryNodes = DiscoveryNodes.builder()
            .add(remoteNode1)
            .localNodeId(remoteNode1.getId())
            .add(remoteNode2)
            .localNodeId(remoteNode2.getId())
            .build();
        sameTypeClusterState = ClusterState.builder(sameTypeClusterState).nodes(discoveryNodes).metadata(metadata).build();
        transportClusterUpdateSettingsAction.validateCompatibilityModeSettingRequest(request, sameTypeClusterState);
    }

    public void testDontAllowSwitchingCompatibilityModeForClusterWithMultipleVersions() {
        // request to change cluster compatibility mode
        boolean toStrictMode = randomBoolean();
        Settings currentCompatibilityModeSettings = Settings.builder()
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.MIXED)
            .build();
        Settings intendedCompatibilityModeSettings = Settings.builder()
            .put(
                REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(),
                toStrictMode ? RemoteStoreNodeService.CompatibilityMode.STRICT : RemoteStoreNodeService.CompatibilityMode.MIXED
            )
            .build();
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(intendedCompatibilityModeSettings);

        // two different but compatible open search versions for the discovery nodes
        final Version version1 = Version.V_2_13_0;
        final Version version2 = Version.V_2_13_1;

        DiscoveryNode discoveryNode1 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            toStrictMode ? getRemoteStoreNodeAttributes() : Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            version1
        );
        DiscoveryNode discoveryNode2 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            toStrictMode ? getRemoteStoreNodeAttributes() : Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            version2 // not same as discoveryNode1
        );

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(discoveryNode1)
            .localNodeId(discoveryNode1.getId())
            .add(discoveryNode2)
            .localNodeId(discoveryNode2.getId())
            .build();

        Metadata metadata = Metadata.builder().persistentSettings(currentCompatibilityModeSettings).build();

        ClusterState differentVersionClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(discoveryNodes)
            .build();
        AllocationService allocationService = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
        TransportClusterUpdateSettingsAction transportClusterUpdateSettingsAction = new TransportClusterUpdateSettingsAction(
            transportService,
            clusterService,
            threadPool,
            allocationService,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            clusterService.getClusterSettings()
        );

        // changing compatibility mode when all nodes are not of the same version
        final SettingsException exception = expectThrows(
            SettingsException.class,
            () -> transportClusterUpdateSettingsAction.validateCompatibilityModeSettingRequest(request, differentVersionClusterState)
        );
        assertThat(
            exception.getMessage(),
            containsString("can not change the compatibility mode when all the nodes in cluster are not of the same version")
        );

        // changing compatibility mode when all nodes are of the same version
        discoveryNode2 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            toStrictMode ? getRemoteStoreNodeAttributes() : Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            version1 // same as discoveryNode1
        );
        discoveryNodes = DiscoveryNodes.builder()
            .add(discoveryNode1)
            .localNodeId(discoveryNode1.getId())
            .add(discoveryNode2)
            .localNodeId(discoveryNode2.getId())
            .build();

        ClusterState sameVersionClusterState = ClusterState.builder(differentVersionClusterState)
            .nodes(discoveryNodes)
            .metadata(createIndexMetadataWithRemoteStoreSettings("test"))
            .build();
        transportClusterUpdateSettingsAction.validateCompatibilityModeSettingRequest(request, sameVersionClusterState);
    }

    public void testIsSwitchToStrictCompatibilityMode() {
        Settings mockSettings = Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "strict").build();
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(mockSettings);
        AllocationService allocationService = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
        TransportClusterUpdateSettingsAction transportClusterUpdateSettingsAction = new TransportClusterUpdateSettingsAction(
            transportService,
            clusterService,
            threadPool,
            allocationService,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            clusterService.getClusterSettings()
        );
        assertTrue(transportClusterUpdateSettingsAction.isSwitchToStrictCompatibilityMode(request));

        mockSettings = Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed").build();
        request.persistentSettings(mockSettings);
        assertFalse(transportClusterUpdateSettingsAction.isSwitchToStrictCompatibilityMode(request));
    }

    public void testFinalizeMigrationWithAllRemoteNodes() {
        AllocationService allocationService = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
        TransportClusterUpdateSettingsAction transportClusterUpdateSettingsAction = new TransportClusterUpdateSettingsAction(
            transportService,
            clusterService,
            threadPool,
            allocationService,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            clusterService.getClusterSettings()
        );

        String migratedIndex = "migrated-index";
        Settings mockSettings = Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "strict").build();
        DiscoveryNode remoteNode1 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            getRemoteStoreNodeAttributes(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        DiscoveryNode remoteNode2 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            getRemoteStoreNodeAttributes(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(remoteNode1)
            .localNodeId(remoteNode1.getId())
            .add(remoteNode2)
            .localNodeId(remoteNode2.getId())
            .build();
        Metadata docrepIdxMetadata = createIndexMetadataWithDocrepSettings(migratedIndex);
        assertDocrepSettingsApplied(docrepIdxMetadata.index(migratedIndex));
        Metadata remoteIndexMd = Metadata.builder(docrepIdxMetadata).persistentSettings(mockSettings).build();
        ClusterState clusterStateWithDocrepIndexSettings = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(remoteIndexMd)
            .nodes(discoveryNodes)
            .routingTable(createRoutingTableAllShardsStarted(migratedIndex, 1, 1, remoteNode1, remoteNode2))
            .build();
        Metadata mutatedMetadata = transportClusterUpdateSettingsAction.finalizeMigration(clusterStateWithDocrepIndexSettings).metadata();
        assertTrue(mutatedMetadata.index(migratedIndex).getVersion() > docrepIdxMetadata.index(migratedIndex).getVersion());
        assertRemoteSettingsApplied(mutatedMetadata.index(migratedIndex));
    }

    public void testFinalizeMigrationWithAllDocrepNodes() {
        AllocationService allocationService = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
        TransportClusterUpdateSettingsAction transportClusterUpdateSettingsAction = new TransportClusterUpdateSettingsAction(
            transportService,
            clusterService,
            threadPool,
            allocationService,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            clusterService.getClusterSettings()
        );

        String docrepIndex = "docrep-index";
        Settings mockSettings = Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "strict").build();
        DiscoveryNode docrepNode1 = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode docrepNode2 = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(docrepNode1)
            .localNodeId(docrepNode1.getId())
            .add(docrepNode2)
            .localNodeId(docrepNode2.getId())
            .build();
        Metadata docrepIdxMetadata = createIndexMetadataWithDocrepSettings(docrepIndex);
        assertDocrepSettingsApplied(docrepIdxMetadata.index(docrepIndex));
        Metadata remoteIndexMd = Metadata.builder(docrepIdxMetadata).persistentSettings(mockSettings).build();
        ClusterState clusterStateWithDocrepIndexSettings = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(remoteIndexMd)
            .nodes(discoveryNodes)
            .routingTable(createRoutingTableAllShardsStarted(docrepIndex, 1, 1, docrepNode1, docrepNode2))
            .build();
        Metadata mutatedMetadata = transportClusterUpdateSettingsAction.finalizeMigration(clusterStateWithDocrepIndexSettings).metadata();
        assertEquals(docrepIdxMetadata.index(docrepIndex).getVersion(), mutatedMetadata.index(docrepIndex).getVersion());
        assertDocrepSettingsApplied(mutatedMetadata.index(docrepIndex));
    }

    private Map<String, String> getRemoteStoreNodeAttributes() {
        Map<String, String> remoteStoreNodeAttributes = new HashMap<>();
        remoteStoreNodeAttributes.put(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, "my-segment-repo-1");
        remoteStoreNodeAttributes.put(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, "my-translog-repo-1");
        return remoteStoreNodeAttributes;
    }

    private void assertRemoteSettingsApplied(IndexMetadata indexMetadata) {
        assertTrue(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexMetadata.getSettings()));
        assertTrue(IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.exists(indexMetadata.getSettings()));
        assertTrue(IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.exists(indexMetadata.getSettings()));
        assertEquals(ReplicationType.SEGMENT, IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(indexMetadata.getSettings()));
    }

    private void assertDocrepSettingsApplied(IndexMetadata indexMetadata) {
        assertFalse(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexMetadata.getSettings()));
        assertFalse(IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.exists(indexMetadata.getSettings()));
        assertFalse(IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.exists(indexMetadata.getSettings()));
        assertEquals(ReplicationType.DOCUMENT, IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(indexMetadata.getSettings()));
    }

    private RoutingTable createRoutingTableAllShardsStarted(
        String indexName,
        int numberOfShards,
        int numberOfReplicas,
        DiscoveryNode primaryHostingNode,
        DiscoveryNode replicaHostingNode
    ) {
        RoutingTable.Builder builder = RoutingTable.builder();
        Index index = new Index(indexName, UUID.randomUUID().toString());

        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (int i = 0; i < numberOfShards; i++) {
            ShardId shardId = new ShardId(index, i);
            IndexShardRoutingTable.Builder indexShardRoutingTable = new IndexShardRoutingTable.Builder(shardId);
            indexShardRoutingTable.addShard(
                TestShardRouting.newShardRouting(shardId, primaryHostingNode.getId(), true, ShardRoutingState.STARTED)
            );
            for (int j = 0; j < numberOfReplicas; j++) {
                indexShardRoutingTable.addShard(
                    TestShardRouting.newShardRouting(shardId, replicaHostingNode.getId(), false, ShardRoutingState.STARTED)
                );
            }
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingTable.build());
        }
        return builder.add(indexRoutingTableBuilder.build()).build();
    }
}
