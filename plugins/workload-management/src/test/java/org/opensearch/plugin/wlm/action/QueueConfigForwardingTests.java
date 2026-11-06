/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Forwards create/update requests that set {@code queue} over a real transport connection to a node running the real
 * transport actions, as a node on a version with request queueing would forward them to a cluster manager on this
 * version. The receiving node refuses them before doing anything else: queueing is not available yet.
 */
public class QueueConfigForwardingTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private MockTransportService forwardingNode;
    private MockTransportService clusterManagerNode;
    private WorkloadGroupPersistenceService persistenceService;

    @Before
    public void startNodes() {
        threadPool = new TestThreadPool(getTestName());
        forwardingNode = startNode();
        clusterManagerNode = startNode();
        persistenceService = mock(WorkloadGroupPersistenceService.class);
        when(persistenceService.getClusterService()).thenReturn(mock(ClusterService.class));
        ActionFilters actionFilters = new ActionFilters(Collections.emptySet());
        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        new TransportCreateWorkloadGroupAction(threadPool, clusterManagerNode, actionFilters, resolver, persistenceService);
        new TransportUpdateWorkloadGroupAction(threadPool, clusterManagerNode, actionFilters, resolver, persistenceService);
        forwardingNode.connectToNode(clusterManagerNode.getLocalNode());
    }

    @After
    public void stopNodes() {
        forwardingNode.close();
        clusterManagerNode.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testForwardedCreateWithQueueIsRefused() throws Exception {
        WorkloadGroup group = WorkloadGroup.builder()
            .name("queued_group")
            ._id("queued_group_id")
            .mutableWorkloadGroupFragment(fragmentWithQueue(Settings.builder().put("size_per_bucket", 50).build()))
            .updatedAt(1690934400000L)
            .build();
        assertQueueingNotAvailable(
            forward(CreateWorkloadGroupAction.NAME, new CreateWorkloadGroupRequest(group), CreateWorkloadGroupResponse::new)
        );
        assertNothingPersisted();
    }

    // A forwarded update keeps the difference between an absent queue and {} or null, so an empty queue is refused too.
    public void testForwardedUpdateWithQueueIsRefused() throws Exception {
        for (Settings queue : List.of(Settings.builder().put("size_per_bucket", 7).build(), Settings.EMPTY)) {
            UpdateWorkloadGroupRequest request = new UpdateWorkloadGroupRequest("queued_group", fragmentWithQueue(queue));
            assertQueueingNotAvailable(forward(UpdateWorkloadGroupAction.NAME, request, UpdateWorkloadGroupResponse::new));
        }
        assertNothingPersisted();
    }

    private MockTransportService startNode() {
        MockTransportService service = MockTransportService.createNewService(
            Settings.EMPTY,
            Version.CURRENT,
            threadPool,
            NoopTracer.INSTANCE
        );
        service.start();
        service.acceptIncomingRequests();
        return service;
    }

    private <T extends TransportResponse> Exception forward(String action, TransportRequest request, Writeable.Reader<T> reader) {
        PlainActionFuture<T> future = new PlainActionFuture<>();
        forwardingNode.sendRequest(clusterManagerNode.getLocalNode(), action, request, new ActionListenerResponseHandler<>(future, reader));
        ExecutionException e = expectThrows(ExecutionException.class, () -> future.get(30, TimeUnit.SECONDS));
        return (Exception) e.getCause();
    }

    private static void assertQueueingNotAvailable(Exception e) {
        assertTrue("expected the cluster manager's refusal but was " + e, e instanceof RemoteTransportException);
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExceptionsHelper.unwrapCause(e)));
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        assertTrue("expected an illegal argument but was " + cause, cause instanceof IllegalArgumentException);
        assertEquals(CreateWorkloadGroupRequest.QUEUEING_NOT_AVAILABLE, cause.getMessage());
    }

    // The refusal comes before the transport action runs, so nothing reaches cluster state.
    private void assertNothingPersisted() {
        verify(persistenceService, atLeastOnce()).getClusterService();
        verifyNoMoreInteractions(persistenceService);
    }

    private static MutableWorkloadGroupFragment fragmentWithQueue(Settings queue) {
        return new MutableWorkloadGroupFragment(
            ResiliencyMode.ENFORCED,
            Map.of(ResourceType.MEMORY, 0.5),
            Settings.EMPTY,
            Settings.builder().put("node_limit", 5).build(),
            queue
        );
    }
}
