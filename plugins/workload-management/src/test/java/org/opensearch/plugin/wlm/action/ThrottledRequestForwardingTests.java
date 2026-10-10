/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.SendRequestTransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Sends throttled create/update requests from a {@link Version#V_3_10_0} node to a pre-3.10 node over a real transport
 * connection, as when the cluster-manager has not been upgraded yet, and checks the error an operator would see.
 */
public class ThrottledRequestForwardingTests extends OpenSearchTestCase {

    private static final String ACTION = "cluster:admin/opensearch/wlm/test_forward";

    private ThreadPool threadPool;
    private MockTransportService newNode;
    private MockTransportService oldNode;

    @Before
    public void startNodes() {
        threadPool = new TestThreadPool(getTestName());
        oldNode = startNode(Version.V_3_9_0);
        newNode = startNode(Version.V_3_10_0);
        newNode.connectToNode(oldNode.getLocalNode());
    }

    @After
    public void stopNodes() {
        newNode.close();
        oldNode.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testThrottledCreateToPre310NodeIsBadRequest() throws IOException {
        WorkloadGroup group = WorkloadGroup.builder()
            .name("throttled_group")
            ._id("throttled_group_id")
            .mutableWorkloadGroupFragment(throttledFragment())
            .updatedAt(1690934400000L)
            .build();
        assertBadRequestNamingVersion(forward(new CreateWorkloadGroupRequest(group)));
    }

    public void testThrottledUpdateToPre310NodeIsBadRequest() throws IOException {
        assertBadRequestNamingVersion(forward(new UpdateWorkloadGroupRequest("throttled_group", throttledFragment())));
    }

    private MockTransportService startNode(Version version) {
        MockTransportService service = MockTransportService.createNewService(Settings.EMPTY, version, threadPool, NoopTracer.INSTANCE);
        service.registerRequestHandler(
            ACTION,
            ThreadPool.Names.SAME,
            UpdateWorkloadGroupRequest::new,
            (request, channel, task) -> fail("the request must not reach the older node")
        );
        service.start();
        service.acceptIncomingRequests();
        return service;
    }

    private Exception forward(TransportRequest request) {
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        newNode.sendRequest(
            oldNode.getLocalNode(),
            ACTION,
            request,
            new ActionListenerResponseHandler<>(future, AcknowledgedResponse::new)
        );
        ExecutionException e = expectThrows(ExecutionException.class, () -> future.get(30, TimeUnit.SECONDS));
        return (Exception) e.getCause();
    }

    private void assertBadRequestNamingVersion(Exception e) throws IOException {
        assertTrue("expected a send failure but was " + e, e instanceof SendRequestTransportException);
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(e));

        // Render the error body the way BytesRestResponse does with detailed errors (the default).
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        OpenSearchException.generateFailureXContent(builder, ToXContent.EMPTY_PARAMS, e, true);
        builder.endObject();
        Map<String, Object> body = XContentHelper.convertToMap(BytesReference.bytes(builder), false, MediaTypeRegistry.JSON).v2();
        @SuppressWarnings("unchecked")
        Map<String, Object> rootCause = ((List<Map<String, Object>>) ((Map<String, Object>) body.get("error")).get("root_cause")).get(0);
        assertEquals("illegal_argument_exception", rootCause.get("type"));
        assertEquals("cannot send workload group throttling to a node before version " + Version.V_3_10_0, rootCause.get("reason"));
    }

    private static MutableWorkloadGroupFragment throttledFragment() {
        return new MutableWorkloadGroupFragment(
            ResiliencyMode.ENFORCED,
            Map.of(ResourceType.MEMORY, 0.5),
            Settings.EMPTY,
            Settings.builder().put("node_limit", 5).build()
        );
    }
}
