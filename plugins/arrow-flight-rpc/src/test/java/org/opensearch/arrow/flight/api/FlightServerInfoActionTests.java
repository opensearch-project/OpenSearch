/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.api;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.SetOnce;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.Collections;

import static org.mockito.Mockito.mock;

public class FlightServerInfoActionTests extends RestActionTestCase {
    private FlightServerInfoAction handler;

    @Before
    public void setUpAction() {
        handler = new FlightServerInfoAction();
        controller().registerHandler(handler);
    }

    public void testGetName() {
        assertEquals("flight_server_info_action", handler.getName());
    }

    public void testRoutes() {
        var routes = handler.routes();
        assertEquals(2, routes.size());
        assertTrue(
            routes.stream().anyMatch(route -> route.getPath().equals("/_flight/info") && route.getMethod() == RestRequest.Method.GET)
        );
        assertTrue(
            routes.stream()
                .anyMatch(route -> route.getPath().equals("/_flight/info/{nodeId}") && route.getMethod() == RestRequest.Method.GET)
        );
    }

    public void testFlightInfoRequest() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_flight/info")
            .build();
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier((action, actionRequest) -> {
            assertEquals(NodesFlightInfoAction.INSTANCE.name(), action.name());
            assertNotNull(actionRequest);
            executeCalled.set(true);
            return new NodesFlightInfoResponse(
                new ClusterName("test-cluster"),
                Collections.singletonList(new NodeFlightInfo(mock(DiscoveryNode.class), mock(BoundTransportAddress.class))),
                Collections.emptyList()
            );
        });
        dispatchRequest(request);
        assertEquals(Boolean.TRUE, executeCalled.get());
    }

    public void testFlightInfoRequestWithNodeId() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_flight/info/local_node")
            .build();
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier((action, actionRequest) -> {
            assertEquals(NodesFlightInfoAction.INSTANCE.name(), action.name());
            assertNotNull(actionRequest);
            executeCalled.set(true);
            return null;
        });
        dispatchRequest(request);
        assertEquals(Boolean.TRUE, executeCalled.get());
    }

    public void testFlightInfoRequestWithInvalidPath() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_flight/invalid_path")
            .build();
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier((action, actionRequest) -> {
            assertEquals(NodesFlightInfoAction.INSTANCE.name(), action.name());
            assertNotNull(actionRequest);
            executeCalled.set(true);
            return new NodesFlightInfoResponse(
                new ClusterName("test-cluster"),
                Collections.singletonList(new NodeFlightInfo(mock(DiscoveryNode.class), mock(BoundTransportAddress.class))),
                Collections.emptyList()
            );
        });
        dispatchRequest(request);
        assertNull(executeCalled.get());
    }
}
