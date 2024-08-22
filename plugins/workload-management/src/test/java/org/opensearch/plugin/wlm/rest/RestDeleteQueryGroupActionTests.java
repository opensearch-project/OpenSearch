/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rest;

import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.wlm.action.DeleteQueryGroupAction;
import org.opensearch.plugin.wlm.action.DeleteQueryGroupRequest;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.List;

import org.mockito.ArgumentCaptor;

import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_ONE;
import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class RestDeleteQueryGroupActionTests extends OpenSearchTestCase {
    /**
     * Test case to validate the construction for RestDeleteQueryGroupAction
     */
    public void testConstruction() {
        RestDeleteQueryGroupAction action = new RestDeleteQueryGroupAction();
        assertNotNull(action);
        assertEquals("delete_query_group", action.getName());
        List<RestHandler.Route> routes = action.routes();
        assertEquals(1, routes.size());
        RestHandler.Route route = routes.get(0);
        assertEquals(DELETE, route.getMethod());
        assertEquals("_wlm/query_group/{name}", route.getPath());
    }

    /**
     * Test case to validate the prepareRequest logic for RestDeleteQueryGroupAction
     */
    @SuppressWarnings("unchecked")
    public void testPrepareRequest() throws Exception {
        RestDeleteQueryGroupAction restDeleteQueryGroupAction = new RestDeleteQueryGroupAction();
        NodeClient nodeClient = mock(NodeClient.class);
        RestRequest realRequest = new FakeRestRequest();
        realRequest.params().put("name", NAME_ONE);
        ;
        RestRequest spyRequest = spy(realRequest);

        doReturn(TimeValue.timeValueSeconds(30)).when(spyRequest).paramAsTime(eq("cluster_manager_timeout"), any(TimeValue.class));
        doReturn(TimeValue.timeValueSeconds(60)).when(spyRequest).paramAsTime(eq("timeout"), any(TimeValue.class));

        CheckedConsumer<RestChannel, Exception> consumer = restDeleteQueryGroupAction.prepareRequest(spyRequest, nodeClient);
        assertNotNull(consumer);
        ArgumentCaptor<DeleteQueryGroupRequest> requestCaptor = ArgumentCaptor.forClass(DeleteQueryGroupRequest.class);
        ArgumentCaptor<RestToXContentListener<AcknowledgedResponse>> listenerCaptor = ArgumentCaptor.forClass(RestToXContentListener.class);
        doNothing().when(nodeClient).execute(eq(DeleteQueryGroupAction.INSTANCE), requestCaptor.capture(), listenerCaptor.capture());

        consumer.accept(mock(RestChannel.class));
        DeleteQueryGroupRequest capturedRequest = requestCaptor.getValue();
        assertEquals(NAME_ONE, capturedRequest.getName());
        assertEquals(TimeValue.timeValueSeconds(30), capturedRequest.clusterManagerNodeTimeout());
        assertEquals(TimeValue.timeValueSeconds(60), capturedRequest.timeout());
        verify(nodeClient).execute(
            eq(DeleteQueryGroupAction.INSTANCE),
            any(DeleteQueryGroupRequest.class),
            any(RestToXContentListener.class)
        );
    }
}
