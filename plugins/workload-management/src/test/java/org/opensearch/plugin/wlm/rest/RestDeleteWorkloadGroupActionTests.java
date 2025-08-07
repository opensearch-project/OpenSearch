/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rest;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.wlm.WlmClusterSettingValuesProvider;
import org.opensearch.plugin.wlm.WorkloadManagementTestUtils;
import org.opensearch.plugin.wlm.action.DeleteWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.DeleteWorkloadGroupRequest;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import org.mockito.ArgumentCaptor;

import static org.opensearch.plugin.wlm.WorkloadManagementTestUtils.NAME_ONE;
import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class RestDeleteWorkloadGroupActionTests extends OpenSearchTestCase {
    /**
     * Test case to validate the construction for RestDeleteWorkloadGroupAction
     */
    public void testConstruction() {
        RestDeleteWorkloadGroupAction action = new RestDeleteWorkloadGroupAction(mock(WlmClusterSettingValuesProvider.class));
        assertNotNull(action);
        assertEquals("delete_workload_group", action.getName());
        List<RestHandler.Route> routes = action.routes();
        assertEquals(1, routes.size());
        RestHandler.Route route = routes.get(0);
        assertEquals(DELETE, route.getMethod());
        assertEquals("_wlm/workload_group/{name}", route.getPath());
    }

    /**
     * Test case to validate the prepareRequest logic for RestDeleteWorkloadGroupAction
     */
    @SuppressWarnings("unchecked")
    public void testPrepareRequest() throws Exception {
        RestDeleteWorkloadGroupAction restDeleteWorkloadGroupAction = new RestDeleteWorkloadGroupAction(
            mock(WlmClusterSettingValuesProvider.class)
        );
        NodeClient nodeClient = mock(NodeClient.class);
        RestRequest realRequest = new FakeRestRequest();
        realRequest.params().put("name", NAME_ONE);
        ;
        RestRequest spyRequest = spy(realRequest);

        doReturn(TimeValue.timeValueSeconds(30)).when(spyRequest).paramAsTime(eq("cluster_manager_timeout"), any(TimeValue.class));
        doReturn(TimeValue.timeValueSeconds(60)).when(spyRequest).paramAsTime(eq("timeout"), any(TimeValue.class));

        CheckedConsumer<RestChannel, Exception> consumer = restDeleteWorkloadGroupAction.prepareRequest(spyRequest, nodeClient);
        assertNotNull(consumer);
        ArgumentCaptor<DeleteWorkloadGroupRequest> requestCaptor = ArgumentCaptor.forClass(DeleteWorkloadGroupRequest.class);
        ArgumentCaptor<RestToXContentListener<AcknowledgedResponse>> listenerCaptor = ArgumentCaptor.forClass(RestToXContentListener.class);
        doNothing().when(nodeClient).execute(eq(DeleteWorkloadGroupAction.INSTANCE), requestCaptor.capture(), listenerCaptor.capture());

        consumer.accept(mock(RestChannel.class));
        DeleteWorkloadGroupRequest capturedRequest = requestCaptor.getValue();
        assertEquals(NAME_ONE, capturedRequest.getName());
        assertEquals(TimeValue.timeValueSeconds(30), capturedRequest.clusterManagerNodeTimeout());
        assertEquals(TimeValue.timeValueSeconds(60), capturedRequest.timeout());
        verify(nodeClient).execute(
            eq(DeleteWorkloadGroupAction.INSTANCE),
            any(DeleteWorkloadGroupRequest.class),
            any(RestToXContentListener.class)
        );
    }

    public void testPrepareRequestThrowsWhenWlmModeDisabled() throws Exception {
        try {
            WlmClusterSettingValuesProvider nonPluginSettingValuesProvider = WorkloadManagementTestUtils
                .setUpNonPluginSettingValuesProvider("disabled");
            RestDeleteWorkloadGroupAction restDeleteWorkloadGroupAction = new RestDeleteWorkloadGroupAction(nonPluginSettingValuesProvider);
            restDeleteWorkloadGroupAction.prepareRequest(mock(RestRequest.class), mock(NodeClient.class));
            fail("Expected exception when WLM mode is DISABLED");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("delete"));
        }
    }
}
