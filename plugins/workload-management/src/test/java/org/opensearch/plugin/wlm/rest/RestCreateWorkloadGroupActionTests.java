/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rest;

import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.wlm.WlmMode;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.junit.Before;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestCreateWorkloadGroupActionTests extends OpenSearchTestCase {
    private WorkloadManagementSettings workloadManagementSettings;
    private RestCreateWorkloadGroupAction restAction;
    private NodeClient nodeClient;
    private RestRequest restRequest;

    @Before
    public void setup() {
        workloadManagementSettings = mock(WorkloadManagementSettings.class);
        restAction = new RestCreateWorkloadGroupAction(workloadManagementSettings);
        nodeClient = mock(NodeClient.class);
        restRequest = mock(RestRequest.class);
    }

    public void testPrepareRequestThrowsWhenWlmModeDisabled() throws IOException {
        when(workloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);
        try {
            restAction.prepareRequest(restRequest, nodeClient);
            fail("Expected OpenSearchException when WLM mode is DISABLED");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Cannot create workload group."));
        }
    }

    public void testPrepareRequestSucceedsWhenWlmModeEnabled() throws IOException {
        when(workloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        try {
            restAction.prepareRequest(restRequest, nodeClient);
        } catch (Exception e) {
            if (e.getMessage().contains("Cannot create workload group.")) {
                fail("Expected no exception when WLM mode is ENABLED");
            }
        }
    }

    public void testPrepareRequestSucceedsWhenWlmModeMonitorOnly() throws IOException {
        when(workloadManagementSettings.getWlmMode()).thenReturn(WlmMode.MONITOR_ONLY);
        try {
            restAction.prepareRequest(restRequest, nodeClient);
        } catch (Exception e) {
            if (e.getMessage().contains("Cannot create workload group.")) {
                fail("Expected no exception when WLM mode is MONITOR_ONLY");
            }
        }
    }
}
