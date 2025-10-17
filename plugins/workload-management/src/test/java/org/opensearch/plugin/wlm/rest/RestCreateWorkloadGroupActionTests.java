/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rest;

import org.opensearch.plugin.wlm.WlmClusterSettingValuesProvider;
import org.opensearch.plugin.wlm.WorkloadManagementTestUtils;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

import static org.mockito.Mockito.mock;

public class RestCreateWorkloadGroupActionTests extends OpenSearchTestCase {

    public void testPrepareRequestThrowsWhenWlmModeDisabled() {
        try {
            WlmClusterSettingValuesProvider nonPluginSettingValuesProvider = WorkloadManagementTestUtils
                .setUpNonPluginSettingValuesProvider("disabled");
            RestCreateWorkloadGroupAction restCreateWorkloadGroupAction = new RestCreateWorkloadGroupAction(nonPluginSettingValuesProvider);
            restCreateWorkloadGroupAction.prepareRequest(mock(RestRequest.class), mock(NodeClient.class));
            fail("Expected exception when WLM mode is DISABLED");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("create"));
        }
    }

    public void testPrepareRequestThrowsWhenWlmModeMonitorOnly() {
        try {
            WlmClusterSettingValuesProvider nonPluginSettingValuesProvider = WorkloadManagementTestUtils
                .setUpNonPluginSettingValuesProvider("monitor_only");
            RestCreateWorkloadGroupAction restCreateWorkloadGroupAction = new RestCreateWorkloadGroupAction(nonPluginSettingValuesProvider);
            restCreateWorkloadGroupAction.prepareRequest(mock(RestRequest.class), mock(NodeClient.class));
            fail("Expected exception when WLM mode is MONITOR_ONLY");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("create"));
        }
    }
}
