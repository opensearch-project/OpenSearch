/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rest;

import org.opensearch.plugin.wlm.NonPluginSettingValuesProvider;
import org.opensearch.plugin.wlm.WorkloadGroupTestUtils;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

import static org.mockito.Mockito.mock;

public class RestUpdateWorkloadGroupActionTests extends OpenSearchTestCase {

    public void testPrepareRequestThrowsWhenWlmModeDisabled() {
        try {
            NonPluginSettingValuesProvider nonPluginSettingValuesProvider = WorkloadGroupTestUtils.setUpNonPluginSettingValuesProvider(
                "disabled"
            );
            RestUpdateWorkloadGroupAction restUpdateWorkloadGroupAction = new RestUpdateWorkloadGroupAction(nonPluginSettingValuesProvider);
            restUpdateWorkloadGroupAction.prepareRequest(mock(RestRequest.class), mock(NodeClient.class));
            fail("Expected exception when WLM mode is DISABLED");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("update"));
        }
    }

    public void testPrepareRequestThrowsWhenWlmModeMonitorOnly() {
        try {
            NonPluginSettingValuesProvider nonPluginSettingValuesProvider = WorkloadGroupTestUtils.setUpNonPluginSettingValuesProvider(
                "monitor_only"
            );
            RestUpdateWorkloadGroupAction restUpdateWorkloadGroupAction = new RestUpdateWorkloadGroupAction(nonPluginSettingValuesProvider);
            restUpdateWorkloadGroupAction.prepareRequest(mock(RestRequest.class), mock(NodeClient.class));
            fail("Expected exception when WLM mode is MONITOR_ONLY");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("update"));
        }
    }
}
