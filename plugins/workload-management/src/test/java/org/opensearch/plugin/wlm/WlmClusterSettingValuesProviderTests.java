/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.junit.Before;

import java.util.HashSet;

import static org.mockito.Mockito.spy;

public class WlmClusterSettingValuesProviderTests extends OpenSearchTestCase {

    private ClusterSettings clusterSettings;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        try (WorkloadManagementPlugin plugin = new WorkloadManagementPlugin()) {
            clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(plugin.getSettings()));
            clusterSettings.registerSetting(WorkloadManagementSettings.WLM_MODE_SETTING);
        }
    }

    public void testEnsureWlmEnabledThrowsWhenDisabled() {
        WlmClusterSettingValuesProvider spyProvider = createSpyProviderWithMode("disabled");
        assertThrows(IllegalStateException.class, () -> spyProvider.ensureWlmEnabled(""));
    }

    public void testEnsureWlmEnabledThrowsWhenMonitorOnly() {
        WlmClusterSettingValuesProvider spyProvider = createSpyProviderWithMode("monitor_only");
        assertThrows(IllegalStateException.class, () -> spyProvider.ensureWlmEnabled(""));
    }

    public void testEnsureWlmEnabledSucceedsWhenEnabled() {
        WlmClusterSettingValuesProvider spyProvider = createSpyProviderWithMode("enabled");
        spyProvider.ensureWlmEnabled("");
    }

    private WlmClusterSettingValuesProvider createSpyProviderWithMode(String mode) {
        Settings settings = Settings.builder().put(WorkloadManagementSettings.WLM_MODE_SETTING.getKey(), mode).build();
        WlmClusterSettingValuesProvider realProvider = new WlmClusterSettingValuesProvider(settings, clusterSettings);
        return spy(realProvider);
    }
}
