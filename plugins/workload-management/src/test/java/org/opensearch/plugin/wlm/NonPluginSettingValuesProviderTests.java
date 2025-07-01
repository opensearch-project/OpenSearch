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
import org.opensearch.plugin.wlm.rule.sync.RefreshBasedSyncMechanism;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.WlmMode;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.junit.Before;

import java.util.HashSet;

public class NonPluginSettingValuesProviderTests extends OpenSearchTestCase {

    private NonPluginSettingValuesProvider provider;
    private ClusterSettings clusterSettings;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        try (WorkloadManagementPlugin plugin = new WorkloadManagementPlugin()) {
            Settings settings = Settings.builder()
                .put(RefreshBasedSyncMechanism.RULE_SYNC_REFRESH_INTERVAL_SETTING_NAME, 1000)
                .put(WorkloadManagementSettings.WLM_MODE_SETTING_NAME, "monitor_only")
                .build();
            clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(plugin.getSettings()));
            clusterSettings.registerSetting(WorkloadManagementSettings.WLM_MODE_SETTING);
            provider = new NonPluginSettingValuesProvider(settings, clusterSettings);
        }
    }

    public void testInitialWlmModeFromSettings() {
        assertEquals(provider.getWlmMode(), WlmMode.MONITOR_ONLY);
    }

    public void testSetWlmModeUpdatesValue() {
        provider.setWlmMode(WlmMode.ENABLED);
        assertEquals(provider.getWlmMode(), WlmMode.ENABLED);
    }

    public void testEnsureWlmEnabledThrowsWhenDisabled() {
        provider.setWlmMode(WlmMode.DISABLED);
        try {
            provider.ensureWlmEnabled("delete workload group");
            fail("Expected exception when WLM mode is DISABLED");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("delete workload group"));
        }
    }

    public void testEnsureWlmEnabledThrowsWhenMonitorOnly() {
        try {
            provider.ensureWlmEnabled("update workload group");
            fail("Expected exception when WLM mode is MONITOR_ONLY");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("update workload group"));
        }
    }

    public void testEnsureWlmEnabledSucceedWhenEnabled() {
        provider.setWlmMode(WlmMode.ENABLED);
        try {
            provider.ensureWlmEnabled("delete workload group");
        } catch (Exception e) {
            fail();
        }
    }
}
