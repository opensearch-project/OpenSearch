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
import org.opensearch.wlm.WlmMode;
import org.opensearch.wlm.WorkloadManagementSettings;

/**
 * Central provider for maintaining and supplying the current values of wlm cluster settings.
 * This class listens for updates to relevant settings and provides the latest setting values.
 */
public class WlmClusterSettingValuesProvider {

    private volatile WlmMode wlmMode;

    /**
     * Constructor for WlmClusterSettingValuesProvider
     * @param settings OpenSearch settings
     * @param clusterSettings Cluster settings to register update listener
     */
    public WlmClusterSettingValuesProvider(Settings settings, ClusterSettings clusterSettings) {
        this.wlmMode = WorkloadManagementSettings.WLM_MODE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(WorkloadManagementSettings.WLM_MODE_SETTING, this::setWlmMode);
    }

    /**
     * Check if WLM mode is ENABLED
     * Throws an IllegalStateException if WLM mode is DISABLED or MONITOR ONLY.
     * @param operationDescription A short text describing the operation, e.g. "create workload group".
     */
    public void ensureWlmEnabled(String operationDescription) {
        if (wlmMode != WlmMode.ENABLED) {
            throw new IllegalStateException(
                "Cannot "
                    + operationDescription
                    + " because workload management mode is disabled or monitor_only."
                    + "To enable this feature, set [wlm.workload_group.mode] to 'enabled' in cluster settings."
            );
        }
    }

    /**
     * Set the latest WLM mode.
     * @param mode The wlm mode to set
     */
    private void setWlmMode(WlmMode mode) {
        this.wlmMode = mode;
    }

    /**
     * Get the latest WLM mode.
     */
    public WlmMode getWlmMode() {
        return wlmMode;
    }
}
