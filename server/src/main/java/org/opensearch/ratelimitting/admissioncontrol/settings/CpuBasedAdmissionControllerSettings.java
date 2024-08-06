/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.settings;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlSettings;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;

/**
 * Settings related to cpu based admission controller.
 * @opensearch.internal
 */
public class CpuBasedAdmissionControllerSettings {

    /**
     * Default parameters for the CpuBasedAdmissionControllerSettings
     */
    public static class Defaults {
        public static final long CPU_USAGE_LIMIT = 95;
    }

    private AdmissionControlMode transportLayerMode;
    private Long searchCPULimit;
    private Long indexingCPULimit;
    private Long clusterInfoCPULimit;

    /**
     * Feature level setting to operate in shadow-mode or in enforced-mode. If enforced field is set
     * rejection will be performed, otherwise only rejection metrics will be populated.
     */
    public static final Setting<AdmissionControlMode> CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE = new Setting<>(
        "admission_control.transport.cpu_usage.mode_override",
        AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE,
        AdmissionControlMode::fromName,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * This setting used to set the CPU Limits for the search requests by default it will use default IO usage limit
     */
    public static final Setting<Long> SEARCH_CPU_USAGE_LIMIT = Setting.longSetting(
        "admission_control.search.cpu_usage.limit",
        Defaults.CPU_USAGE_LIMIT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * This setting used to set the CPU limits for the indexing requests by default it will use default IO usage limit
     */
    public static final Setting<Long> INDEXING_CPU_USAGE_LIMIT = Setting.longSetting(
        "admission_control.indexing.cpu_usage.limit",
        Defaults.CPU_USAGE_LIMIT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Long> CLUSTER_ADMIN_CPU_USAGE_LIMIT = Setting.longSetting(
        "admission_control.cluster.admin.cpu_usage.limit",
        Defaults.CPU_USAGE_LIMIT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    // currently limited to one setting will add further more settings in follow-up PR's
    public CpuBasedAdmissionControllerSettings(ClusterSettings clusterSettings, Settings settings) {
        this.transportLayerMode = CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE, this::setTransportLayerMode);
        this.searchCPULimit = SEARCH_CPU_USAGE_LIMIT.get(settings);
        this.indexingCPULimit = INDEXING_CPU_USAGE_LIMIT.get(settings);
        this.clusterInfoCPULimit = CLUSTER_ADMIN_CPU_USAGE_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(INDEXING_CPU_USAGE_LIMIT, this::setIndexingCPULimit);
        clusterSettings.addSettingsUpdateConsumer(SEARCH_CPU_USAGE_LIMIT, this::setSearchCPULimit);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ADMIN_CPU_USAGE_LIMIT, this::setClusterInfoCPULimit);

    }

    private void setTransportLayerMode(AdmissionControlMode admissionControlMode) {
        this.transportLayerMode = admissionControlMode;
    }

    public AdmissionControlMode getTransportLayerAdmissionControllerMode() {
        return transportLayerMode;
    }

    public Long getSearchCPULimit() {
        return searchCPULimit;
    }

    public Long getIndexingCPULimit() {
        return indexingCPULimit;
    }

    public Long getClusterAdminCPULimit() {
        return clusterInfoCPULimit;
    }

    public void setIndexingCPULimit(Long indexingCPULimit) {
        this.indexingCPULimit = indexingCPULimit;
    }

    public void setSearchCPULimit(Long searchCPULimit) {
        this.searchCPULimit = searchCPULimit;
    }

    public void setClusterInfoCPULimit(Long clusterInfoCPULimit) {
        this.clusterInfoCPULimit = clusterInfoCPULimit;
    }

}
