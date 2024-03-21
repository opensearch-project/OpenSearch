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
 * Settings related to IO based admission controller.
 * @opensearch.internal
 */
public class IoBasedAdmissionControllerSettings {

    /**
     * Default parameters for the IoBasedAdmissionControllerSettings
     */
    public static class Defaults {
        public static final long IO_USAGE_LIMIT = 95;
        public static final long CLUSTER_ADMIN_IO_USAGE_LIMIT = 100;

    }

    private AdmissionControlMode transportLayerMode;
    private Long searchIOUsageLimit;
    private Long indexingIOUsageLimit;
    private Long clusterAdminIOUsageLimit;

    /**
     * Feature level setting to operate in shadow-mode or in enforced-mode. If enforced field is set
     * rejection will be performed, otherwise only rejection metrics will be populated.
     */
    public static final Setting<AdmissionControlMode> IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE = new Setting<>(
        "admission_control.transport.io_usage.mode_override",
        AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE,
        AdmissionControlMode::fromName,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * This setting used to set the IO Limits for the search requests by default it will use default IO usage limit
     */
    public static final Setting<Long> SEARCH_IO_USAGE_LIMIT = Setting.longSetting(
        "admission_control.search.io_usage.limit",
        Defaults.IO_USAGE_LIMIT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * This setting used to set the IO limits for the indexing requests by default it will use default IO usage limit
     */
    public static final Setting<Long> INDEXING_IO_USAGE_LIMIT = Setting.longSetting(
        "admission_control.indexing.io_usage.limit",
        Defaults.IO_USAGE_LIMIT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * This setting used to set the limits for cluster admin requests by default it will use default cluster_admin IO usage limit
     */
    public static final Setting<Long> CLUSTER_ADMIN_IO_USAGE_LIMIT = Setting.longSetting(
        "admission_control.cluster_admin.io_usage.limit",
        Defaults.CLUSTER_ADMIN_IO_USAGE_LIMIT,
        Setting.Property.Final,
        Setting.Property.NodeScope
    );

    public IoBasedAdmissionControllerSettings(ClusterSettings clusterSettings, Settings settings) {
        this.transportLayerMode = IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE, this::setTransportLayerMode);
        this.searchIOUsageLimit = SEARCH_IO_USAGE_LIMIT.get(settings);
        this.indexingIOUsageLimit = INDEXING_IO_USAGE_LIMIT.get(settings);
        this.clusterAdminIOUsageLimit = CLUSTER_ADMIN_IO_USAGE_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(INDEXING_IO_USAGE_LIMIT, this::setIndexingIOUsageLimit);
        clusterSettings.addSettingsUpdateConsumer(SEARCH_IO_USAGE_LIMIT, this::setSearchIOUsageLimit);
    }

    public void setIndexingIOUsageLimit(Long indexingIOUsageLimit) {
        this.indexingIOUsageLimit = indexingIOUsageLimit;
    }

    public void setSearchIOUsageLimit(Long searchIOUsageLimit) {
        this.searchIOUsageLimit = searchIOUsageLimit;
    }

    public AdmissionControlMode getTransportLayerAdmissionControllerMode() {
        return transportLayerMode;
    }

    public void setTransportLayerMode(AdmissionControlMode transportLayerMode) {
        this.transportLayerMode = transportLayerMode;
    }

    public Long getIndexingIOUsageLimit() {
        return indexingIOUsageLimit;
    }

    public Long getSearchIOUsageLimit() {
        return searchIOUsageLimit;
    }

    public Long getClusterAdminIOUsageLimit() {
        return clusterAdminIOUsageLimit;
    }
}
