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
 * Settings related to native memory based admission controller.
 * @opensearch.internal
 */
public class NativeMemoryBasedAdmissionControllerSettings {

    /**
     * Default parameters for the NativeMemoryBasedAdmissionControllerSettings
     */
    public static class Defaults {
        public static final long NATIVE_MEMORY_USAGE_LIMIT = 95;
        public static final long CLUSTER_ADMIN_NATIVE_MEMORY_USAGE_LIMIT = 95;
    }

    private AdmissionControlMode transportLayerMode;
    private Long searchNativeMemoryUsageLimit;
    private Long indexingNativeMemoryUsageLimit;
    private Long clusterAdminNativeMemoryUsageLimit;

    /**
     * Feature level setting to operate in shadow-mode or in enforced-mode. If enforced field is set
     * rejection will be performed, otherwise only rejection metrics will be populated.
     */
    public static final Setting<AdmissionControlMode> NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE = new Setting<>(
        "admission_control.transport.native_memory_usage.mode_override",
        AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE,
        AdmissionControlMode::fromName,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * This setting is used to set the native memory limits for search requests.
     * By default it will use the default native memory usage limit.
     */
    public static final Setting<Long> SEARCH_NATIVE_MEMORY_USAGE_LIMIT = Setting.longSetting(
        "admission_control.search.native_memory_usage.limit",
        Defaults.NATIVE_MEMORY_USAGE_LIMIT,
        0,
        100,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * This setting is used to set the native memory limits for indexing requests.
     * By default it will use the default native memory usage limit.
     */
    public static final Setting<Long> INDEXING_NATIVE_MEMORY_USAGE_LIMIT = Setting.longSetting(
        "admission_control.indexing.native_memory_usage.limit",
        Defaults.NATIVE_MEMORY_USAGE_LIMIT,
        0,
        100,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * This setting is used to set the native memory limits for cluster admin requests.
     * By default it will use the default cluster admin native memory usage limit.
     */
    public static final Setting<Long> CLUSTER_ADMIN_NATIVE_MEMORY_USAGE_LIMIT = Setting.longSetting(
        "admission_control.cluster_admin.native_memory_usage.limit",
        Defaults.CLUSTER_ADMIN_NATIVE_MEMORY_USAGE_LIMIT,
        0,
        100,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public NativeMemoryBasedAdmissionControllerSettings(ClusterSettings clusterSettings, Settings settings) {
        this.transportLayerMode = NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            NATIVE_MEMORY_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE,
            this::setTransportLayerMode
        );
        this.searchNativeMemoryUsageLimit = SEARCH_NATIVE_MEMORY_USAGE_LIMIT.get(settings);
        this.indexingNativeMemoryUsageLimit = INDEXING_NATIVE_MEMORY_USAGE_LIMIT.get(settings);
        this.clusterAdminNativeMemoryUsageLimit = CLUSTER_ADMIN_NATIVE_MEMORY_USAGE_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SEARCH_NATIVE_MEMORY_USAGE_LIMIT, this::setSearchNativeMemoryUsageLimit);
        clusterSettings.addSettingsUpdateConsumer(INDEXING_NATIVE_MEMORY_USAGE_LIMIT, this::setIndexingNativeMemoryUsageLimit);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ADMIN_NATIVE_MEMORY_USAGE_LIMIT, this::setClusterAdminNativeMemoryUsageLimit);
    }

    public void setTransportLayerMode(AdmissionControlMode transportLayerMode) {
        this.transportLayerMode = transportLayerMode;
    }

    public AdmissionControlMode getTransportLayerAdmissionControllerMode() {
        return transportLayerMode;
    }

    public Long getSearchNativeMemoryUsageLimit() {
        return searchNativeMemoryUsageLimit;
    }

    public void setSearchNativeMemoryUsageLimit(Long searchNativeMemoryUsageLimit) {
        this.searchNativeMemoryUsageLimit = searchNativeMemoryUsageLimit;
    }

    public Long getIndexingNativeMemoryUsageLimit() {
        return indexingNativeMemoryUsageLimit;
    }

    public void setIndexingNativeMemoryUsageLimit(Long indexingNativeMemoryUsageLimit) {
        this.indexingNativeMemoryUsageLimit = indexingNativeMemoryUsageLimit;
    }

    public Long getClusterAdminNativeMemoryUsageLimit() {
        return clusterAdminNativeMemoryUsageLimit;
    }

    public void setClusterAdminNativeMemoryUsageLimit(Long clusterAdminNativeMemoryUsageLimit) {
        this.clusterAdminNativeMemoryUsageLimit = clusterAdminNativeMemoryUsageLimit;
    }
}
