/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.admissioncontrol.settings;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.throttling.admissioncontrol.AdmissionControlSettings;
import org.opensearch.throttling.admissioncontrol.controllers.IOBasedAdmissionController;
import org.opensearch.throttling.admissioncontrol.enums.AdmissionControlMode;

/**
 * Settings related to io based admission controller.
 * @opensearch.internal
 */
public class IOBasedAdmissionControllerSettings {

    /**
     * Default parameters for the IOBasedAdmissionControllerSettings
     */
    public static class Defaults {
        public static final String MODE = "disabled";
        public static final long IO_USAGE = 95;
    }
    private AdmissionControlMode transportLayerMode;
    private Long searchIOLimit;
    private Long indexingIOLimit;
    private Long defaultIOLimit;

    /**
     * Feature level setting to operate in shadow-mode or in enforced-mode. If enforced field is set
     * rejection will be performed, otherwise only rejection metrics will be populated.
     */
    public static final Setting<AdmissionControlMode> IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE = new Setting<>(
        "admission_control.transport.global_io_usage.mode",
        AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE,
        AdmissionControlMode::fromName,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Long> GLOBAL_IO_USAGE_AC_LIMIT_SETTING = Setting.longSetting(
        "admission_control.global_io_usage.limit",
        Defaults.IO_USAGE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Long> IO_USAGE_SEARCH_AC_LIMIT_SETTING = Setting.longSetting(
        "admission_control.global_io_usage.search.limit",
        GLOBAL_IO_USAGE_AC_LIMIT_SETTING,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Long> IO_USAGE_INDEXING_AC_LIMIT_SETTING = Setting.longSetting(
        "admission_control.global_io_usage.indexing.limit",
        GLOBAL_IO_USAGE_AC_LIMIT_SETTING,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    // currently limited to one setting will add further more settings in follow-up PR's
    public IOBasedAdmissionControllerSettings(
        ClusterSettings clusterSettings,
        Settings settings
    ) {
        this.transportLayerMode = IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(IO_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE, this::setTransportLayerMode);
        this.searchIOLimit = IO_USAGE_SEARCH_AC_LIMIT_SETTING.get(settings);
        this.indexingIOLimit = IO_USAGE_INDEXING_AC_LIMIT_SETTING.get(settings);
        this.defaultIOLimit = GLOBAL_IO_USAGE_AC_LIMIT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(IO_USAGE_INDEXING_AC_LIMIT_SETTING, this::setIndexingIOLimit);
        clusterSettings.addSettingsUpdateConsumer(IO_USAGE_SEARCH_AC_LIMIT_SETTING, this::setSearchIOLimit);
        clusterSettings.addSettingsUpdateConsumer(GLOBAL_IO_USAGE_AC_LIMIT_SETTING, this::setDefaultIOLimit);
    }

    private void setTransportLayerMode(AdmissionControlMode admissionControlMode) {
        this.transportLayerMode = admissionControlMode;
    }

    public AdmissionControlMode getTransportLayerAdmissionControllerMode() {
        return transportLayerMode;
    }

    public Long getIndexingIOLimit() {
        return indexingIOLimit;
    }

    public Long getSearchIOLimit() {
        return searchIOLimit;
    }

    public Long getDefaultIOLimit() {
        return defaultIOLimit;
    }

    public void setIndexingIOLimit(Long indexingIOLimit) {
        this.indexingIOLimit = indexingIOLimit;
    }

    public void setSearchIOLimit(Long searchIOLimit) {
        this.searchIOLimit = searchIOLimit;
    }

    public void setDefaultIOLimit(Long defaultIOLimit) {
        this.defaultIOLimit = defaultIOLimit;
        this.setSearchIOLimit(defaultIOLimit);
        this.setIndexingIOLimit(defaultIOLimit);
    }
}
