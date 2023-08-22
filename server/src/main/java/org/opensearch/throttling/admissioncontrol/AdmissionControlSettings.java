/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.admissioncontrol;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.throttling.admissioncontrol.enums.AdmissionControlMode;

/**
 * Settings related to admission control.
 * @opensearch.internal
 */
public final class AdmissionControlSettings {

    /**
     * Default parameters for the AdmissionControlSettings
     */
    public static class Defaults {
        public static final String MODE = "disabled";
    }

    public static final String IO_BASED_ADMISSION_CONTROLLER = "global_io_usage";

    /**
     * Feature level setting to operate in shadow-mode or in enforced-mode. If enforced field is set
     * rejection will be performed, otherwise only rejection metrics will be populated.
     */
    public static final Setting<AdmissionControlMode> ADMISSION_CONTROL_TRANSPORT_LAYER_MODE = new Setting<>(
        "admission_control.transport.mode",
        Defaults.MODE,
        AdmissionControlMode::fromName,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile AdmissionControlMode transportLayeradmissionControlMode;

    /**
     * @param clusterSettings clusterSettings Instance
     * @param settings        settings instance
     */
    public AdmissionControlSettings(ClusterSettings clusterSettings, Settings settings) {
        this.transportLayeradmissionControlMode = ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE, this::setAdmissionControlTransportLayerMode);
    }

    /**
     *
     * @param admissionControlMode update the mode of admission control feature
     */
    private void setAdmissionControlTransportLayerMode(AdmissionControlMode admissionControlMode) {
        this.transportLayeradmissionControlMode = admissionControlMode;
    }

    /**
     *
     * @return return the default mode of the admissionControl
     */
    public AdmissionControlMode getAdmissionControlTransportLayerMode() {
        return this.transportLayeradmissionControlMode;
    }

    /**
     *
     * @return true based on the admission control feature is enforced else false
     */
    public Boolean isTransportLayerAdmissionControlEnforced() {
        return this.transportLayeradmissionControlMode == AdmissionControlMode.ENFORCED;
    }

    /**
     *
     * @return true based on the admission control feature is enabled else false
     */
    public Boolean isTransportLayerAdmissionControlEnabled() {
        return this.transportLayeradmissionControlMode != AdmissionControlMode.DISABLED;
    }
}
