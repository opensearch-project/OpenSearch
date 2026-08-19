/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;

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

    /**
     * Dynamic, opt-in feature setting for the CPU-only transport layer admission control flow. Disabled by
     * default to preserve backward compatibility. When this setting is enabled and the legacy transport
     * admission control ({@link #ADMISSION_CONTROL_TRANSPORT_LAYER_MODE}) is disabled, only the CPU admission
     * controller participates in admission decisions and requests are rejected (enforced) once the configured
     * CPU usage limit is breached. The IO and native-memory admission controllers do not participate in this
     * flow. When the legacy transport admission control is enabled, it takes precedence and this flow is
     * bypassed.
     */
    public static final Setting<Boolean> ADMISSION_CONTROL_TRANSPORT_CPU_ENABLED = Setting.boolSetting(
        "admission_control.transport.cpu.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile AdmissionControlMode transportLayeradmissionControlMode;

    private volatile boolean cpuTransportLayerAdmissionControlEnabled;

    /**
     * @param clusterSettings clusterSettings Instance
     * @param settings        settings instance
     */
    public AdmissionControlSettings(ClusterSettings clusterSettings, Settings settings) {
        this.transportLayeradmissionControlMode = ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE, this::setAdmissionControlTransportLayerMode);
        this.cpuTransportLayerAdmissionControlEnabled = ADMISSION_CONTROL_TRANSPORT_CPU_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            ADMISSION_CONTROL_TRANSPORT_CPU_ENABLED,
            this::setCpuTransportLayerAdmissionControlEnabled
        );
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
     * @param cpuTransportLayerAdmissionControlEnabled update the enabled state of the CPU-only transport
     *                                                 layer admission control flow
     */
    private void setCpuTransportLayerAdmissionControlEnabled(boolean cpuTransportLayerAdmissionControlEnabled) {
        this.cpuTransportLayerAdmissionControlEnabled = cpuTransportLayerAdmissionControlEnabled;
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

    /**
     *
     * @return true if the CPU-only transport layer admission control flow is enabled else false
     */
    public boolean isCpuTransportLayerAdmissionControlEnabled() {
        return this.cpuTransportLayerAdmissionControlEnabled;
    }
}
