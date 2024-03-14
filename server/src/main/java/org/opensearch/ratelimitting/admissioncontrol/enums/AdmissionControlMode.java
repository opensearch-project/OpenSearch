/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.enums;

import java.util.Locale;

/**
 * Defines the AdmissionControlMode
 */
public enum AdmissionControlMode {
    /**
     * AdmissionController is completely disabled.
     */
    DISABLED("disabled"),

    /**
     * AdmissionController only monitors the rejection criteria for the requests.
     */
    MONITOR("monitor_only"),

    /**
     * AdmissionController monitors and rejects tasks that exceed resource usage thresholds.
     */
    ENFORCED("enforced");

    private final String mode;

    /**
     * @param mode update mode of the admission controller
     */
    AdmissionControlMode(String mode) {
        this.mode = mode;
    }

    /**
     *
     * @return mode of the admission controller
     */
    public String getMode() {
        return this.mode;
    }

    /**
     *
     * @param name is the mode of the current
     * @return Enum of AdmissionControlMode based on the mode
     */
    public static AdmissionControlMode fromName(String name) {
        switch (name.toLowerCase(Locale.ROOT)) {
            case "disabled":
                return DISABLED;
            case "monitor_only":
                return MONITOR;
            case "enforced":
                return ENFORCED;
            default:
                throw new IllegalArgumentException("Invalid AdmissionControlMode: " + name);
        }
    }
}
