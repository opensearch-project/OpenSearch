/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.throttle;

import java.util.Map;

/**
 * Settings for a {@link AdmissionController}
 *
 * @opensearch.internal
 */
class AdmissionControlSetting {
    private final String name;
    private final boolean enabled;
    private final Map<String, Long> limits;

    /**
     * @param name of admission controller
     * @param enabled status of admission controller
     * @param limits of admission controller per endpoint (key)
     */
    public AdmissionControlSetting(String name, boolean enabled, Map<String, Long> limits) {
        this.name = name;
        this.enabled = enabled;
        this.limits = limits;
    }

    /**
     * @return name of admission controller
     */
    public String getName() {
        return name;
    }

    /**
     * @return if admission controller is enabled or not
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @return limits of admission controller
     */
    public Map<String, Long> getLimits() {
        return limits;
    }

    @Override
    public String toString() {
        return "AdmissionControlSetting{" + "name='" + name + '\'' + ", enabled=" + enabled + ", limit=" + limits + '}';
    }
}
