/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.annotation.PublicApi;

/**
 * Enum to hold the values whether wlm is enabled or not
 */
@PublicApi(since = "2.18.0")
public enum WlmMode {
    ENABLED("enabled"),
    MONITOR_ONLY("monitor_only"),
    DISABLED("disabled");

    private final String name;
    WlmMode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static WlmMode fromName(String name) {
        for (WlmMode wlmMode: values()) {
            if (wlmMode.getName().equals(name)) {
                return wlmMode;
            }
        }
        throw new IllegalArgumentException(name + " is an invalid WlmMode");
    }
}
