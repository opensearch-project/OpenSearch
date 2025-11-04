/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

/**
 * Enum representing the load status of a plugin for hot reload functionality
 *
 * @opensearch.internal
 */
public enum PluginLoadStatus {
    /**
     * Plugin was loaded during initial startup
     */
    INITIAL("initial"),
    
    /**
     * Plugin was loaded via the load API
     */
    LOADED("loaded"),
    
    /**
     * Plugin was refreshed via the refresh API
     */
    ACTIVE("active");
    
    private final String displayName;
    
    PluginLoadStatus(String displayName) {
        this.displayName = displayName;
    }
    
    public String getDisplayName() {
        return displayName;
    }
    
    @Override
    public String toString() {
        return displayName;
    }
}
