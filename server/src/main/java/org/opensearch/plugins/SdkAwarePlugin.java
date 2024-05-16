/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.plugins.sdk.PluginMetadataClient;

/**
 * SdkAwarePlugin is an additional extension point for {@link Plugin}'s for receiving
 * OpenSearch abstractions.
 */
public interface SdkAwarePlugin {

    void setupSdkPlugin(Dependencies dependencies);

    /**
     * Dependencies is a simple container class holding plugin sdk dependencies.
     */
    public static interface Dependencies {
        PluginMetadataClient getPluginMetadataClient();
    }
}
