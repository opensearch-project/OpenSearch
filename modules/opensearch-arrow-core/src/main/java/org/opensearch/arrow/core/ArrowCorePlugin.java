/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.core;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;

/**
 * Provides all dependencies for the Arrow vectors APIs
 */
public class ArrowCorePlugin extends Plugin implements ExtensiblePlugin {
    /**
     * Constructor for the ArrowCorePlugin class.
     * @param settings The settings for the plugin.
     */
    public ArrowCorePlugin(Settings settings) {}

    /**
     * Loads extensions for the plugin.
     * @param loader The extension loader.
     */
    @Override
    public void loadExtensions(ExtensionLoader loader) {}
}
