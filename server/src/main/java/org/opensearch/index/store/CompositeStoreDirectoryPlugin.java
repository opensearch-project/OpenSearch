/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;

import java.util.Collections;
import java.util.Map;

/**
 * Example plugin that demonstrates CompositeStoreDirectoryFactory registration.
 * This plugin provides the default composite store directory factory implementation
 * and can be extended by other plugins to provide custom composite directory implementations.
 * 
 * This plugin serves as both:
 * 1. A reference implementation for other plugins
 * 2. A default provider of CompositeStoreDirectoryFactory
 * 
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeStoreDirectoryPlugin extends Plugin implements IndexStorePlugin {

    /**
     * Provides CompositeStoreDirectoryFactory mappings for this plugin.
     * This demonstrates how plugins can register their own composite store directory factories
     * following the established IndexStorePlugin pattern.
     * 
     * @return a map from factory type to composite store directory factory
     */
    @Override
    public Map<String, CompositeStoreDirectoryFactory> getCompositeStoreDirectoryFactories() {
        // Register the default implementation with "default" key
        // Other plugins can register with different keys like "custom", "performance", etc.
        return Collections.singletonMap("default", new DefaultCompositeStoreDirectoryFactory());
    }

    // Plugin metadata is handled by the Plugin base class
    // No need to override name() or description() methods
}
