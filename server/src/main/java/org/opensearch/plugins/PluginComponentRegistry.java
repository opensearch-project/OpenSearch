/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Optional;

/**
 * A registry of components created by plugins during node initialization. Plugins are
 * initialized in dependency order, so a plugin can look up components registered by its
 * declared dependencies (via {@code extendedPlugins}).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface PluginComponentRegistry {

    /**
     * Returns a component matching the given type, if one has been registered by a
     * previously-initialized plugin.
     *
     * @param type the component interface or class to look up
     * @param <T> the component type
     * @return the component, or empty if no matching component has been registered
     */
    <T> Optional<T> getComponent(Class<T> type);
}
