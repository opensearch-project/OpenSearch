/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.identity.PluginSubject;
import org.opensearch.identity.Subject;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Plugin that performs transport actions with a plugin system context. IdentityAwarePlugins are initialized
 * with a {@link Subject} that they can utilize to perform transport actions outside the default subject.
 *
 * When the Security plugin is installed, the default subject is the authenticated user. In particular,
 * SystemIndexPlugins utilize the {@link Subject} to perform transport actions that interact with system indices.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IdentityAwarePlugin {

    /**
     * Passes necessary classes for this plugin to operate as an IdentityAwarePlugin
     *
     * @param pluginSubject A subject for running transport actions in the plugin context for system index
     *                      interaction
     */
    default void assignSubject(PluginSubject pluginSubject) {}

    /**
     * Returns a set of cluster actions this plugin can perform within a pluginSubject.runAs(() -> { ... }) block.
     *
     * @return Set of cluster actions
     */
    default Set<String> getClusterActions() {
        return Collections.emptySet();
    }

    /**
     * Returns a map of index pattern -> allowed index actions this plugin can perform within a pluginSubject.runAs(() -> { ... }) block.
     *
     * @return Map of index pattern -> allowed index actions
     */
    default Map<String, Set<String>> getIndexActions() {
        return Collections.emptyMap();
    }
}
