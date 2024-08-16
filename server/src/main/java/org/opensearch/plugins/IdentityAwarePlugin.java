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
     * @param pluginSystemSubject A subject for running transport actions in the plugin system context for system index
     *                      interaction
     */
    default void assignSubject(PluginSubject pluginSystemSubject) {}
}
