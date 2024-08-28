/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.identity.PluginSubject;
import org.opensearch.identity.Subject;
import org.opensearch.identity.tokens.TokenManager;
import org.opensearch.rest.RestHandler;

import java.util.function.UnaryOperator;

import static org.opensearch.rest.RestController.PASS_THROUGH_REST_HANDLER_WRAPPER;

/**
 * Plugin that provides identity and access control for OpenSearch
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IdentityPlugin {

    /**
     * Get the current subject.
     *
     * @return Should never return null
     * */
    Subject getCurrentSubject();

    /**
     * Get the Identity Plugin's token manager implementation
     * @return Should never return null.
     */
    TokenManager getTokenManager();

    /**
     * Returns a function used to wrap each rest request before handling the request.
     * The returned {@link UnaryOperator} is called for every incoming rest request and receives
     * the original rest handler as it's input.
     */
    default UnaryOperator<RestHandler> authenticate(ThreadContext threadContext) {
        return PASS_THROUGH_REST_HANDLER_WRAPPER;
    }

    /**
     * Gets a subject corresponding to the passed plugin that can be utilized to perform transport actions
     * in the plugin system context
     *
     * @param plugin The corresponding plugin
     * @return Subject corresponding to the plugin
     */
    PluginSubject getPluginSubject(Plugin plugin);
}
