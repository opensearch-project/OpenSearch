/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import java.security.Principal;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ApplicationManager;
import org.opensearch.common.settings.Settings;
import org.opensearch.identity.noop.NoopIdentityPlugin;
import org.opensearch.identity.tokens.TokenManager;
import org.opensearch.plugins.IdentityPlugin;

/**
 * Identity and access control for OpenSearch
 *
 * @opensearch.experimental
 * */
public class IdentityService {
    private static final Logger log = LogManager.getLogger(IdentityService.class);

    private final Settings settings;
    private final IdentityPlugin identityPlugin;
    private final NodeClient client;

    public IdentityService(final Settings settings, final List<IdentityPlugin> identityPlugins, final NodeClient client) {
        this.settings = settings;
        this.client = client;

        if (identityPlugins.size() == 0) {
            log.debug("Identity plugins size is 0");
            identityPlugin = new NoopIdentityPlugin();
        } else if (identityPlugins.size() == 1) {
            log.debug("Identity plugins size is 1");
            identityPlugin = identityPlugins.get(0);
        } else {
            throw new OpenSearchException(
                "Multiple identity plugins are not supported, found: "
                    + identityPlugins.stream().map(Object::getClass).map(Class::getName).collect(Collectors.joining(","))
            );
        }
    }

    /**
     * runAs allows for logging the active Subject in an OpenSearch cluster.
     *
     * It takes a principal and a client consumer and sets the active principal to the requested principal.
     * After completing the action, it restores the previous principal.
     *
     * @param principal The principal to be used for completing the action
     * @param action A client consumer meant to execute the actual logic of a request
     */
    public void runAs(Principal principal, Consumer<Client> action) {

        // Save the current principal
        Principal currentPrincipal = identityPlugin.getSubject().getPrincipal();

        try {
            setPrincipal(principal);
            action.accept(client);
        } finally {
            setPrincipal(currentPrincipal);
        }
    }

    public void setPrincipal(Principal principal) {
        log.info("Operating as subject with principal: " + principal.getName());
        identityPlugin.setIdentityContext(principal); // Assume the permissions of the provided principal
    }

    /**
     * Gets the current Subject
     */
    public Subject getSubject() {
        return identityPlugin.getSubject();
    }

    /**
     * Gets the token manager
     */
    public TokenManager getTokenManager() {
        return identityPlugin.getTokenManager();
    }

    /**
     * Gets the Application Manager
     */
    public ApplicationManager getApplicationManager() {
        return identityPlugin.getApplicationManager();
    }
}
