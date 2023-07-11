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
import org.opensearch.threadpool.ThreadPool;

/**
 * Identity and access control for OpenSearch.
 *
 * @opensearch.experimental
 * */
public class IdentityService {
    private static final Logger log = LogManager.getLogger(IdentityService.class);

    private final Settings settings;
    private final IdentityPlugin identityPlugin;
    private final ThreadPool threadPool;

    public IdentityService(final Settings settings, final List<IdentityPlugin> identityPlugins, final ThreadPool threadPool) {
        this.settings = settings;
        this.threadPool = threadPool;

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

    public void runAs(Principal principal, Consumer<Client> action) {

        // Save the current principal
        Principal currentPrincipal = identityPlugin.getSubject().getPrincipal();

        identityPlugin.getSubject().setPrincipal(principal);

        try {
            Client client = createClient();
            action.accept(client);
        }
        finally {

        }



    }

    public void releaseRunAs() {

    }

    public NodeClient createClient() {
        return new NodeClient(settings, threadPool);
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
