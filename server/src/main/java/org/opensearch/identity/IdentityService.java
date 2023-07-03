/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.cluster.ApplicationManager;
import org.opensearch.common.settings.Settings;
import org.opensearch.identity.noop.NoopIdentityPlugin;
import org.opensearch.identity.tokens.TokenManager;
import org.opensearch.plugins.IdentityPlugin;

/**
 * Identity and access control for OpenSearch.
 *
 * @opensearch.experimental
 * */
public class IdentityService {
    private static final Logger log = LogManager.getLogger(IdentityService.class);

    private final Settings settings;
    private final IdentityPlugin identityPlugin;
    private final ApplicationManager applicationManager;

    private static IdentityService instance = null;

    public IdentityService(ApplicationManager applicationManager, final Settings settings, final List<IdentityPlugin> identityPlugins) {
        this.settings = settings;
        this.applicationManager = applicationManager;

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
        IdentityService.instance = this;
    }

    /**
     * Gets the current subject
     */
    public ApplicationAwareSubject getSubject() {
        return new ApplicationAwareSubject(identityPlugin.getSubject());
    }

    /**
     * Gets the token manager
     */
    public TokenManager getTokenManager() {
        return identityPlugin.getTokenManager();
    }

    public static IdentityService getInstance() {
        if (instance == null) {
            new IdentityService(new ApplicationManager(), Settings.EMPTY, List.of());
        }
        return instance;
    }
}
