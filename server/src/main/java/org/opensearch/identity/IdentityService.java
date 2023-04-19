/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.identity.noop.NoopIdentityPlugin;
import java.util.List;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.IdentityPlugin;
import java.util.stream.Collectors;

/**
 * Identity and access control for OpenSearch.
 *
 * @opensearch.experimental
 * */
public class IdentityService {
    private static final Logger logger = LogManager.getLogger(IdentityService.class);

    private final Settings settings;
    private final IdentityPlugin identityPlugin;

    public IdentityService(final Settings settings, final List<IdentityPlugin> identityPlugins) {
        this.settings = settings;

        if (identityPlugins.size() == 0) {
            identityPlugin = new NoopIdentityPlugin();
        } else if (identityPlugins.size() == 1) {
            identityPlugin = identityPlugins.get(0);
        } else {
            throw new OpenSearchException(
                "Multiple identity plugins are not supported, found: "
                    + identityPlugins.stream().map(Object::getClass).map(Class::getName).collect(Collectors.joining(","))
            );
        }

        logger.info("Identity module loaded with " + identityPlugin.getClass().getName());
        logger.info("Current subject " + getSubject());
    }

    /**
     * Gets the current subject
     */
    public Subject getSubject() {
        return identityPlugin.getSubject();
    }
}
