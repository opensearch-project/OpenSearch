/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.accesscontrol.resources;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.plugins.NoOpResourcePlugin;
import org.opensearch.plugins.ResourceAccessControlPlugin;
import org.opensearch.plugins.ResourcePlugin;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Resource access control for OpenSearch
 *
 * @opensearch.experimental
 * */
public class ResourceService {
    private static final Logger log = LogManager.getLogger(ResourceService.class);

    private final ResourcePlugin resourcePlugin;

    public ResourceService(final List<ResourceAccessControlPlugin> resourcePlugins) {
        if (resourcePlugins.size() == 0) {
            log.debug("Security plugin disabled: Using NoopResourcePlugin");
            resourcePlugin = new NoOpResourcePlugin();
        } else if (resourcePlugins.size() == 1) {
            log.debug("Security plugin enabled: Using OpenSearchSecurityPlugin");
            resourcePlugin = resourcePlugins.get(0);
        } else {
            throw new OpenSearchException(
                "Multiple resource access control plugins are not supported, found: "
                    + resourcePlugins.stream().map(Object::getClass).map(Class::getName).collect(Collectors.joining(","))
            );
        }
    }

    /**
     * Gets the current ResourcePlugin to perform authorization
     */
    public ResourcePlugin getResourceAccessControlPlugin() {
        return resourcePlugin;
    }
}
