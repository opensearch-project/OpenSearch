/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.accesscontrol.resources;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.plugins.NoOpResourceAccessControlPlugin;
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

    private final ResourceAccessControlPlugin resourceACPlugin;
    private final List<ResourcePlugin> resourcePlugins;

    public ResourceService(final List<ResourceAccessControlPlugin> resourceACPlugins, List<ResourcePlugin> resourcePlugins) {
        this.resourcePlugins = resourcePlugins;

        if (resourceACPlugins.isEmpty()) {
            log.info("Security plugin disabled: Using NoOpResourceAccessControlPlugin");
            resourceACPlugin = new NoOpResourceAccessControlPlugin();
        } else if (resourceACPlugins.size() == 1) {
            log.info("Security plugin enabled: Using OpenSearchSecurityPlugin");
            resourceACPlugin = resourceACPlugins.get(0);
        } else {
            throw new OpenSearchException(
                "Multiple resource access control plugins are not supported, found: "
                    + resourceACPlugins.stream().map(Object::getClass).map(Class::getName).collect(Collectors.joining(","))
            );
        }
    }

    /**
     * Gets the current ResourcePlugin to perform authorization
     */
    public ResourceAccessControlPlugin getResourceAccessControlPlugin() {
        return resourceACPlugin;
    }

    /**
     * List active plugins that define resources
     */
    public List<ResourcePlugin> listResourcePlugins() {
        return resourcePlugins;
    }
}
