/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.accesscontrol.resources.fallback.DefaultResourceAccessControlPlugin;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.plugins.ResourceAccessControlPlugin;
import org.opensearch.plugins.ResourcePlugin;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Service to get the current ResourcePlugin to perform authorization.
 *
 * @opensearch.experimental
 */
public class ResourceService {
    private static final Logger log = LogManager.getLogger(ResourceService.class);

    private final ResourceAccessControlPlugin resourceACPlugin;
    private final List<ResourcePlugin> resourcePlugins;

    @Inject
    public ResourceService(
        final List<ResourceAccessControlPlugin> resourceACPlugins,
        List<ResourcePlugin> resourcePlugins,
        Client client,
        ThreadPool threadPool
    ) {
        this.resourcePlugins = resourcePlugins;

        if (resourceACPlugins.isEmpty()) {
            log.info("Security plugin disabled: Using DefaultResourceAccessControlPlugin");
            resourceACPlugin = new DefaultResourceAccessControlPlugin(client, threadPool);
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
     * Gets the ResourceAccessControlPlugin in-effect to perform authorization
     */
    public ResourceAccessControlPlugin getResourceAccessControlPlugin() {
        return resourceACPlugin;
    }

    /**
     * Gets the list of ResourcePlugins
     */
    public List<ResourcePlugin> listResourcePlugins() {
        return List.copyOf(resourcePlugins);
    }
}
