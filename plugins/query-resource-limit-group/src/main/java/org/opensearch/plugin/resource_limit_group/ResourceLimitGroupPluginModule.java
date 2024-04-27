/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.cluster.metadata.ResourceLimitGroup;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.plugin.resource_limit_group.service.Persistable;
import org.opensearch.plugin.resource_limit_group.service.ResourceLimitGroupPersistenceService;

/**
 * Guice Module to manage ResourceLimitGroup related objects
 */
public class ResourceLimitGroupPluginModule extends AbstractModule {

    /**
     * Constructor for ResourceLimitGroupPluginModule
     */
    public ResourceLimitGroupPluginModule() {}

    @Override
    protected void configure() {
        // bind(Persistable.class).to(ResourceLimitGroupPersistenceService.class).asEagerSingleton();
        bind(new TypeLiteral<Persistable<ResourceLimitGroup>>() {
        }).to(ResourceLimitGroupPersistenceService.class).asEagerSingleton();
    }
}
