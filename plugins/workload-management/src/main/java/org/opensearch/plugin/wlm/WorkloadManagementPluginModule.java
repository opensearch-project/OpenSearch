/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Singleton;
import org.opensearch.plugin.wlm.service.QueryGroupPersistenceService;

/**
 * Guice Module to manage WorkloadManagement related objects
 */
public class WorkloadManagementPluginModule extends AbstractModule {

    /**
     * Constructor for WorkloadManagementPluginModule
     */
    public WorkloadManagementPluginModule() {}

    @Override
    protected void configure() {
        // Bind QueryGroupPersistenceService as a singleton to ensure a single instance is used,
        // preventing multiple throttling key registrations in the constructor.
        bind(QueryGroupPersistenceService.class).in(Singleton.class);
    }
}
