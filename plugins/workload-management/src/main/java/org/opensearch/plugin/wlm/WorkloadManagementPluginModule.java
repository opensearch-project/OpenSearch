/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.common.inject.AbstractModule;

/**
 * Guice Module to manage WorkloadManagement related objects
 */
public class WorkloadManagementPluginModule extends AbstractModule {

    /**
     * Constructor for WorkloadManagementPluginModule
     */
    public WorkloadManagementPluginModule() {}

    @Override
    protected void configure() {}
}
