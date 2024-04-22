/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resource_limit_group.module;

import org.opensearch.common.inject.AbstractModule;
import org.opensearch.search.resource_limit_group.ResourceLimitGroupPruner;
import org.opensearch.search.resource_limit_group.cancellation.ResourceLimitGroupTaskCanceller;
import org.opensearch.search.resource_limit_group.tracker.ResourceLimitGroupResourceUsageTracker;
import org.opensearch.search.resource_limit_group.tracker.ResourceLimitsGroupResourceUsageTrackerService;

/**
 * Module class for resource usage limiting  related artifacts
 */
public class ResourceLimitGroupModule extends AbstractModule {

    /**
     * Default constructor
     */
    public ResourceLimitGroupModule() {}

    @Override
    protected void configure() {
        bind(ResourceLimitGroupResourceUsageTracker.class).to(ResourceLimitsGroupResourceUsageTrackerService.class).asEagerSingleton();
        bind(ResourceLimitGroupTaskCanceller.class).to(ResourceLimitsGroupResourceUsageTrackerService.class).asEagerSingleton();
        bind(ResourceLimitGroupPruner.class).to(ResourceLimitsGroupResourceUsageTrackerService.class).asEagerSingleton();
    }
}
