/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.querygroup.module;

import org.opensearch.common.inject.AbstractModule;
import org.opensearch.search.querygroup.tracker.QueryGroupResourceUsageTrackerService;
import org.opensearch.search.querygroup.tracker.QueryGroupUsageTracker;

/**
 * Module class for resource usage limiting  related artifacts
 */
public class QueryGroupModule extends AbstractModule {

    /**
     * Default constructor
     */
    public QueryGroupModule() {}

    @Override
    protected void configure() {
        bind(QueryGroupUsageTracker.class).to(QueryGroupResourceUsageTrackerService.class).asEagerSingleton();
    }
}
