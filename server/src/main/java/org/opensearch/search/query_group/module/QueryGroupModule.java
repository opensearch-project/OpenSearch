/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query_group.module;

import org.opensearch.common.inject.AbstractModule;
import org.opensearch.search.query_group.QueryGroupPruner;
import org.opensearch.search.query_group.cancellation.QueryGroupRequestCanceller;
import org.opensearch.search.query_group.tracker.QueryGroupResourceUsageTracker;
import org.opensearch.search.query_group.tracker.QueryGroupResourceUsageTrackerService;

/**
 * Module class for resource usage limiting related artifacts
 */
public class QueryGroupModule extends AbstractModule {

    /**
     * Default constructor
     */
    public QueryGroupModule() {}

    @Override
    protected void configure() {
        bind(QueryGroupResourceUsageTracker.class).to(QueryGroupResourceUsageTrackerService.class).asEagerSingleton();
        bind(QueryGroupRequestCanceller.class).to(QueryGroupResourceUsageTrackerService.class).asEagerSingleton();
        bind(QueryGroupPruner.class).to(QueryGroupResourceUsageTrackerService.class).asEagerSingleton();
    }
}
