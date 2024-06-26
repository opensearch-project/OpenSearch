/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query_group;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.search.query_group.cancellation.QueryGroupRequestCanceller;
import org.opensearch.search.query_group.tracker.QueryGroupResourceUsageTracker;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * Main service which will run periodically to track and cancel resource constraint violating tasks in QueryGroup
 */
public class QueryGroupService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(QueryGroupService.class);

    private final QueryGroupResourceUsageTracker requestTracker;
    private final QueryGroupRequestCanceller requestCanceller;
    private final QueryGroupPruner queryGroupPruner;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final QueryGroupServiceSettings queryGroupServiceSettings;
    private final ThreadPool threadPool;

    /**
     * Guice managed constructor
     * @param requestTrackerService
     * @param requestCanceller
     * @param queryGroupPruner
     * @param queryGroupServiceSettings
     * @param threadPool
     */
    @Inject
    public QueryGroupService(
        QueryGroupResourceUsageTracker requestTrackerService,
        QueryGroupRequestCanceller requestCanceller,
        QueryGroupPruner queryGroupPruner,
        QueryGroupServiceSettings queryGroupServiceSettings,
        ThreadPool threadPool
    ) {
        this.requestTracker = requestTrackerService;
        this.requestCanceller = requestCanceller;
        this.queryGroupPruner = queryGroupPruner;
        this.queryGroupServiceSettings = queryGroupServiceSettings;
        this.threadPool = threadPool;
    }

    /**
     * run at regular interval
     */
    private void doRun() {
        requestTracker.updateQueryGroupsResourceUsage();
        requestCanceller.cancelViolatingTasks();
        queryGroupPruner.pruneQueryGroup();
    }

    /**
     * {@link AbstractLifecycleComponent} lifecycle method
     */
    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            try {
                doRun();
            } catch (Exception e) {
                logger.debug("Exception occurred in Resource Limit Group service", e);
            }
        }, queryGroupServiceSettings.getRunIntervalMillis(), ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {}
}
