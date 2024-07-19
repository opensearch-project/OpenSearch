/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.querygroup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.querygroup.tracker.QueryGroupUsageTracker;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Main service which will run periodically to track and cancel resource constraint violating tasks in QueryGroups
 */
public class QueryGroupService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(QueryGroupService.class);

    private final QueryGroupUsageTracker queryGroupUsageTracker;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    /**
     * Guice managed constructor
     *
     * @param queryGroupUsageTracker tracker service
     * @param threadPool threadPool this will be used to schedule the service
     */
    @Inject
    public QueryGroupService(
        QueryGroupUsageTracker queryGroupUsageTracker,
        // QueryGroupServiceSettings queryGroupServiceSettings,
        // QueryGroupPruner queryGroupPruner,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        this.queryGroupUsageTracker = queryGroupUsageTracker;
        // this.queryGroupServiceSettings = queryGroupServiceSettings;
        // this.queryGroupPruner = queryGroupPruner;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * run at regular interval
     */
    private void doRun() {
        Map<String, QueryGroupLevelResourceUsageView> queryGroupLevelResourceUsageViews = queryGroupUsageTracker
            .constructQueryGroupLevelUsageViews();
        Set<QueryGroup> activeQueryGroups = getActiveQueryGroups();
    }

    private Set<QueryGroup> getActiveQueryGroups() {
        return new HashSet<>(clusterService.state().metadata().queryGroups().values());
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
                logger.debug("Exception occurred in QueryGroup service", e);
            }
        },
            new TimeValue(1, TimeUnit.SECONDS), // TODO get this from QueryGroupServiceSettings
            ThreadPool.Names.GENERIC
        );
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
