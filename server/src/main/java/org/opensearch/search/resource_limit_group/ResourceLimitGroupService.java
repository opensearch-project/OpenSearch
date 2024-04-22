/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resource_limit_group;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.search.resource_limit_group.cancellation.ResourceLimitGroupTaskCanceller;
import org.opensearch.search.resource_limit_group.tracker.ResourceLimitGroupResourceUsageTracker;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * Main service which will run periodically to track and cancel resource constraint violating tasks in resourceLimitGroups
 */
public class ResourceLimitGroupService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(ResourceLimitGroupService.class);

    private final ResourceLimitGroupResourceUsageTracker requestTracker;
    private final ResourceLimitGroupTaskCanceller requestCanceller;
    private final ResourceLimitGroupPruner resourceLimitGroupPruner;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final ResourceLimitGroupServiceSettings sandboxServiceSettings;
    private final ThreadPool threadPool;
    private final ResourceLimitGroupTaskCanceller taskCanceller;

    /**
     * Guice managed constructor
     * @param requestTrackerService
     * @param requestCanceller
     * @param resourceLimitGroupPruner
     * @param sandboxServiceSettings
     * @param threadPool
     */
    @Inject
    public ResourceLimitGroupService(
        ResourceLimitGroupResourceUsageTracker requestTrackerService,
        ResourceLimitGroupTaskCanceller requestCanceller,
        ResourceLimitGroupPruner resourceLimitGroupPruner,
        ResourceLimitGroupServiceSettings sandboxServiceSettings,
        ThreadPool threadPool,
        ResourceLimitGroupTaskCanceller taskCanceller
    ) {
        this.requestTracker = requestTrackerService;
        this.requestCanceller = requestCanceller;
        this.resourceLimitGroupPruner = resourceLimitGroupPruner;
        this.sandboxServiceSettings = sandboxServiceSettings;
        this.threadPool = threadPool;
        this.taskCanceller = taskCanceller;
    }

    /**
     * run at regular interval
     */
    private void doRun() {
        requestTracker.updateResourceLimitGroupsResourceUsage();
        taskCanceller.cancelTasks();
        resourceLimitGroupPruner.pruneResourceLimitGroup();
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
                logger.debug("Exception occurred in Query Sandbox service", e);
            }
        }, sandboxServiceSettings.getRunIntervalMillis(), ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {}

    // public SandboxStatsHolder stats() {
    // return requestTrackerService.getSandboxLevelStats();
    // }
}
