/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandbox;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * Main service which will run periodically to track and cancel resource constraint violating tasks in sandboxes
 */
public class QuerySandboxService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(QuerySandboxService.class);

    private final SandboxedRequestTrackerService requestTrackerService;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final QuerySandboxServiceSettings sandboxServiceSettings;
    private final ThreadPool threadPool;

    public QuerySandboxService(
        SandboxedRequestTrackerService requestTrackerService,
        QuerySandboxServiceSettings sandboxServiceSettings,
        ThreadPool threadPool
    ) {
        this.requestTrackerService = requestTrackerService;
        this.sandboxServiceSettings = sandboxServiceSettings;
        this.threadPool = threadPool;
    }

    private void doRun() {
        requestTrackerService.updateSandboxResourceUsages();
        requestTrackerService.cancelViolatingTasks();
        requestTrackerService.pruneSandboxes();
    }

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

    public SandboxStatsHolder stats() {
        return requestTrackerService.getSandboxLevelStats();
    }
}
