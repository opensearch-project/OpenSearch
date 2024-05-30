/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.Sandbox;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.sandboxing.cancellation.DefaultTaskCancellation;
import org.opensearch.search.sandboxing.cancellation.LongestRunningTaskFirstStrategy;
import org.opensearch.search.sandboxing.tracker.SandboxUsageTracker;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Main service which will run periodically to track and cancel resource constraint violating tasks in sandboxes
 */
public class SandboxService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(SandboxService.class);

    private final SandboxUsageTracker sandboxUsageTracker;
//    private final SandboxPruner sandboxPruner;
    private volatile Scheduler.Cancellable scheduledFuture;
//    private final SandboxServiceSettings sandboxServiceSettings;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    /**
     * Guice managed constructor
     *
     * @param sandboxUsageTracker
//     * @param sandboxPruner
//     * @param sandboxServiceSettings
     * @param threadPool
     */
    @Inject
    public SandboxService(
        SandboxUsageTracker sandboxUsageTracker,
//        SandboxServiceSettings sandboxServiceSettings,
//        SandboxPruner sandboxPruner,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        this.sandboxUsageTracker = sandboxUsageTracker;
//        this.sandboxServiceSettings = sandboxServiceSettings;
//        this.sandboxPruner = sandboxPruner;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * run at regular interval
     */
    private void doRun() {
        Map<String, SandboxLevelResourceUsageView> sandboxLevelResourceUsageViews = sandboxUsageTracker.constructSandboxLevelUsageViews();
        Set<Sandbox> activeSandboxes = getActiveSandboxes();
        DefaultTaskCancellation taskCancellation = new DefaultTaskCancellation(
            new LongestRunningTaskFirstStrategy(),
            sandboxLevelResourceUsageViews,
            activeSandboxes
        );
        taskCancellation.cancelTasks();
        // TODO Prune the sandboxes
    }

    private Set<Sandbox> getActiveSandboxes() {
        return new HashSet<>(clusterService.state().metadata().sandboxes().values());
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
            },
            new TimeValue(1, TimeUnit.SECONDS), // TODO get this from SandboxServiceSettings
            ThreadPool.Names.GENERIC);
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
