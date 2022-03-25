/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.sampleextension;

import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.plugins.Plugin;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;

/**
 * A sample job runner class.
 *
 * The job runner should be a singleton class if it uses OpenSearch client or other objects passed
 * from OpenSearch. Because when registering the job runner to JobScheduler plugin, OpenSearch has
 * not invoke plugins' createComponents() method. That is saying the plugin is not completely initalized,
 * and the OpenSearch {@link org.opensearch.client.Client}, {@link ClusterService} and other objects
 * are not available to plugin and this job runner.
 *
 * So we have to move this job runner intialization to {@link Plugin} createComponents() method, and using
 * singleton job runner to ensure we register a usable job runner instance to JobScheduler plugin.
 *
 * This sample job runner takes the "indexToWatch" from job parameter and logs that index's shards.
 */
public class SampleJobRunner implements ScheduledJobRunner {

    private static final Logger log = LogManager.getLogger(ScheduledJobRunner.class);

    private static SampleJobRunner INSTANCE;

    public static SampleJobRunner getJobRunnerInstance() {
        if(INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (SampleJobRunner.class) {
            if(INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new SampleJobRunner();
            return INSTANCE;
        }
    }

    private ClusterService clusterService;
    private ThreadPool threadPool;

    private SampleJobRunner() {
        // Singleton class, use getJobRunner method instead of constructor
    }

    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }
    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public void runJob(ScheduledJobParameter jobParameter, JobExecutionContext context) {
        if(!(jobParameter instanceof SampleJobParameter)) {
            throw new IllegalStateException("Job parameter is not instance of SampleJobParameter, type: "
                    + jobParameter.getClass().getCanonicalName());
        }

        if(this.clusterService == null) {
            throw new IllegalStateException("ClusterService is not initialized.");
        }

        if (this.threadPool == null) {
            throw new IllegalStateException("ThreadPool is not initialized.");
        }

        final LockService lockService = context.getLockService();

        Runnable runnable = () -> {
            if (jobParameter.getLockDurationSeconds() != null) {
                lockService.acquireLock(jobParameter, context, ActionListener.wrap(
                        lock -> {
                            if (lock == null) {
                                return;
                            }

                            SampleJobParameter parameter = (SampleJobParameter) jobParameter;
                            StringBuilder msg = new StringBuilder();
                            msg.append("Watching index ").append(parameter.getIndexToWatch()).append("\n");

                            List<ShardRouting> shardRoutingList = this.clusterService.state()
                                    .routingTable().allShards(parameter.getIndexToWatch());
                            for(ShardRouting shardRouting : shardRoutingList) {
                                msg.append(shardRouting.shardId().getId()).append("\t").append(shardRouting.currentNodeId()).append("\t")
                                        .append(shardRouting.active() ? "active" : "inactive").append("\n");
                            }
                            log.info(msg.toString());

                            lockService.release(lock, ActionListener.wrap(
                                    released -> {
                                        log.info("Released lock for job {}", jobParameter.getName());
                                    },
                                    exception -> {
                                        throw new IllegalStateException("Failed to release lock.");
                                    }
                            ));
                        },
                        exception -> {
                            throw new IllegalStateException("Failed to acquire lock.");
                        }
                ));
            }
        };

        threadPool.generic().submit(runnable);
    }
}
