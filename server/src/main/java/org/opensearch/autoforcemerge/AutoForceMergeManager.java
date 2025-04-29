/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autoforcemerge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.indices.IndicesService;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.os.OsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;

/**
 * AutoForceMergeManager : Manages automatic force merge operations for indices in OpenSearch. This component monitors and
 * triggers force merge on primary shards based on their translog age and system conditions. It ensures
 * optimal segment counts while respecting node resources and health constraints. Force merge operations
 * are executed with configurable delays to prevent system overload.
 *
 * @opensearch.internal
 */
public class AutoForceMergeManager extends AbstractLifecycleComponent {

    private final ThreadPool threadPool;
    private final OsService osService;
    private final JvmService jvmService;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private AsyncForceMergeTask task;
    private ConfigurationValidator configurationValidator;
    private NodeValidator nodeValidator;
    private ShardValidator shardValidator;
    private final ForceMergeManagerSettings forceMergeManagerSettings;
    private final AtomicBoolean initialCheckDone = new AtomicBoolean(false);

    private static final Logger logger = LogManager.getLogger(AutoForceMergeManager.class);

    public AutoForceMergeManager(ThreadPool threadPool, OsService osService, JvmService jvmService,
                                 IndicesService indicesService, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.osService = osService;
        this.indicesService = indicesService;
        this.jvmService = jvmService;
        this.clusterService = clusterService;
        this.forceMergeManagerSettings = new ForceMergeManagerSettings(clusterService.getSettings(), clusterService.getClusterSettings(), this);
        if (forceMergeManagerSettings.getAutoForceMergeFeatureEnabled()) {
            this.doStart();
        }
    }

    @Override
    protected void doStart() {
        this.task = new AsyncForceMergeTask();
        this.configurationValidator = new ConfigurationValidator();
        this.nodeValidator = new NodeValidator();
        this.shardValidator = new ShardValidator();
    }

    @Override
    protected void doStop() {
        if (this.task != null) {
            this.task.close();
        }
    }

    @Override
    protected void doClose() {
        if (this.task != null) {
            this.task.close();
        }
    }

    private void triggerForceMerge() {
        if (!configurationValidator.hasWarmNodes()) {
            logger.debug("No warm nodes found. Skipping Auto Force merge.");
            return;
        }

        if (!(nodeValidator.validate().isAllowed())) {
            logger.debug("Node capacity constraints are not allowing to trigger auto ForceMerge");
            return;
        }

        List<IndexShard> shards = new ArrayList<>();
        for (IndexService indexService : indicesService) {
            for (IndexShard shard : indexService) {
                if (shard.routingEntry().primary()) {
                    if (shardValidator.validate(shard).isAllowed()) {
                        shards.add(shard);
                    }
                }
            }
        }

        List<IndexShard> sortedShards = getSortedShardsByTranslogAge(shards);
        int iteration = nodeValidator.getMaxConcurrentForceMerges();
        for (IndexShard shard : sortedShards) {
            if (!nodeValidator.validate().isAllowed()) {
                logger.debug("Node conditions no longer suitable for force merge");
                break;
            }
            if (iteration == 0) {
                break;
            }
            iteration--;
            CompletableFuture.runAsync(() -> {
                try {
                    shard.forceMerge(new ForceMergeRequest()
                        .maxNumSegments(forceMergeManagerSettings.getSegmentCountThreshold()));
                    logger.info("Merging is completed successfully for the shard {}", shard.shardId());
                } catch (IOException e) {
                    logger.error("Error during force merge for shard {}", shard.shardId(), e);
                }
            }, threadPool.executor(ThreadPool.Names.FORCE_MERGE));

            logger.debug("Successfully triggered force merge for shard {}", shard.shardId());
            try {
                Thread.sleep(forceMergeManagerSettings.getForcemergeDelay().getMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Timer was interrupted while waiting between shards", e);
                break;
            }
        }
    }

    private List<IndexShard> getSortedShardsByTranslogAge(List<IndexShard> shards) {

        return shards.stream()
            .sorted(new ShardAgeComparator())
            .collect(Collectors.toList());
    }

    private class ShardAgeComparator implements Comparator<IndexShard> {
        @Override
        public int compare(IndexShard s1, IndexShard s2) {
            long age1 = getTranslogAge(s1);
            long age2 = getTranslogAge(s2);
            return Long.compare(age1, age2);
        }
    }

    private long getTranslogAge(IndexShard shard) {
        CommonStatsFlags flags = new CommonStatsFlags(CommonStatsFlags.Flag.Translog);
        CommonStats stats = new CommonStats(indicesService.getIndicesQueryCache(), shard, flags);
        return stats.getTranslog() != null ? stats.getTranslog().getEarliestLastModifiedAge() : 0;
    }

    /**
     * Validates the node configuration requirements for auto force merge operations.
     * This validator ensures that the node meets two primary criteria:
     * 1. It must be a dedicated data node (hot node)
     * 2. Remote store must be enabled
     * The validation is performed once and cached for subsequent checks to improve performance.
     */
    protected class ConfigurationValidator implements ValidationStrategy {

        private boolean isOnlyDataNode = false;
        private boolean isRemoteStoreEnabled = false;
        private boolean hasWarmNodes = false;

        /**
         * Validates the node configuration against required criteria.
         * This method first ensures initialization is complete, then checks if the node
         * is a dedicated data node with remote store enabled.
         *
         * @return ValidationResult with true if all configuration requirements are met,
         * ValidationResult(false) otherwise. If validation fails, the associated task is closed.
         */
        @Override
        public ValidationResult validate() {
            initializeIfNeeded();
            if (!isRemoteStoreEnabled) {
                logger.debug("Cluster configuration is not meeting the criteria. Closing task.");
                task.close();
                return new ValidationResult(false);
            }
            if (!isOnlyDataNode) {
                logger.debug("Node configuration doesn't meet requirements. Closing task.");
                task.close();
                return new ValidationResult(false);
            }
            return new ValidationResult(true);
        }

        /**
         * Initializes the configuration check results if not already done.
         * This method performs a one-time check of:
         * - Node type (must be data node but not warm node)
         * - Remote store configuration
         * The results are cached to avoid repeated checks.
         * Thread-safe through atomic operation on initialCheckDone.
         */
        private void initializeIfNeeded() {
            if (!initialCheckDone.get()) {
                DiscoveryNode localNode = clusterService.localNode();
                isOnlyDataNode = localNode.isDataNode() && !localNode.isWarmNode();
                isRemoteStoreEnabled = isRemoteStorageEnabled();
                initialCheckDone.set(true);
            }
        }

        /**
         * Checks if remote storage is enabled in the cluster settings.
         */
        private boolean isRemoteStorageEnabled() {
            return clusterService.getSettings().getAsBoolean(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), false);
        }

        /**
         * Checks if cluster has warm nodes.
         */
        private boolean hasWarmNodes() {
            if (hasWarmNodes) return true;
            ClusterState clusterState = clusterService.state();
            return hasWarmNodes = clusterState.getNodes().getNodes()
                .values()
                .stream()
                .anyMatch(DiscoveryNode::isWarmNode
                );
        }
    }

    /**
     * Validates node-level conditions for force merge operations.
     * This validator checks CPU usage, JVM memory usage, and force merge thread availability
     * to determine if force merge operations can proceed safely.
     */
    protected class NodeValidator implements ValidationStrategy {

        private int maxConcurrentForceMerges;

        @Override
        public ValidationResult validate() {
            double cpuPercent = osService.stats().getCpu().getPercent();
            if (cpuPercent >= forceMergeManagerSettings.getCpuThreshold()) {
                logger.debug("CPU usage too high: {}%", cpuPercent);
                return new ValidationResult(false);
            }
            double jvmUsedPercent = jvmService.stats().getMem().getHeapUsedPercent();
            if (jvmUsedPercent >= forceMergeManagerSettings.getJvmThreshold()) {
                logger.debug("JVM memory usage too high: {}%", jvmUsedPercent);
                return new ValidationResult(false);
            }
            if (!areForceMergeThreadsAvailable()) {
                logger.info("No force merge threads available");
                return new ValidationResult(false);
            }
            return new ValidationResult(true);
        }

        private boolean areForceMergeThreadsAvailable() {
            for (ThreadPoolStats.Stats stats : threadPool.stats()) {
                if (stats.getName().equals(ThreadPool.Names.FORCE_MERGE)) {
                    this.maxConcurrentForceMerges = Math.max(1, stats.getThreads()) * forceMergeManagerSettings.getConcurrencyMultiplier();
                    if ((stats.getQueue() == 0))
                        // If force merge thread count is set by the customer( greater than 0) and active thread count is already equal or more than this threshold block skip any more force merges
                        return forceMergeManagerSettings.getForceMergeThreadCount() <= 0 || stats.getActive() < forceMergeManagerSettings.getForceMergeThreadCount();
                }
            }
            return false;
        }

        public Integer getMaxConcurrentForceMerges() {
            return this.maxConcurrentForceMerges;
        }
    }

    /**
     * Validates shard-level conditions for force merge operations.
     * This validator checks segment count and translog age to determine
     * if a specific shard is eligible for force merge.
     */
    protected class ShardValidator implements ValidationStrategy {

        private final CommonStatsFlags flags = new CommonStatsFlags(CommonStatsFlags.Flag.Segments, CommonStatsFlags.Flag.Translog);

        @Override
        public ValidationResult validate(IndexShard shard) {
            if (shard == null) {
                logger.debug("No shard found.");
                return new ValidationResult(false);
            }
            CommonStats stats = new CommonStats(indicesService.getIndicesQueryCache(), shard, flags);
            SegmentsStats segmentsStats = stats.getSegments();
            TranslogStats translogStats = stats.getTranslog();
            if (segmentsStats != null && segmentsStats.getCount() <= forceMergeManagerSettings.getSegmentCountThreshold()) {
                logger.debug("Shard {} doesn't have enough segments to merge.", shard.shardId());
                return new ValidationResult(false);
            }
            if (translogStats != null && translogStats.getEarliestLastModifiedAge() < forceMergeManagerSettings.getSchedulerInterval().getMillis()) {
                logger.debug("Shard {} translog is too recent.", shard.shardId());
                return new ValidationResult(false);
            }
            return new ValidationResult(true);
        }
    }

    /**
     * Strategy interface for implementing different validation approaches
     * in the force merge process. Implementations can validate different aspects
     * such as node conditions, shard conditions, or custom criteria.
     */
    public interface ValidationStrategy {
        default ValidationResult validate() {
            return new ValidationResult(false);
        }

        default ValidationResult validate(IndexShard shard) {
            return new ValidationResult(false);
        }
    }

    /**
     * Represents the result of a validation operation.
     * This class is immutable and thread-safe.
     */
    public static final class ValidationResult {
        private final boolean allowed;

        public ValidationResult(boolean allowed) {
            this.allowed = allowed;
        }

        public boolean isAllowed() {
            return allowed;
        }
    }

    /**
     * Asynchronous task that manages force merge operations.
     * This task runs periodically to check conditions and trigger force merge
     * operations when appropriate.
     */
    protected final class AsyncForceMergeTask extends AbstractAsyncTask {

        /**
         * Constructs a new AsyncForceMergeTask and initializes its schedule.
         */
        public AsyncForceMergeTask() {
            super(logger, threadPool, forceMergeManagerSettings.getSchedulerInterval(), true);
            rescheduleIfNecessary();
        }

        /**
         * Determines if the task should be rescheduled after completion.
         *
         * @return true to indicate that the task should always be rescheduled
         */
        @Override
        protected boolean mustReschedule() {
            return true;
        }

        /**
         * Executes the force merge task's core logic.
         * Validates configuration and triggers force merge if conditions are met.
         */
        @Override
        protected void runInternal() {
            if (!initialCheckDone.get() && !(configurationValidator.validate().isAllowed())) {
                return;
            }
            triggerForceMerge();
        }

        /**
         * Specifies which thread pool should be used for this task.
         */
        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }
    }

    protected AsyncForceMergeTask getTask() {
        return task;
    }

    protected ConfigurationValidator getConfigurationValidator() {
        return configurationValidator;
    }

    protected NodeValidator getNodeValidator() {
        return nodeValidator;
    }

    protected ShardValidator getShardValidator() {
        return shardValidator;
    }
}

