/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.autoforcemerge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.indices.IndicesService;
import org.opensearch.monitor.MonitorService;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.os.OsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
    private final FsService fsService;
    private final JvmService jvmService;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private AsyncForceMergeTask task;
    private ConfigurationValidator configurationValidator;
    private NodeValidator nodeValidator;
    private ShardValidator shardValidator;
    public String nodeId;
    private final AutoForceMergeMetrics autoForceMergeMetrics;
    private final ForceMergeManagerSettings forceMergeManagerSettings;
    private final CommonStatsFlags flags = new CommonStatsFlags(CommonStatsFlags.Flag.Segments, CommonStatsFlags.Flag.Translog);

    private static final Logger logger = LogManager.getLogger(AutoForceMergeManager.class);

    public AutoForceMergeManager(
        ThreadPool threadPool,
        MonitorService monitorService,
        IndicesService indicesService,
        ClusterService clusterService,
        AutoForceMergeMetrics autoForceMergeMetrics
    ) {
        this.threadPool = threadPool;
        this.osService = monitorService.osService();
        this.fsService = monitorService.fsService();
        this.jvmService = monitorService.jvmService();
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.autoForceMergeMetrics = autoForceMergeMetrics;
        this.forceMergeManagerSettings = new ForceMergeManagerSettings(clusterService, this::modifySchedulerInterval);
    }

    @Override
    protected void doStart() {
        this.configurationValidator = new ConfigurationValidator();
        this.nodeValidator = new NodeValidator();
        this.shardValidator = new ShardValidator();
        this.nodeId = clusterService.localNode().getId();
        this.task = new AsyncForceMergeTask();
    }

    @Override
    protected void doStop() {
        if (task != null) {
            this.task.close();
        }
    }

    @Override
    protected void doClose() {
        if (task != null) {
            this.task.close();
        }
    }

    private void modifySchedulerInterval(TimeValue schedulerInterval) {
        this.task.setInterval(schedulerInterval);
    }

    private void triggerForceMerge() {
        long startTime = System.currentTimeMillis();
        if (configurationValidator.hasWarmNodes() == false) {
            logger.debug("No warm nodes found. Skipping Auto Force merge.");
            autoForceMergeMetrics.incrementCounter(
                autoForceMergeMetrics.skipsFromConfigValidator,
                1.0,
                autoForceMergeMetrics.getTags(nodeId, null)
            );
            autoForceMergeMetrics.incrementCounter(
                autoForceMergeMetrics.totalMergesSkipped,
                1.0,
                autoForceMergeMetrics.getTags(nodeId, null)
            );
            return;
        }
        if (nodeValidator.validate().isAllowed() == false) {
            logger.debug("Node capacity constraints are not allowing to trigger auto ForceMerge");
            autoForceMergeMetrics.incrementCounter(
                autoForceMergeMetrics.skipsFromNodeValidator,
                1.0,
                autoForceMergeMetrics.getTags(nodeId, null)
            );
            autoForceMergeMetrics.incrementCounter(
                autoForceMergeMetrics.totalMergesSkipped,
                1.0,
                autoForceMergeMetrics.getTags(nodeId, null)
            );
            return;
        }
        int iteration = nodeValidator.getMaxConcurrentForceMerges();
        long mergesTriggered = 0;
        for (IndexShard shard : getShardsBasedOnSorting(indicesService)) {
            if (iteration == 0 || nodeValidator.validate().isAllowed() == false) {
                logger.debug("Node conditions no longer suitable for force merge.");
                autoForceMergeMetrics.incrementCounter(
                    autoForceMergeMetrics.skipsFromNodeValidator,
                    1.0,
                    autoForceMergeMetrics.getTags(nodeId, null)
                );
                autoForceMergeMetrics.incrementCounter(
                    autoForceMergeMetrics.totalMergesSkipped,
                    1.0,
                    autoForceMergeMetrics.getTags(nodeId, null)
                );
                break;
            }
            iteration--;
            CompletableFuture.runAsync(() -> {
                try {
                    long forceMergeStartTime = System.currentTimeMillis();
                    String shardId = String.valueOf(shard.shardId().getId());
                    autoForceMergeMetrics.incrementCounter(
                        autoForceMergeMetrics.currentActiveMerges,
                        1.0,
                        autoForceMergeMetrics.getTags(nodeId, null)
                    );
                    autoForceMergeMetrics.incrementCounter(
                        autoForceMergeMetrics.totalMergesTriggered,
                        1.0,
                        autoForceMergeMetrics.getTags(nodeId, null)
                    );
                    shard.forceMerge(new ForceMergeRequest().maxNumSegments(forceMergeManagerSettings.getSegmentCount()));
                    logger.debug("Merging is completed successfully for the shard {}", shard.shardId());
                    autoForceMergeMetrics.incrementCounter(
                        autoForceMergeMetrics.totalMergesCompleted,
                        1.0,
                        autoForceMergeMetrics.getTags(nodeId, null)
                    );
                    autoForceMergeMetrics.recordInHistogram(
                        autoForceMergeMetrics.shardForceMergeLatency,
                        (double) System.currentTimeMillis() - forceMergeStartTime,
                        autoForceMergeMetrics.getTags(nodeId, shardId)
                    );
                } catch (IOException e) {
                    logger.error("Error during force merge for shard {}\nException: {}", shard.shardId(), e);
                    autoForceMergeMetrics.incrementCounter(
                        autoForceMergeMetrics.totalMergesFailed,
                        1.0,
                        autoForceMergeMetrics.getTags(nodeId, null)
                    );
                } finally {
                    autoForceMergeMetrics.incrementCounter(
                        autoForceMergeMetrics.currentActiveMerges,
                        -1.0,
                        autoForceMergeMetrics.getTags(nodeId, null)
                    );
                }
            }, threadPool.executor(ThreadPool.Names.FORCE_MERGE));
            mergesTriggered++;
            logger.info("Successfully triggered force merge for shard {}", shard.shardId());
            try {
                Thread.sleep(forceMergeManagerSettings.getForcemergeDelay().getMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Timer was interrupted while waiting between shards", e);
                break;
            }
        }
        autoForceMergeMetrics.recordInHistogram(
            autoForceMergeMetrics.autoForceMergeSchedulerLatency,
            (double) System.currentTimeMillis() - startTime,
            autoForceMergeMetrics.getTags(nodeId, null)
        );
        autoForceMergeMetrics.recordInHistogram(
            autoForceMergeMetrics.mergesTriggeredInLastExecution,
            (double) mergesTriggered,
            autoForceMergeMetrics.getTags(nodeId, null)
        );
    }

    private List<IndexShard> getShardsBasedOnSorting(Iterable<IndexService> indicesService) {
        List<IndexShard> indexShardList = StreamSupport.stream(indicesService.spliterator(), false)
            .flatMap(indexService -> StreamSupport.stream(indexService.spliterator(), false))
            .filter(shard -> shard.routingEntry().primary())
            .filter(shard -> shardValidator.validate(shard).isAllowed())
            .sorted(new ShardAgeComparator())
            .limit(getNodeValidator().getMaxConcurrentForceMerges())
            .collect(Collectors.toList());
        autoForceMergeMetrics.recordInHistogram(
            autoForceMergeMetrics.totalEligibleShards,
            (double) indexShardList.size(),
            autoForceMergeMetrics.getTags(nodeId, null)
        );
        return indexShardList;
    }

    private class ShardAgeComparator implements Comparator<IndexShard> {
        @Override
        public int compare(IndexShard s1, IndexShard s2) {
            long age1 = getEarliestLastModifiedAge(s1);
            long age2 = getEarliestLastModifiedAge(s2);
            return Long.compare(age1, age2);
        }
    }

    private long getEarliestLastModifiedAge(IndexShard shard) {
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

        private final boolean isOnlyDataNode;
        private boolean isRemoteStoreEnabled = false;
        private boolean hasWarmNodes = false;

        ConfigurationValidator() {
            DiscoveryNode localNode = clusterService.localNode();
            isOnlyDataNode = localNode.isDataNode() && !localNode.isWarmNode();
            isRemoteStoreEnabled = isRemoteStorageEnabled();
        }

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
            if (forceMergeManagerSettings.isAutoForceMergeFeatureEnabled() == false) {
                logger.debug("Cluster configuration shows auto force merge feature is disabled. Closing task.");
                return new ValidationResult(false);
            }
            if (isRemoteStoreEnabled == false) {
                logger.debug("Cluster configuration is not meeting the criteria. Closing task.");
                task.close();
                return new ValidationResult(false);
            }
            if (isOnlyDataNode == false) {
                logger.debug("Node configuration doesn't meet requirements. Closing task.");
                task.close();
                return new ValidationResult(false);
            }
            return new ValidationResult(true);
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
            if (hasWarmNodes == true) return true;
            ClusterState clusterState = clusterService.state();
            return hasWarmNodes = clusterState.getNodes().getNodes().values().stream().anyMatch(DiscoveryNode::isWarmNode);
        }
    }

    /**
     * Validates node-level conditions for force merge operations.
     * This validator checks CPU usage, JVM memory usage, and force merge thread availability
     * to determine if force merge operations can proceed safely.
     */
    protected class NodeValidator implements ValidationStrategy {

        @Override
        public ValidationResult validate() {
            double cpuPercent = osService.stats().getCpu().getPercent();
            if (cpuPercent >= forceMergeManagerSettings.getCpuThreshold()) {
                logger.debug("CPU usage: {} breached the threshold: {}", cpuPercent, forceMergeManagerSettings.getCpuThreshold());
                return new ValidationResult(false);
            }
            long total = fsService.stats().getTotal().getTotal().getBytes();
            long available = fsService.stats().getTotal().getAvailable().getBytes();
            double diskPercent = ((double) available / total) * 100;
            if (diskPercent >= forceMergeManagerSettings.getDiskThreshold()) {
                logger.debug("Disk usage: {} breached the threshold: {}", diskPercent, forceMergeManagerSettings.getDiskThreshold());
                return new ValidationResult(false);
            }
            double jvmUsedPercent = jvmService.stats().getMem().getHeapUsedPercent();
            if (jvmUsedPercent >= forceMergeManagerSettings.getJvmThreshold()) {
                logger.debug("JVM memory: {}% breached the threshold: {}", jvmUsedPercent, forceMergeManagerSettings.getJvmThreshold());
                return new ValidationResult(false);
            }
            if (areForceMergeThreadsAvailable() == false) {
                logger.debug("No force merge threads available");
                return new ValidationResult(false);
            }
            return new ValidationResult(true);
        }

        private boolean areForceMergeThreadsAvailable() {
            for (ThreadPoolStats.Stats stats : threadPool.stats()) {
                if (stats.getName().equals(ThreadPool.Names.FORCE_MERGE)) {
                    return stats.getQueue() == 0;
                }
            }
            return false;
        }

        public Integer getMaxConcurrentForceMerges() {
            return Math.max(1, (OpenSearchExecutors.allocatedProcessors(clusterService.getSettings()) / 8)) * forceMergeManagerSettings
                .getConcurrencyMultiplier();
        }

        public String getNodeId() {
            return clusterService.localNode().getName();
        }
    }

    /**
     * Validates shard-level conditions for force merge operations.
     * This validator checks segment count and translog age to determine
     * if a specific shard is eligible for force merge.
     */
    protected class ShardValidator implements ValidationStrategy {

        @Override
        public ValidationResult validate(IndexShard shard) {
            if (shard.state() != IndexShardState.STARTED) {
                logger.debug("Shard({}) skipped: Shard is not in started state.", shard.shardId());
                return new ValidationResult(false);
            }
            if (isIndexAutoForceMergeEnabled(shard) == false) {
                logger.debug("Shard({}) skipped: Shard doesn't belong to a warm candidate index", shard.shardId());
                return new ValidationResult(false);
            }
            CommonStats stats = new CommonStats(indicesService.getIndicesQueryCache(), shard, flags);
            SegmentsStats segmentsStats = stats.getSegments();
            TranslogStats translogStats = stats.getTranslog();
            if (segmentsStats != null && segmentsStats.getCount() <= forceMergeManagerSettings.getSegmentCount()) {
                logger.debug(
                    "Shard({}) skipped: Shard has {} segments, not exceeding threshold of {}",
                    shard.shardId(),
                    segmentsStats.getCount(),
                    forceMergeManagerSettings.getSegmentCount()
                );
                return new ValidationResult(false);
            }
            if (translogStats != null
                && translogStats.getEarliestLastModifiedAge() < forceMergeManagerSettings.getTranslogAge().getMillis()) {
                logger.debug(
                    "Shard({}) skipped: Translog is too recent. Age({}ms)",
                    shard.shardId(),
                    translogStats.getEarliestLastModifiedAge()
                );
                return new ValidationResult(false);
            }
            return new ValidationResult(true);
        }

        private boolean isIndexAutoForceMergeEnabled(IndexShard shard) {
            IndexSettings indexSettings = shard.indexSettings();
            return indexSettings.isAutoForcemergeEnabled();
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
            if (configurationValidator.validate().isAllowed() == false) {
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
