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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
    private Integer allocatedProcessors;
    private String nodeId;
    private final AutoForceMergeMetrics autoForceMergeMetrics;
    private ResourceTrackerProvider.ResourceTrackers resourceTrackers;
    private ForceMergeManagerSettings forceMergeManagerSettings;
    private final CommonStatsFlags flags = new CommonStatsFlags(CommonStatsFlags.Flag.Segments, CommonStatsFlags.Flag.Translog);
    private final Set<Integer> mergingShards;

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
        this.mergingShards = new HashSet<>();
    }

    @Override
    protected void doStart() {
        this.forceMergeManagerSettings = new ForceMergeManagerSettings(clusterService, this::modifySchedulerInterval);
        this.task = new AsyncForceMergeTask();
        this.configurationValidator = new ConfigurationValidator();
        this.nodeValidator = new NodeValidator();
        this.shardValidator = new ShardValidator();
        this.allocatedProcessors = OpenSearchExecutors.allocatedProcessors(clusterService.getSettings());
        this.resourceTrackers = ResourceTrackerProvider.create(threadPool);
        this.nodeId = clusterService.localNode().getId();
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
        try {
            if (isValidForForceMerge() == false) {
                return;
            }
            executeForceMergeOnShards();
        } finally {
            autoForceMergeMetrics.recordInHistogram(
                autoForceMergeMetrics.schedulerExecutionTime,
                (double) System.currentTimeMillis() - startTime,
                autoForceMergeMetrics.getTags(Optional.of(nodeId), Optional.empty())
            );
        }
    }

    private boolean isValidForForceMerge() {
        if (configurationValidator.hasWarmNodes() == false) {
            resourceTrackers.stop();
            logger.debug("No warm nodes found. Skipping Auto Force merge.");
            autoForceMergeMetrics.incrementCounter(
                autoForceMergeMetrics.skipsFromConfigValidator,
                1.0,
                autoForceMergeMetrics.getTags(Optional.of(nodeId), Optional.empty())
            );
            return false;
        }
        if (nodeValidator.validate().isAllowed() == false) {
            logger.debug("Node capacity constraints are not allowing to trigger auto ForceMerge");
            autoForceMergeMetrics.incrementCounter(
                autoForceMergeMetrics.skipsFromNodeValidator,
                1.0,
                autoForceMergeMetrics.getTags(Optional.of(nodeId), Optional.empty())
            );
            return false;
        }
        return true;
    }

    private void executeForceMergeOnShards() {
        int remainingIterations = nodeValidator.getMaxConcurrentForceMerges();
        for (IndexShard shard : getShardsBasedOnSorting(indicesService)) {
            if (remainingIterations == 0 || !nodeValidator.validate().isAllowed()) {
                if (remainingIterations > 0) {
                    logger.debug("Node conditions no longer suitable for force merge.");
                }
                break;
            }
            remainingIterations--;
            executeForceMergeForShard(shard);
            if (!waitBetweenShards()) {
                break;
            }
        }
    }

    private void executeForceMergeForShard(IndexShard shard) {
        CompletableFuture.runAsync(() -> {
            long startTime = System.currentTimeMillis();
            try {
                mergingShards.add(shard.shardId().getId());
                autoForceMergeMetrics.incrementCounter(
                    autoForceMergeMetrics.mergesTriggered,
                    1.0,
                    autoForceMergeMetrics.getTags(Optional.of(nodeId), Optional.empty())
                );

                CommonStats stats = new CommonStats(indicesService.getIndicesQueryCache(), shard, flags);
                if (stats.getSegments() != null) {
                    autoForceMergeMetrics.incrementCounter(
                        autoForceMergeMetrics.segmentCount,
                        (double) stats.getSegments().getCount(),
                        autoForceMergeMetrics.getTags(Optional.of(nodeId), Optional.of(String.valueOf(shard.shardId().getId())))
                    );
                }

                long shardSizeInBytes = shard.store().stats(0L).sizeInBytes();
                autoForceMergeMetrics.incrementCounter(
                    autoForceMergeMetrics.shardSize,
                    (double) shardSizeInBytes,
                    autoForceMergeMetrics.getTags(Optional.of(nodeId), Optional.of(String.valueOf(shard.shardId().getId())))
                );

                shard.forceMerge(new ForceMergeRequest().maxNumSegments(forceMergeManagerSettings.getSegmentCount()));

                logger.debug("Merging is completed successfully for the shard {}", shard.shardId());
            } catch (Exception e) {
                autoForceMergeMetrics.incrementCounter(
                    autoForceMergeMetrics.mergesFailed,
                    1.0,
                    autoForceMergeMetrics.getTags(Optional.of(nodeId), Optional.empty())
                );
                logger.error("Error during force merge for shard {}\nException: {}", shard.shardId(), e);
            } finally {
                autoForceMergeMetrics.recordInHistogram(
                    autoForceMergeMetrics.shardMergeLatency,
                    (double) System.currentTimeMillis() - startTime,
                    autoForceMergeMetrics.getTags(Optional.of(nodeId), Optional.of(String.valueOf(shard.shardId().getId())))
                );
                mergingShards.remove(shard.shardId().getId());
            }
        }, threadPool.executor(ThreadPool.Names.FORCE_MERGE));
        logger.info("Successfully triggered force merge for shard {}", shard.shardId());
    }

    private boolean waitBetweenShards() {
        try {
            Thread.sleep(forceMergeManagerSettings.getForcemergeDelay().getMillis());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Timer was interrupted while waiting between shards", e);
            return false;
        }
    }

    private List<IndexShard> getShardsBasedOnSorting(Iterable<IndexService> indicesService) {
        Map<IndexShard, Long> shardAgeCache = new HashMap<>();

        return StreamSupport.stream(indicesService.spliterator(), false)
            .flatMap(indexService -> StreamSupport.stream(indexService.spliterator(), false))
            .filter(shard -> !shard.shardId().getIndexName().startsWith("."))
            .filter(shard -> shard.routingEntry().primary())
            .filter(shard -> !mergingShards.contains(shard.shardId().getId()))
            .filter(shard -> shardValidator.validate(shard).isAllowed())
            .peek(shard -> shardAgeCache.computeIfAbsent(shard, this::getEarliestLastModifiedAge))
            .sorted(new ShardAgeComparator(shardAgeCache))
            .limit(getNodeValidator().getMaxConcurrentForceMerges())
            .collect(Collectors.toList());
    }

    private static class ShardAgeComparator implements Comparator<IndexShard> {
        private final Map<IndexShard, Long> shardAgeCache;

        public ShardAgeComparator(Map<IndexShard, Long> shardAgeCache) {
            this.shardAgeCache = shardAgeCache;
        }

        @Override
        public int compare(IndexShard s1, IndexShard s2) {
            long age1 = shardAgeCache.get(s1);
            long age2 = shardAgeCache.get(s2);
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
            resourceTrackers.start();
            if (isCpuUsageOverThreshold()) {
                return new ValidationResult(false);
            }
            if (isDiskUsageOverThreshold()) {
                return new ValidationResult(false);
            }
            if (isJvmUsageOverThreshold()) {
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

        private boolean isJvmUsageOverThreshold() {
            double jvmAverage = resourceTrackers.jvmFiveMinute.getAverage();
            if (jvmAverage >= forceMergeManagerSettings.getJvmThreshold()) {
                logger.debug("JVM Average: 5m({}%) breached the threshold: {}", jvmAverage, forceMergeManagerSettings.getJvmThreshold());
                return true;
            }
            jvmAverage = resourceTrackers.jvmOneMinute.getAverage();
            if (jvmAverage >= forceMergeManagerSettings.getJvmThreshold()) {
                logger.debug("JVM Average: 1m({}%) breached the threshold: {}", jvmAverage, forceMergeManagerSettings.getJvmThreshold());
                return true;
            }
            double jvmUsedPercent = jvmService.stats().getMem().getHeapUsedPercent();
            if (jvmUsedPercent >= forceMergeManagerSettings.getJvmThreshold()) {
                logger.debug("JVM memory: {}% breached the threshold: {}", jvmUsedPercent, forceMergeManagerSettings.getJvmThreshold());
                return true;
            }
            return false;
        }

        private boolean isCpuUsageOverThreshold() {
            double cpuAverage = resourceTrackers.cpuFiveMinute.getAverage();
            if (cpuAverage >= forceMergeManagerSettings.getCpuThreshold()) {
                logger.debug("CPU Average: 5m({}%) breached the threshold: {}", cpuAverage, forceMergeManagerSettings.getCpuThreshold());
                return true;
            }
            cpuAverage = resourceTrackers.cpuOneMinute.getAverage();
            if (cpuAverage >= forceMergeManagerSettings.getCpuThreshold()) {
                logger.debug("CPU Average: 1m({}%) breached the threshold: {}", cpuAverage, forceMergeManagerSettings.getCpuThreshold());
                return true;
            }
            double cpuPercent = osService.stats().getCpu().getPercent();
            if (cpuPercent >= forceMergeManagerSettings.getCpuThreshold()) {
                logger.debug("CPU usage: {} breached the threshold: {}", cpuPercent, forceMergeManagerSettings.getCpuThreshold());
                return true;
            }
            return false;
        }

        private boolean isDiskUsageOverThreshold() {
            long total = fsService.stats().getTotal().getTotal().getBytes();
            long available = fsService.stats().getTotal().getAvailable().getBytes();
            double diskPercent = ((double) (total - available) / total) * 100;
            if (diskPercent >= forceMergeManagerSettings.getDiskThreshold()) {
                logger.debug("Disk usage: {}% breached the threshold: {}", diskPercent, forceMergeManagerSettings.getDiskThreshold());
                return true;
            }
            return false;
        }

        public Integer getMaxConcurrentForceMerges() {
            return Math.max(1, (allocatedProcessors / 8)) * forceMergeManagerSettings.getConcurrencyMultiplier();
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
            TranslogStats translogStats = stats.getTranslog();
            if (translogStats != null
                && translogStats.getEarliestLastModifiedAge() < forceMergeManagerSettings.getTranslogAge().getMillis()) {
                logger.debug(
                    "Shard({}) skipped: Translog is too recent. Age({}ms)",
                    shard.shardId(),
                    translogStats.getEarliestLastModifiedAge()
                );
                return new ValidationResult(false);
            }
            SegmentsStats segmentsStats = stats.getSegments();
            if (segmentsStats != null && segmentsStats.getCount() <= forceMergeManagerSettings.getSegmentCount()) {
                logger.debug(
                    "Shard({}) skipped: Shard has {} segments, not exceeding threshold of {}",
                    shard.shardId(),
                    segmentsStats.getCount(),
                    forceMergeManagerSettings.getSegmentCount()
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
                resourceTrackers.stop();
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
