/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * AutoForceMergeManager : Manages automatic force merge operations for indices in OpenSearch. This component monitors and
 * triggers force merge on primary shards based on their translog age and system conditions. It ensures
 * optimal segment counts while respecting node resources and health constraints. Force merge operations
 * are executed with configurable delays to prevent system overload.
 *
 * @opensearch.internal
 */
public class AutoForceMergeManager {

    private final ThreadPool threadPool;
    private final OsService osService;
    private final JvmService jvmService;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final AsyncForceMergeTask task;
    private final ConfigurationValidator configurationValidator;
    private final NodeValidator nodeValidator;
    private final ShardValidator shardValidator;
    private final Settings settings;
    private final ClusterSettings clusterSettings;
    private boolean isOnlyDataNode = false;
    private boolean isRemoteStoreEnabled = false;
    private boolean hasWarmNodes = false;
    private final AtomicBoolean initialCheckDone = new AtomicBoolean(false);
    private Integer segmentCountThreshold;
    private TimeValue waitTime;
    private TimeValue schedulerFrequency;
    private Double cpuThreshold;
    private Double jvmThreshold;
    private Integer forceMergeThreadCount;

    public static final Setting<Integer> SEGMENT_COUNT_THRESHOLD_FOR_AUTO_FORCE_MERGE = Setting.intSetting(
        "cluster.auto.force.merge.segment.count",
        1,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> WAIT_BETWEEN_AUTO_FORCE_MERGE_SHARDS = Setting.timeSetting(
        "cluster.auto.force.merge.wait",
        TimeValue.timeValueSeconds(15),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> AUTO_FORCE_MERGE_SCHEDULER_FREQUENCY = Setting.timeSetting(
        "cluster.auto.force.merge.scheduler.frequency",
        TimeValue.timeValueMinutes(30),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Double> CPU_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE = Setting.doubleSetting(
        "cluster.auto.force.merge.cpu.threshold",
        80.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Double> JVM_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE = Setting.doubleSetting(
        "cluster.auto.force.merge.jvm.threshold",
        70.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> FORCE_MERGE_THREADS_THRESHOLD_COUNT_FOR_AUTO_FORCE_MERGE = Setting.intSetting(
        "cluster.auto.force.merge.threads.threshold",
        1,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private void setSegmentCountThreshold(Integer segmentCountThreshold) {
        this.segmentCountThreshold = segmentCountThreshold;
    }

    private void setWaitTime(TimeValue waitTime) {
        this.waitTime = waitTime;
    }

    private void setSchedulerFrequency(TimeValue schedulerFrequency) {
        this.schedulerFrequency = schedulerFrequency;
    }

    private void setCpuThreshold(Double cpuThreshold) {
        this.cpuThreshold = cpuThreshold;
    }

    private void setJvmThreshold(Double jvmThreshold) {
        this.jvmThreshold = jvmThreshold;
    }

    private void setForceMergeThreadCount(Integer forceMergeThreadCount) {
        this.forceMergeThreadCount = forceMergeThreadCount;
    }

    private static final Logger logger = LogManager.getLogger(AutoForceMergeManager.class);

    @Inject
    public AutoForceMergeManager(ThreadPool threadPool, OsService osService, JvmService jvmService,
                                 IndicesService indicesService, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.osService = osService;
        this.indicesService = indicesService;
        this.jvmService = jvmService;
        this.clusterService = clusterService;
        this.settings = clusterService.getSettings();
        this.clusterSettings = clusterService.getClusterSettings();
        this.segmentCountThreshold = SEGMENT_COUNT_THRESHOLD_FOR_AUTO_FORCE_MERGE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SEGMENT_COUNT_THRESHOLD_FOR_AUTO_FORCE_MERGE, this::setSegmentCountThreshold);
        this.waitTime = WAIT_BETWEEN_AUTO_FORCE_MERGE_SHARDS.get(settings);
        clusterSettings.addSettingsUpdateConsumer(WAIT_BETWEEN_AUTO_FORCE_MERGE_SHARDS, this::setWaitTime);
        this.schedulerFrequency = AUTO_FORCE_MERGE_SCHEDULER_FREQUENCY.get(settings);
        clusterSettings.addSettingsUpdateConsumer(AUTO_FORCE_MERGE_SCHEDULER_FREQUENCY, this::setSchedulerFrequency);
        this.cpuThreshold = CPU_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CPU_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE, this::setCpuThreshold);
        this.jvmThreshold = JVM_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(JVM_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE, this::setJvmThreshold);
        this.forceMergeThreadCount = FORCE_MERGE_THREADS_THRESHOLD_COUNT_FOR_AUTO_FORCE_MERGE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(FORCE_MERGE_THREADS_THRESHOLD_COUNT_FOR_AUTO_FORCE_MERGE, this::setForceMergeThreadCount);
        task = new AsyncForceMergeTask();
        configurationValidator = new ConfigurationValidator();
        nodeValidator = new NodeValidator();
        shardValidator = new ShardValidator();
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

    private void triggerForceMerge() {
        if (!hasWarmNodes && !hasWarmNodes()) {
            logger.info("No warm nodes found. Skipping Auto Force merge.");
            return;
        }
        hasWarmNodes = true;

        if (!(nodeValidator.validate().isAllowed())) {
            logger.info("Node capacity constraints are not allowing to trigger auto ForceMerge");
            return;
        }

        List<IndexShard> shards = new ArrayList<>();
        for (IndexService indexService : indicesService) {
            for (IndexShard shard : indexService) {
                if (shard.routingEntry().primary()) {
                    shardValidator.setShard(shard);
                    if (shardValidator.validate().isAllowed()) {
                        shards.add(shard);
                    }
                }
            }
        }

        List<IndexShard> sortedShards = getSortedShardsByTranslogAge(shards);
        for (IndexShard shard : sortedShards) {
            if (!nodeValidator.validate().isAllowed()) {
                logger.info("Node conditions no longer suitable for force merge");
                break;
            }
            try {
                shard.forceMerge(new ForceMergeRequest().maxNumSegments(segmentCountThreshold));
                logger.info("Successfully triggered force merge for shard {}", shard.shardId());
                Thread.sleep(waitTime.getMillis());
            } catch (IOException e) {
                logger.error("Error during force merge for shard {}", shard.shardId(), e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Timer was interrupted while waiting between shards", e);
                break;
            }
        }
    }

    private boolean hasWarmNodes() {
        ClusterState clusterState = clusterService.state();
        return clusterState.getNodes().getNodes()
            .values()
            .stream()
            .anyMatch(DiscoveryNode::isWarmNode
            );
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

        /**
         * Validates the node configuration against required criteria.
         * This method first ensures initialization is complete, then checks if the node
         * is a dedicated data node with remote store enabled.
         *
         * @return ValidationResult with true if all configuration requirements are met,
         *         false otherwise. If validation fails, the associated task is closed.
         */
        @Override
        public ValidationResult validate() {
            initializeIfNeeded();
            if (!isRemoteStoreEnabled) {
                logger.info("Domain configuration is not meeting the criteria. Closing task.");
                task.close();
                return new ValidationResult(false);
            }
            if (!isOnlyDataNode) {
                logger.info("Node configuration doesn't meet requirements. Closing task.");
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
         *
         * @return true if remote store is enabled in cluster settings,
         *         false otherwise. Defaults to false if setting is not present.
         */
        private boolean isRemoteStorageEnabled() {
            Settings clusterSettings = clusterService.getSettings();
            return clusterSettings.getAsBoolean("cluster.remote_store.state.enabled", false);
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
            if (cpuPercent >= cpuThreshold) {
                logger.info("CPU usage too high: {}%", cpuPercent);
                return new ValidationResult(false);
            }
            double jvmUsedPercent = jvmService.stats().getMem().getHeapUsedPercent();
            if (jvmUsedPercent >= jvmThreshold) {
                logger.info("JVM memory usage too high: {}%", jvmUsedPercent);
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
                if (stats.getName().equals(ThreadPool.Names.FORCE_MERGE))
                    return stats.getActive() >= forceMergeThreadCount;
            }
            return false;
        }
    }

    /**
     * Validates shard-level conditions for force merge operations.
     * This validator checks segment count and translog age to determine
     * if a specific shard is eligible for force merge.
     */
    protected class ShardValidator implements ValidationStrategy {

        private IndexShard shard;
        private SegmentsStats segmentsStats;
        private TranslogStats translogStats;

        void setShard(IndexShard shard) {
            this.shard = shard;
            CommonStatsFlags flags = new CommonStatsFlags(CommonStatsFlags.Flag.Segments, CommonStatsFlags.Flag.Translog);
            CommonStats stats = new CommonStats(indicesService.getIndicesQueryCache(), shard, flags);
            this.segmentsStats = stats.getSegments();
            this.translogStats = stats.getTranslog();
        }

        @Override
        public ValidationResult validate() {
            if (shard == null) {
                logger.info("No shard found.");
                return new ValidationResult(false);
            }
            if (segmentsStats.getCount() <= segmentCountThreshold) {
                logger.info("Shard {} doesn't have enough segments to merge.", shard.shardId());
                return new ValidationResult(false);
            }
            if (translogStats.getEarliestLastModifiedAge() < schedulerFrequency.getMillis()) {
                logger.info("Shard {} translog is too recent.", shard.shardId());
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
        ValidationResult validate();
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
            super(logger, threadPool, schedulerFrequency, true);
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
         *
         * @return the name of the force merge thread pool
         */
        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.FORCE_MERGE;
        }
    }
}

