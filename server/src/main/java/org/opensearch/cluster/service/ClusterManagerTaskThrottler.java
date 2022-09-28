/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * This class does throttling on task submission to cluster manager node, it uses throttling key defined in various executors
 * as key for throttling. Throttling will be performed over task executor's class level, different task types have different executors class.
 *
 * Set specific setting to for setting the threshold of throttling of particular task type.
 * e.g : Set "cluster_manager.throttling.thresholds.put_mapping" to set throttling limit of "put mapping" tasks,
 *       Set it to default value(-1) to disable the throttling for this task type.
 */
public class ClusterManagerTaskThrottler implements TaskBatcherListener {
    private static final Logger logger = LogManager.getLogger(ClusterManagerTaskThrottler.class);

    public static final Setting<Settings> THRESHOLD_SETTINGS = Setting.groupSetting(
        "cluster_manager.throttling.thresholds.",
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static Map<String, Boolean> THROTTLING_TASK_KEYS = new ConcurrentHashMap<>();

    /**
     * To configure more task for throttling, override getClusterManagerThrottlingKey method with task name in task executor.
     * Verify that throttled tasks would be retry.
     *
     * Added retry mechanism in TransportClusterManagerNodeAction so it would be retried for customer generated tasks.
     */
    public static Set<String> CONFIGURED_TASK_FOR_THROTTLING = Collections.unmodifiableSet(
        new HashSet<>(
            Arrays.asList(
                "update-settings",
                "cluster-update-settings",
                "create-index",
                "auto-create",
                "delete-index",
                "delete-dangling-index",
                "create-data-stream",
                "remove-data-stream",
                "rollover-index",
                "index-aliases",
                "put-mapping",
                "create-index-template",
                "remove-index-template",
                "create-component-template",
                "remove-component-template",
                "create-index-template-v2",
                "remove-index-template-v2",
                "put-pipeline",
                "delete-pipeline",
                "create-persistent-task",
                "finish-persistent-task",
                "remove-persistent-task",
                "update-task-state",
                "put-script",
                "delete-script",
                "put_repository",
                "delete_repository",
                "create-snapshot",
                "delete-snapshot",
                "update-snapshot-state",
                "restore_snapshot",
                "cluster-reroute-api"
            )
        )
    );
    private final int MIN_THRESHOLD_VALUE = -1; // Disabled throttling
    private final ClusterManagerTaskThrottlerListener clusterManagerTaskThrottlerListener;

    private final ConcurrentMap<String, Long> tasksCount;
    private final ConcurrentMap<String, Long> tasksThreshold;
    private final Supplier<Version> minNodeVersionSupplier;

    public ClusterManagerTaskThrottler(
        final ClusterSettings clusterSettings,
        final Supplier<Version> minNodeVersionSupplier,
        final ClusterManagerTaskThrottlerListener clusterManagerTaskThrottlerListener
    ) {
        clusterSettings.addSettingsUpdateConsumer(THRESHOLD_SETTINGS, this::updateSetting, this::validateSetting);
        this.minNodeVersionSupplier = minNodeVersionSupplier;
        this.clusterManagerTaskThrottlerListener = clusterManagerTaskThrottlerListener;
        tasksCount = new ConcurrentHashMap<>(128); // setting initial capacity so each task will land in different segment
        tasksThreshold = new ConcurrentHashMap<>(128); // setting initial capacity so each task will land in different segment
    }

    // need to validate if same key is not mapped against multiple actions.
    protected void registerThrottlingKey(String throttlingKey, boolean retryableOnDataNode) {
        if (THROTTLING_TASK_KEYS.containsKey(throttlingKey)) {
            throw new IllegalArgumentException("Duplicate throttling keys are configured ");
        }
        THROTTLING_TASK_KEYS.put(throttlingKey, retryableOnDataNode);
    }

    void validateSetting(final Settings settings) {
        /**
         * TODO: Change the version number of check as per version in which this change will be merged.
         */
        if (minNodeVersionSupplier.get().compareTo(Version.V_3_0_0) < 0) {
            throw new IllegalArgumentException("All the nodes in cluster should be on version later than or equal to 3.0.0");
        }
        Map<String, Settings> groups = settings.getAsGroups();
        for (String key : groups.keySet()) {
            if (!THROTTLING_TASK_KEYS.containsKey(key)) {
                throw new IllegalArgumentException("Cluster manager task throttling is not configured for given task type: " + key);
            }
            if (!THROTTLING_TASK_KEYS.get(key)) {
                throw new IllegalArgumentException(
                    "Data node doesn't perform retries for Cluster manager task throttling for given task type: " + key
                );
            }
            int threshold = groups.get(key).getAsInt("value", MIN_THRESHOLD_VALUE);
            if (threshold < MIN_THRESHOLD_VALUE) {
                throw new IllegalArgumentException("Provide positive integer for limit or -1 for disabling throttling");
            }
        }
    }

    void updateSetting(final Settings settings) {
        Map<String, Settings> groups = settings.getAsGroups();
        for (String key : groups.keySet()) {
            updateLimit(key, groups.get(key).getAsInt("value", MIN_THRESHOLD_VALUE));
        }
    }

    void updateLimit(final String taskKey, final int limit) {
        assert limit >= MIN_THRESHOLD_VALUE;
        if (limit == MIN_THRESHOLD_VALUE) {
            tasksThreshold.remove(taskKey);
        } else {
            tasksThreshold.put(taskKey, (long) limit);
        }
    }

    Long getThrottlingLimit(final String taskKey) {
        return tasksThreshold.get(taskKey);
    }

    @Override
    public void onBeginSubmit(List<? extends TaskBatcher.BatchedTask> tasks) {
        String clusterManagerThrottlingKey = ((ClusterStateTaskExecutor<Object>) tasks.get(0).batchingKey).getClusterManagerThrottlingKey();
        tasksCount.putIfAbsent(clusterManagerThrottlingKey, 0L);
        tasksCount.computeIfPresent(clusterManagerThrottlingKey, (key, count) -> {
            int size = tasks.size();
            Long threshold = tasksThreshold.get(clusterManagerThrottlingKey);
            if (threshold != null && (count + size > threshold)) {
                clusterManagerTaskThrottlerListener.onThrottle(clusterManagerThrottlingKey, size);
                logger.warn(
                    "Throwing Throttling Exception for [{}]. Trying to add [{}] tasks to queue, limit is set to [{}]",
                    clusterManagerThrottlingKey,
                    tasks.size(),
                    threshold
                );
                throw new ClusterManagerThrottlingException("Throttling Exception : Limit exceeded for " + clusterManagerThrottlingKey);
            }
            return count + size;
        });
    }

    @Override
    public void onSubmitFailure(List<? extends TaskBatcher.BatchedTask> tasks) {
        reduceTaskCount(tasks);
    }

    /**
     * Tasks will be removed from the queue before processing, so here we will reduce the count of tasks.
     *
     * @param tasks list of tasks which will be executed.
     */
    @Override
    public void onBeginProcessing(List<? extends TaskBatcher.BatchedTask> tasks) {
        reduceTaskCount(tasks);
    }

    @Override
    public void onTimeout(List<? extends TaskBatcher.BatchedTask> tasks) {
        reduceTaskCount(tasks);
    }

    private void reduceTaskCount(List<? extends TaskBatcher.BatchedTask> tasks) {
        String masterTaskKey = ((ClusterStateTaskExecutor<Object>) tasks.get(0).batchingKey).getClusterManagerThrottlingKey();
        tasksCount.computeIfPresent(masterTaskKey, (key, count) -> count - tasks.size());
    }
}
