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
 * This class is extension of {@link Throttler} and does throttling of master tasks.
 *
 * This class does throttling on task submission to master, it uses class name of request of tasks as key for
 * throttling. Throttling will be performed over task executor's class level, different task types have different executors class.
 *
 * Set specific setting to for setting the threshold of throttling of particular task type.
 * e.g : Set "master.throttling.thresholds.put_mapping" to set throttling limit of "put mapping" tasks,
 *       Set it to default value(-1) to disable the throttling for this task type.
 */
public class MasterTaskThrottler implements TaskBatcherListener {
    private static final Logger logger = LogManager.getLogger(MasterTaskThrottler.class);

    public static final Setting<Settings> THRESHOLD_SETTINGS = Setting.groupSetting(
        "master.throttling.thresholds.",
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * To configure more task for throttling, override getMasterThrottlingKey method with task name in task executor.
     * Verify that throttled tasks would be retry.
     *
     * Added retry mechanism in TransportMasterNodeAction so it would be retried for customer generated tasks.
     */
    public static Set<String> CONFIGURED_TASK_FOR_THROTTLING = Collections.unmodifiableSet(
        // TODO
        // Add throttling key for other master task as well.
        // Will be added as part of upcoming PRs,
        // Right now just added "put-mapping" for ref only.
        new HashSet<>(Arrays.asList("put-mapping"))
    );

    private final int MIN_THRESHOLD_VALUE = -1; // Disabled throttling
    private final MasterTaskThrottlerListener masterTaskThrottlerListener;

    private final ConcurrentMap<String, Long> tasksCount;
    private final ConcurrentMap<String, Long> tasksThreshold;
    private final Supplier<Version> minNodeVersionSupplier;

    public MasterTaskThrottler(
        final ClusterSettings clusterSettings,
        final Supplier<Version> minNodeVersionSupplier,
        final MasterTaskThrottlerListener masterTaskThrottlerListener
    ) {
        clusterSettings.addSettingsUpdateConsumer(THRESHOLD_SETTINGS, this::updateSetting, this::validateSetting);
        this.minNodeVersionSupplier = minNodeVersionSupplier;
        this.masterTaskThrottlerListener = masterTaskThrottlerListener;
        tasksCount = new ConcurrentHashMap<>(128); // setting initial capacity so each task will land in different segment
        tasksThreshold = new ConcurrentHashMap<>(128); // setting initial capacity so each task will land in different segment
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
            if (!CONFIGURED_TASK_FOR_THROTTLING.contains(key)) {
                throw new IllegalArgumentException("Master task throttling is not configured for given task type: " + key);
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
        String masterTaskKey = ((ClusterStateTaskExecutor<Object>) tasks.get(0).batchingKey).getMasterThrottlingKey();
        tasksCount.putIfAbsent(masterTaskKey, 0L);
        tasksCount.computeIfPresent(masterTaskKey, (key, count) -> {
            int size = tasks.size();
            Long threshold = tasksThreshold.get(masterTaskKey);
            if (threshold != null && (count + size > threshold)) {
                masterTaskThrottlerListener.onThrottle(masterTaskKey, size);
                logger.warn(
                    "Throwing Throttling Exception for [{}]. Trying to add [{}] tasks to queue, limit is set to [{}]",
                    masterTaskKey,
                    tasks.size(),
                    threshold
                );
                throw new MasterTaskThrottlingException("Throttling Exception : Limit exceeded for " + masterTaskKey);
            }
            return count + size;
        });
    }

    @Override
    public void onSubmitFailure(List<? extends TaskBatcher.BatchedTask> tasks) {
        reduceTaskCount(tasks);
    }

    @Override
    public void onProcessed(List<? extends TaskBatcher.BatchedTask> tasks) {
        reduceTaskCount(tasks);
    }

    private void reduceTaskCount(List<? extends TaskBatcher.BatchedTask> tasks) {
        String masterTaskKey = ((ClusterStateTaskExecutor<Object>) tasks.get(0).batchingKey).getMasterThrottlingKey();
        tasksCount.computeIfPresent(masterTaskKey, (key, count) -> count - tasks.size());
    }
}
