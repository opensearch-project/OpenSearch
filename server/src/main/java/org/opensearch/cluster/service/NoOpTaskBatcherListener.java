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
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * This class does throttling on task submission to cluster manager node, it uses throttling key defined in various executors
 * as key for throttling. Throttling will be performed over task executor's class level, different task types have different executors class.
 * <p>
 * Set specific setting for setting the threshold of throttling of a particular task type.
 * e.g : Set "cluster_manager.throttling.thresholds.put_mapping" to set throttling limit of "put mapping" tasks,
 * <p>
 * Set it to (-1) to disable the throttling for this task type.
 * <p>
 * All tasks must have a default threshold configured in {@link ClusterManagerTask}.
 */
public class NoOpTaskBatcherListener implements TaskBatcherListener {

    /**
     * Callback called before submitting tasks.
     *
     * @param tasks list of tasks which will be submitted.
     */
    @Override
    public void onBeginSubmit(List<? extends TaskBatcher.BatchedTask> tasks) {

    }

    /**
     * Callback called if tasks submission due to any reason
     * for e.g. failing due to duplicate tasks.
     *
     * @param tasks list of tasks which was failed to submit.
     */
    @Override
    public void onSubmitFailure(List<? extends TaskBatcher.BatchedTask> tasks) {

    }

    /**
     * Callback called before processing any tasks.
     *
     * @param tasks list of tasks which will be executed.
     */
    @Override
    public void onBeginProcessing(List<? extends TaskBatcher.BatchedTask> tasks) {

    }

    /**
     * Callback called when tasks are timed out.
     *
     * @param tasks list of tasks which will be executed.
     */
    @Override
    public void onTimeout(List<? extends TaskBatcher.BatchedTask> tasks) {

    }
}
