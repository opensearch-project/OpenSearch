/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks.consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.tasks.ResourceStats;
import org.opensearch.tasks.Task;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.Consumer;

/**
 * A simple listener that logs resource information of high memory consuming search tasks
 *
 * @opensearch.internal
 */
public class TopNSearchTasksLogger implements Consumer<Task> {
    public static final String TASK_DETAILS_LOG_PREFIX = "task.detailslog";
    public static final String LOG_TOP_QUERIES_SIZE = "cluster.task.consumers.top_n.size";
    public static final String LOG_TOP_QUERIES_FREQUENCY = "cluster.task.consumers.top_n.frequency";

    private static final Logger SEARCH_TASK_DETAILS_LOGGER = LogManager.getLogger(TASK_DETAILS_LOG_PREFIX + ".search");

    // number of memory expensive search tasks that are logged
    private static final Setting<Integer> LOG_TOP_QUERIES_SIZE_SETTING = Setting.intSetting(
        LOG_TOP_QUERIES_SIZE,
        10,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    // frequency in which memory expensive search tasks are logged
    private static final Setting<TimeValue> LOG_TOP_QUERIES_FREQUENCY_SETTING = Setting.timeSetting(
        LOG_TOP_QUERIES_FREQUENCY,
        TimeValue.timeValueSeconds(60L),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final int topQueriesSize;
    private final long topQueriesLogFrequencyInNanos;
    private final Queue<Tuple<Long, SearchShardTask>> topQueries;
    private long lastReportedTimeInNanos = System.nanoTime();

    public TopNSearchTasksLogger(Settings settings) {
        this.topQueriesSize = LOG_TOP_QUERIES_SIZE_SETTING.get(settings);
        this.topQueriesLogFrequencyInNanos = LOG_TOP_QUERIES_FREQUENCY_SETTING.get(settings).getNanos();
        this.topQueries = new PriorityQueue<>(topQueriesSize, Comparator.comparingLong(Tuple::v1));
    }

    /**
     * Called when task is unregistered and task has resource stats present.
     */
    @Override
    public void accept(Task task) {
        if (task instanceof SearchShardTask) {
            recordSearchTask((SearchShardTask) task);
        }
    }

    private synchronized void recordSearchTask(SearchShardTask searchTask) {
        final long memory_in_bytes = searchTask.getTotalResourceUtilization(ResourceStats.MEMORY);
        if (System.nanoTime() - lastReportedTimeInNanos >= topQueriesLogFrequencyInNanos) {
            publishTopNEvents();
            lastReportedTimeInNanos = System.nanoTime();
        }
        if (topQueries.size() >= topQueriesSize && topQueries.peek().v1() < memory_in_bytes) {
            // evict the element
            topQueries.poll();
        }
        if (topQueries.size() < topQueriesSize) {
            topQueries.offer(new Tuple<>(memory_in_bytes, searchTask));
        }
    }

    private void publishTopNEvents() {
        logTopResourceConsumingQueries();
        topQueries.clear();
    }

    private void logTopResourceConsumingQueries() {
        for (Tuple<Long, SearchShardTask> topQuery : topQueries) {
            SEARCH_TASK_DETAILS_LOGGER.info(new SearchShardTaskDetailsLogMessage(topQuery.v2()));
        }
    }
}
