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
import org.opensearch.action.search.SearchTask;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.tasks.Task;

import java.time.Instant;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.Consumer;

/**
 * A simple listener that logs resource information of high memory consuming search tasks
 */
public class TopNSearchTasksLogger implements Consumer<Task> {
    public static final String TASK_DETAILS_LOG_PREFIX = "task.detailslog";
    public static final String LOG_TOP_QUERIES_SIZE = "cluster.task.consumers.topn.size";
    public static final String LOG_TOP_QUERIES_FREQUENCY = "cluster.task.consumers.topn.frequency";

    private static final Logger SEARCH_TASK_DETAILS_LOGGER = LogManager.getLogger(TASK_DETAILS_LOG_PREFIX + ".search");

    // By default, logs top 10 memory expensive search request in the last 60 sec.
    private static final Setting<Integer> LOG_TOP_QUERIES_SIZE_SETTINGS = Setting.intSetting(
        LOG_TOP_QUERIES_SIZE,
        10,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Setting<Long> LOG_TOP_QUERIES_FREQUENCY_SETTINGS = Setting.longSetting(
        LOG_TOP_QUERIES_FREQUENCY,
        60_000L,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final int topQueriesSize;
    private final long topQueriesLogFrequencyInMillis;
    private final Queue<Tuple<Long, Task>> topQueries;
    private long lastReportedTimeInMillis = Instant.now().toEpochMilli();

    public TopNSearchTasksLogger(Settings settings) {
        this.topQueriesSize = LOG_TOP_QUERIES_SIZE_SETTINGS.get(settings);
        this.topQueriesLogFrequencyInMillis = LOG_TOP_QUERIES_FREQUENCY_SETTINGS.get(settings);
        this.topQueries = new PriorityQueue<>(topQueriesSize, Comparator.comparingLong(Tuple::v1));
        Loggers.setLevel(SEARCH_TASK_DETAILS_LOGGER, "info");
    }

    /**
     * Called when task is unregistered and task has resource stats present.
     */
    @Override
    public void accept(Task task) {
        if (task instanceof SearchShardTask | task instanceof SearchTask) {
            recordSearchTask(task);
        }
    }

    // TODO: Need performance testing results to understand if we can to use synchronized here.
    private synchronized void recordSearchTask(final Task searchTask) {
        final long memory_in_bytes = searchTask.getResourceStats().get("memory");
        if (Instant.now().toEpochMilli() - lastReportedTimeInMillis > topQueriesLogFrequencyInMillis) {
            logTopResourceConsumingQueries();
            lastReportedTimeInMillis = Instant.now().toEpochMilli();
        }
        if (topQueries.size() >= topQueriesSize && topQueries.peek().v1() < memory_in_bytes) {
            // evict the element
            topQueries.poll();
        }
        if (topQueries.size() < topQueriesSize) {
            topQueries.offer(new Tuple<>(memory_in_bytes, searchTask));
        }
    }

    private void logTopResourceConsumingQueries() {
        for (Tuple<Long, Task> topQuery : topQueries) {
            SEARCH_TASK_DETAILS_LOGGER.info(new TaskDetailsLogMessage(topQuery.v2()));
        }
        topQueries.clear();
    }
}
