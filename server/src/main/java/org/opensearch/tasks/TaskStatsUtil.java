/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaskStatsUtil {
    private Thread.State threadState;
    private Map<TaskStatsType, TaskStatsInfo> statsInfo;

    public TaskStatsUtil(Thread.State threadState, TaskResourceMetric... taskResourceMetrics) {
        this.threadState = threadState;
        this.statsInfo = new ConcurrentHashMap<TaskStatsType, TaskStatsInfo>() {
            {
                for (TaskResourceMetric taskResourceMetric : taskResourceMetrics) {
                    put(taskResourceMetric.getStatsType(), new TaskStatsInfo(taskResourceMetric.getValue()));
                }
            }
        };
    }

    public Thread.State getThreadState() {
        return threadState;
    }

    public Map<TaskStatsType, TaskStatsInfo> getStatsInfo() {
        return statsInfo;
    }

    public void setThreadState(Thread.State threadState) {
        this.threadState = threadState;
    }

    public void updateStatsInfo(TaskStatsType statsType, long value) {
        TaskStatsInfo taskStatsInfo = statsInfo.get(statsType);
        if (taskStatsInfo == null) {
            statsInfo.put(statsType, new TaskStatsInfo(value));
        } else {
            taskStatsInfo.setEndValue(value);
        }
    }
}
