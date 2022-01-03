/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import java.util.HashMap;
import java.util.Map;

public class TaskResourceInfo {
    private final Map<TaskStats, TaskStatsInfo> statsInfo = new HashMap<>();

    public TaskResourceInfo(TaskStatsInfo... statsInfo) {
        for (TaskStatsInfo taskStats : statsInfo) {
            this.statsInfo.put(taskStats.getStats(), taskStats);
        }
    }

    public Map<TaskStats, TaskStatsInfo> getStatsInfo() {
        return statsInfo;
    }

    public void updateStatsInfo(TaskStatsInfo... taskStatsInfos) {
        for (TaskStatsInfo taskStatsInfo : taskStatsInfos) {
            statsInfo.put(taskStatsInfo.getStats(), taskStatsInfo);
        }
    }

    @Override
    public String toString() {
        return statsInfo.toString();
    }
}
