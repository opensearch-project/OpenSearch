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

public class TaskCompleteResourceInfo {
    private final Map<TaskStatsType, TaskResourceInfo> resourceInfo = new HashMap<>();
    private boolean isActive;

    public TaskCompleteResourceInfo(boolean isActive, TaskStatsType statsType, TaskStatsInfo... taskStatsInfos) {
        this.isActive = isActive;
        this.resourceInfo.put(statsType, new TaskResourceInfo(taskStatsInfos));
    }

    public boolean isActive() {
        return isActive;
    }

    public Map<TaskStatsType, TaskResourceInfo> getResourceInfo() {
        return resourceInfo;
    }

    public void setActive(boolean isActive) {
        this.isActive = isActive;
    }

    public void update(boolean isActive, TaskStatsType statsName, TaskStatsInfo... taskResourceMetrics) {
        TaskResourceInfo resourceStats = resourceInfo.get(statsName);
        if (resourceStats == null) {
            resourceInfo.put(statsName, new TaskResourceInfo(taskResourceMetrics));
        } else {
            resourceStats.updateStatsInfo(taskResourceMetrics);
        }
        setActive(isActive);
    }

    @Override
    public String toString() {
        return resourceInfo + ", is_active=" + isActive;
    }
}
