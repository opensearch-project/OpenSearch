/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import java.util.List;
import java.util.Map;

/**
 * Represents the point in time view of resource usage of a WorkloadGroup and
 * has a 1:1 relation with a WorkloadGroup.
 * This class holds the resource usage data and the list of active tasks.
 */
public class WorkloadGroupLevelResourceUsageView {
    // resourceUsage holds the resource usage data for a WorkloadGroup at a point in time
    private final Map<ResourceType, Double> resourceUsage;
    // activeTasks holds the list of active tasks for a WorkloadGroup at a point in time
    private final List<WorkloadGroupTask> activeTasks;

    public WorkloadGroupLevelResourceUsageView(Map<ResourceType, Double> resourceUsage, List<WorkloadGroupTask> activeTasks) {
        this.resourceUsage = resourceUsage;
        this.activeTasks = activeTasks;
    }

    /**
     * Returns the resource usage data.
     *
     * @return The map of resource usage data
     */
    public Map<ResourceType, Double> getResourceUsageData() {
        return resourceUsage;
    }

    /**
     * Returns the list of active tasks.
     *
     * @return The list of active tasks
     */
    public List<WorkloadGroupTask> getActiveTasks() {
        return activeTasks;
    }
}
