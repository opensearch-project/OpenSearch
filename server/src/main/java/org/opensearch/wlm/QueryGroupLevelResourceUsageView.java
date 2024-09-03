/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;


import org.opensearch.wlm.tracker.QueryGroupResourceUsage;

import java.util.List;
import java.util.Map;

/**
 * Represents the point in time view of resource usage of a QueryGroup and
 * has a 1:1 relation with a QueryGroup.
 * This class holds the resource usage data and the list of active tasks.
 */
public class QueryGroupLevelResourceUsageView {
    // resourceUsage holds the resource usage data for a QueryGroup at a point in time
    private final Map<ResourceType, QueryGroupResourceUsage> resourceUsage;
    // activeTasks holds the list of active tasks for a QueryGroup at a point in time
    private final List<QueryGroupTask> activeTasks;

    public QueryGroupLevelResourceUsageView(Map<ResourceType, QueryGroupResourceUsage> resourceUsage, List<QueryGroupTask> activeTasks) {
        this.resourceUsage = resourceUsage;
        this.activeTasks = activeTasks;
    }

    /**
     * Returns the resource usage data.
     *
     * @return The map of resource usage data
     */
    public Map<ResourceType, QueryGroupResourceUsage> getResourceUsageData() {
        return resourceUsage;
    }

    /**
     * Returns the list of active tasks.
     *
     * @return The list of active tasks
     */
    public List<QueryGroupTask> getActiveTasks() {
        return activeTasks;
    }
}
