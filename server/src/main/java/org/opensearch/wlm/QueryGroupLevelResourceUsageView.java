/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.ResourceType;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the point in time view of resource usage of a QueryGroup and
 * has a 1:1 relation with a QueryGroup.
 * This class holds the QueryGroup ID, the resource usage data, and the list of active tasks.
 */
public class QueryGroupLevelResourceUsageView {

    private final String queryGroupId;
    // resourceUsage holds the resource usage data for a QueryGroup at a point in time
    private final Map<ResourceType, Long> resourceUsage;
    // activeTasks holds the list of active tasks for a QueryGroup at a point in time
    private final List<Task> activeTasks;

    public QueryGroupLevelResourceUsageView(String queryGroupId) {
        this.queryGroupId = queryGroupId;
        this.resourceUsage = new HashMap<>();
        this.activeTasks = new ArrayList<>();
    }

    public QueryGroupLevelResourceUsageView(String queryGroupId, Map<ResourceType, Long> resourceUsage, List<Task> activeTasks) {
        this.queryGroupId = queryGroupId;
        this.resourceUsage = resourceUsage;
        this.activeTasks = activeTasks;
    }

    /**
     * Returns the resource usage data.
     *
     * @return The map of resource usage data
     */
    public Map<ResourceType, Long> getResourceUsageData() {
        return resourceUsage;
    }

    /**
     * Returns the list of active tasks.
     *
     * @return The list of active tasks
     */
    public List<Task> getActiveTasks() {
        return activeTasks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryGroupLevelResourceUsageView that = (QueryGroupLevelResourceUsageView) o;
        return Objects.equals(queryGroupId, that.queryGroupId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(queryGroupId);
    }
}
