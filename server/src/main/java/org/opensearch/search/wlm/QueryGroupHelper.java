/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.wlm;

import org.opensearch.search.ResourceType;
import org.opensearch.tasks.Task;

import java.util.Map;
import java.util.function.Function;

/**
 * Helper class for calculating resource usage for different resource types.
 */
public class QueryGroupHelper {

    /**
     * A map that associates each {@link ResourceType} with a function that calculates the resource usage for a given {@link Task}.
     */
    private static final Map<ResourceType, Function<Task, Long>> resourceUsageCalculator = Map.of(
        ResourceType.MEMORY,
        (task) -> task.getTotalResourceStats().getMemoryInBytes(),
        ResourceType.CPU,
        (task) -> task.getTotalResourceStats().getCpuTimeInNanos()
    );

    /**
     * Gets the resource usage for a given resource type and task.
     *
     * @param resource the resource type
     * @param task the task for which to calculate resource usage
     * @return the resource usage
     */
    public static long getResourceUsage(ResourceType resource, Task task) {
        return resourceUsageCalculator.get(resource).apply(task);
    }
}
