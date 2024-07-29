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

public class QueryGroupHelper {

    private static final Map<ResourceType, Function<Task, Long>> resourceUsageCalculator = Map.of(
        ResourceType.MEMORY,
        (task) -> task.getTotalResourceStats().getMemoryInBytes(),
        ResourceType.CPU,
        (task) -> task.getTotalResourceStats().getCpuTimeInNanos()
    );

    public static long getResourceUsage(ResourceType resource, Task task) {
        return resourceUsageCalculator.get(resource).apply(task);
    }
}
