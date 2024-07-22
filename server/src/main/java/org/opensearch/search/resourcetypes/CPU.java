/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resourcetypes;

import org.opensearch.tasks.Task;

/**
 * Represents the CPU resource type for tasks.
 * This class provides methods to retrieve CPU usage and convert threshold percentages to long values.
 */
public class CPU extends ResourceType {
    /**
     * Returns the CPU time usage of the provided task.
     *
     * @param task The task whose CPU time usage is to be returned
     * @return The CPU time usage of the task
     */
    @Override
    public long getResourceUsage(Task task) {
        return task.getTotalResourceStats().getCpuTimeInNanos();
    }

    /**
     * Returns the name of the resource type.
     *
     * @return The name of the resource type, which is "CPU"
     */
    @Override
    public String getName() {
        return "cpu";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CPU;
    }

    @Override
    public int hashCode() {
        return "CPU".hashCode();
    }
}
