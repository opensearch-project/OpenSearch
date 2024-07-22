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
 * Represents the Memory resource type for tasks.
 * This class provides methods to retrieve Memory usage and convert threshold percentages to long values.
 */
public class Memory extends ResourceType {
    /**
     * Returns the Memory usage of the provided task.
     *
     * @param task The task whose Memory usage is to be returned
     * @return The Memory usage of the task
     */
    @Override
    public long getResourceUsage(Task task) {
        return task.getTotalResourceStats().getMemoryInBytes();
    }

    /**
     * Returns the name of the resource type.
     *
     * @return The name of the resource type, which is "Memory"
     */
    @Override
    public String getName() {
        return "memory";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Memory;
    }

    @Override
    public int hashCode() {
        return "Memory".hashCode();
    }
}
