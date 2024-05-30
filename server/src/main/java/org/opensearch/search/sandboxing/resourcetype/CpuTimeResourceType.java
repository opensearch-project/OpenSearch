/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.resourcetype;

import org.opensearch.tasks.Task;

/**
 * Represents the CPU time resource type.
 */
public class CpuTimeResourceType extends SandboxResourceType {
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

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CpuTimeResourceType;
    }

    @Override
    public int hashCode() {
        return "CPU".hashCode();
    }
}
