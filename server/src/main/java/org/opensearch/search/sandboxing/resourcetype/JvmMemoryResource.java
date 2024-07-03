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
 * Represents the JVM memory resource type.
 */
public class JvmMemoryResource extends SystemResource {
    /**
     * Returns the memory usage of the provided task.
     *
     * @param task The task whose memory usage is to be returned
     * @return The memory usage of the task
     */
    @Override
    public long getResourceUsage(Task task) {
        return task.getTotalResourceStats().getMemoryInBytes();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof JvmMemoryResource;
    }

    @Override
    public int hashCode() {
        return "JVM".hashCode();
    }
}
