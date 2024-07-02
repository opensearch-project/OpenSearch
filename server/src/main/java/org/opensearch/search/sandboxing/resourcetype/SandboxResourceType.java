/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.resourcetype;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.tasks.Task;

/**
 * Represents a type of resource that can be used in a sandbox.
 * This class is abstract and requires the implementation of the getResourceUsage method.
 */
@ExperimentalApi
public abstract class SandboxResourceType {
    /**
     * Returns the resource usage of the provided task.
     * The specific resource that this method returns depends on the implementation.
     *
     * @param task The task whose resource usage is to be returned
     * @return The resource usage of the task
     */
    public abstract long getResourceUsage(Task task);

    /**
     * Creates a SandboxResourceType from a string.
     * If the string is "JVM", a JvmMemoryResourceType is returned.
     * If the string is "CPU", a CpuTimeResourceType is returned.
     * If the string is not recognized, an IllegalArgumentException is thrown.
     *
     * @param type The string from which to create a SandboxResourceType
     * @return The created SandboxResourceType
     * @throws IllegalArgumentException If the string is not recognized
     */
    public static SandboxResourceType fromString(String type) {
        if (type.equalsIgnoreCase("JVM")) {
            return new JvmMemoryResourceType();
        } else if (type.equalsIgnoreCase("CPU")) {
            return new CpuTimeResourceType();
        } else {
            throw new IllegalArgumentException("Unsupported resource type: " + type);
        }
    }
}
