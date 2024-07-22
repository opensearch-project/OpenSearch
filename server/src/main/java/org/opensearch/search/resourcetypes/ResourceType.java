/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resourcetypes;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.tasks.Task;

import java.io.IOException;

/**
 * Abstract class representing a resource type.
 */
@PublicApi(since = "2.x")
public abstract class ResourceType {
    public static ResourceType[] values() {
        return new ResourceType[] { new CPU(), new Memory() };
    }

    /**
     * Returns the resource usage of the provided task.
     * The specific resource that this method returns depends on the implementation.
     *
     * @param task The task whose resource usage is to be returned
     * @return The resource usage of the task
     */
    public abstract long getResourceUsage(Task task);

    /**
     * Creates a SystemResource from a string.
     * If the string is "Memory", a Memory is returned.
     * If the string is "CPU", a CPU is returned.
     * If the string is not recognized, an IllegalArgumentException is thrown.
     *
     * @param type The string from which to create a SystemResource
     * @return The created SystemResource
     * @throws IllegalArgumentException If the string is not recognized
     */
    public static ResourceType fromName(String type) {
        if (type.equalsIgnoreCase("JVM") || type.equalsIgnoreCase("memory")) {
            return new Memory();
        } else if (type.equalsIgnoreCase("CPU")) {
            return new CPU();
        } else {
            throw new IllegalArgumentException("Unsupported resource type: " + type);
        }
    }

    /**
     * Returns the name of the resource type.
     *
     * @return The name of the resource type
     */
    public abstract String getName();

    public static void writeTo(StreamOutput out, ResourceType resourceType) throws IOException {
        out.writeString(resourceType.getName());
    }
}
