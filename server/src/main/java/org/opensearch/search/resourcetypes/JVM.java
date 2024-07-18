/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resourcetypes;

import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.tasks.Task;

/**
 * Represents the JVM resource type for tasks.
 * This class provides methods to retrieve JVM usage and convert threshold percentages to long values.
 */
public class JVM extends ResourceType {
    /**
     * Returns the JVM usage of the provided task.
     *
     * @param task The task whose JVM usage is to be returned
     * @return The JVM usage of the task
     */
    @Override
    public long getResourceUsage(Task task) {
        return task.getTotalResourceStats().getMemoryInBytes();
    }

    /**
     * Returns the name of the resource type.
     *
     * @return The name of the resource type, which is "JVM"
     */
    @Override
    public String getName() {
        return "JVM";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof JVM;
    }

    @Override
    public int hashCode() {
        return "JVM".hashCode();
    }

    /**
     * Converts the given threshold percentage to a long value that can be compared.
     *
     * @param threshold The threshold percentage to be converted
     * @return The threshold value in bytes
     */
    @Override
    public long convertThresholdPercentageToLong(Double threshold) {
        return (long) (threshold * JvmStats.jvmStats().getMem().getHeapMax().getBytes());
    }
}
