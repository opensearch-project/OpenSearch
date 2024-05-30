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

@ExperimentalApi
public abstract class SandboxResourceType {
    public abstract long getResourceUsage(Task task);

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
