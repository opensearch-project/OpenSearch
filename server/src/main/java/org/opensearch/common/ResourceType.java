/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Enum to specify the system level resources
 */
@ExperimentalApi
public enum ResourceType {
    CPU("cpu"),
    HEAP_ALLOCATIONS("heap_allocations");

    final String name;

    ResourceType(String s) {
        this.name = s;
    }

    public static ResourceType fromName(String s) {
        for (ResourceType resourceType : values()) {
            if (resourceType.getName().equals(s)) {
                return resourceType;
            }
        }
        throw new IllegalArgumentException(s + " is not a valid ResourceType");
    }

    public String getName() {
        return name;
    }

    public static void writeTo(StreamOutput out, ResourceType val) throws IOException {
        out.writeString(val.getName());
    }
}
