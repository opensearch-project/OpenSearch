/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Enum to hold all multitenant labels in workloads
 */
public enum MultiTenantLabel implements Writeable {
    // This label is basically used to define tenancy for multiple features e,g; Query Sandboxing, Query Insights
    TENANT("tenant");

    private final String value;

    MultiTenantLabel(String name) {
        this.value = name;
    }

    public String getValue() {
        return value;
    }

    public static MultiTenantLabel fromName(String name) {
        for (MultiTenantLabel label : values()) {
            if (label.getValue().equalsIgnoreCase(name)) {
                return label;
            }
        }
        throw new IllegalArgumentException("Illegal name + " + name);
    }

    public static MultiTenantLabel fromName(StreamInput in) throws IOException {
        return fromName(in.readString());
    }

    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param out
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(value);
    }
}
