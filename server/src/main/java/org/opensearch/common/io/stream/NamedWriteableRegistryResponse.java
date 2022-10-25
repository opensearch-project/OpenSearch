/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io.stream;

import org.opensearch.transport.TransportResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Extensibility support for Named Writeable Registry: response from extensions for name writeable registry entries
 *
 * @opensearch.internal
 */
public class NamedWriteableRegistryResponse extends TransportResponse {

    private final Map<String, Class<? extends NamedWriteable>> registry;

    /**
     * @param registry Map of writeable names and their associated category class
     */
    public NamedWriteableRegistryResponse(Map<String, Class<? extends NamedWriteable>> registry) {
        this.registry = new HashMap<>(registry);
    }

    /**
     * @param in StreamInput from which map entries of writeable names and their associated category classes are read from
     * @throws IllegalArgumentException if the fully qualified class name is invalid and the class object cannot be generated at runtime
     */
    @SuppressWarnings("unchecked")
    public NamedWriteableRegistryResponse(StreamInput in) throws IOException {
        super(in);
        // Stream output for registry map begins with a variable integer that tells us the number of entries being sent across the wire
        Map<String, Class<? extends NamedWriteable>> registry = new HashMap<>();
        int registryEntryCount = in.readVInt();
        for (int i = 0; i < registryEntryCount; i++) {
            try {
                String name = in.readString();
                Class<? extends NamedWriteable> categoryClass = (Class<? extends NamedWriteable>) Class.forName(in.readString());
                registry.put(name, categoryClass);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Category class definition not found", e);
            }
        }

        this.registry = registry;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Stream out registry size prior to streaming out registry entries
        out.writeVInt(this.registry.size());
        for (Map.Entry<String, Class<? extends NamedWriteable>> entry : registry.entrySet()) {
            out.writeString(entry.getKey());   // Unique named writeable name
            out.writeString(entry.getValue().getName()); // Fully qualified category class name
        }
    }

    @Override
    public String toString() {
        return "NamedWritableRegistryResponse{" + "registry=" + registry.toString() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedWriteableRegistryResponse that = (NamedWriteableRegistryResponse) o;
        return Objects.equals(registry, that.registry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(registry);
    }

    /**
     * Returns a map of writeable names and their associated category class
     */
    public Map<String, Class<? extends NamedWriteable>> getRegistry() {
        return Collections.unmodifiableMap(this.registry);
    }

}
