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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Extensibility support for Named Writeable Registry: response from extensions for name writeable registry entries
 *
 * @opensearch.internal
 */
public class NamedWriteableRegistryResponse extends TransportResponse {

    private final Map<String, String> registry;

    public NamedWriteableRegistryResponse(Map<String, String> registry) {
        this.registry = registry;
    }

    public NamedWriteableRegistryResponse(StreamInput in) throws IOException {

        Map<String, String> registry = new HashMap<>();

        // stream output for registry map begins with a variable integer that tells us the number of entries being sent across the wire
        int registryEntryCount = in.readVInt();
        for (int i = 0; i < registryEntryCount; i++) {
            String name = in.readString();
            String categoryClassName = in.readString();
            registry.put(name, categoryClassName);
        }

        this.registry = registry;
    }

    public Map<String, String> getRegistry() {
        return this.registry;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

        // stream out registry size prior to streaming out registry entries
        out.writeVInt(this.registry.size());
        for (Map.Entry<String, String> entry : registry.entrySet()) {
            out.writeString(entry.getKey());   // unique named writeable name
            out.writeString(entry.getValue()); // fully qualified category class name
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

}
