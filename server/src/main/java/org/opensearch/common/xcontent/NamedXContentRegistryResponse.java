/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Extensibility support for Named XContent Registry : response from extensions for named xcontent registry entries
 *
 * @opensearch.internal
 */

public class NamedXContentRegistryResponse extends TransportResponse {

    private final Map<ParseField, Class> registry;

    /**
     * @param registry Map of ParseFields and their associated category class
     */
    public NamedXContentRegistryResponse(Map<ParseField, Class> registry) {
        this.registry = new HashMap<>(registry);
    }

    /**
     * @param in StreamInput from which map entries of writeable names and their associated category classes are read from
     * @throws IllegalArgumentException if the fully qualified class name is invalid and the class object cannot be generated at runtime
     */
    public NamedXContentRegistryResponse(StreamInput in) throws IOException {
        super(in);

        Map<ParseField, Class> registry = new HashMap<>();
        int registryEntryCount = in.readVInt();
        for (int i = 0; i < registryEntryCount; i++) {
            try {
                String name = in.readString();
                String[] deprecatedNames = in.readStringArray();
                ParseField parseField = new ParseField(name, deprecatedNames);
                Class categoryClass = Class.forName(in.readString());
                registry.put(parseField, categoryClass);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Category class definition not found", e);
            }
        }
        this.registry = registry;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

        out.writeVInt(this.registry.size());
        for (Map.Entry<ParseField, Class> entry : registry.entrySet()) {

            ParseField parseField = entry.getKey();
            Class categoryClass = entry.getValue();

            // First write out ParseField metadata then category class
            out.writeString(parseField.getPreferredName());
            out.writeStringArray(parseField.getDeprecatedNames());
            out.writeString(categoryClass.getName());
        }
    }

    @Override
    public String toString() {
        return "NamedXContentRegistryResponse{" + "registry=" + registry.toString() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedXContentRegistryResponse that = (NamedXContentRegistryResponse) o;
        return Objects.equals(registry, that.registry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(registry);
    }

    /**
     * Returns a map of ParseFields and their associated category class
     */
    public Map<ParseField, Class> getRegistry() {
        return Collections.unmodifiableMap(this.registry);
    }

}
