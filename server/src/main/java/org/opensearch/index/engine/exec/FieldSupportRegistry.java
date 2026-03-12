/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Central registry tracking which data formats have which capabilities for which field types.
 * Keyed by fieldTypeName → DataFormat → Set&lt;FieldCapability&gt;.
 */
@ExperimentalApi
public class FieldSupportRegistry {

    private final Map<String, Map<DataFormat, Set<FieldCapability>>> registry = new HashMap<>();

    /**
     * Registers capabilities for a field type and data format.
     * Multiple calls for the same (fieldType, format) pair merge capabilities.
     */
    public void register(String fieldTypeName, DataFormat format, Set<FieldCapability> capabilities) {
        registry.computeIfAbsent(fieldTypeName, k -> new HashMap<>())
            .merge(format, EnumSet.copyOf(capabilities), (existing, incoming) -> {
                existing.addAll(incoming);
                return existing;
            });
    }

    /**
     * Returns the set of capabilities a format has for a field type, or empty set if none.
     */
    public Set<FieldCapability> getCapabilities(String fieldTypeName, DataFormat format) {
        Map<DataFormat, Set<FieldCapability>> formatMap = registry.get(fieldTypeName);
        if (formatMap == null) {
            return Collections.emptySet();
        }
        Set<FieldCapability> caps = formatMap.get(format);
        return caps != null ? Collections.unmodifiableSet(caps) : Collections.emptySet();
    }

    /**
     * Returns true if the format has at least one capability for the field type.
     */
    public boolean hasAnyCapability(String fieldTypeName, DataFormat format) {
        return !getCapabilities(fieldTypeName, format).isEmpty();
    }

    /**
     * Returns true if the format has a specific capability for the field type.
     */
    public boolean hasCapability(String fieldTypeName, DataFormat format, FieldCapability capability) {
        return getCapabilities(fieldTypeName, format).contains(capability);
    }

    /**
     * Returns all field type names a format has any capabilities for.
     */
    public Set<String> supportedFieldTypes(DataFormat format) {
        return registry.entrySet()
            .stream()
            .filter(e -> e.getValue().containsKey(format))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    /**
     * Returns all data formats registered in this registry.
     */
    public Set<DataFormat> allFormats() {
        return registry.values().stream().flatMap(m -> m.keySet().stream()).collect(Collectors.toSet());
    }
}
