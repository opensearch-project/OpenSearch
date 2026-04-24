/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MapperParsingException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Stateless utility class that encapsulates the capability assignment algorithm.
 * <p>
 * Computes capability maps using {@link DataFormatRegistry#supportsCapability(String, FieldTypeCapabilities.Capability)}
 * and validates that field types are supported by at least one registered data format.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class CapabilityAssigner {

    private CapabilityAssigner() {}

    /**
     * Computes the capability map for a given field type name across the provided data formats,
     * using the {@link DataFormatRegistry} to resolve which formats support each capability.
     * <p>
     * Algorithm:
     * <ol>
     *   <li>For each {@link FieldTypeCapabilities.Capability} enum value, call
     *       {@link DataFormatRegistry#supportsCapability(String, FieldTypeCapabilities.Capability)}</li>
     *   <li>The registry returns formats sorted by priority ascending (lowest = highest priority)</li>
     *   <li>Filter to only formats in the provided {@code formats} set</li>
     *   <li>Take the first match (lowest priority) — it wins that capability</li>
     *   <li>Accumulate into {@code Map<DataFormat, Set<Capability>>}</li>
     *   <li>Return only entries with non-empty capability sets</li>
     * </ol>
     *
     * @param fieldTypeName the field type name (e.g., "keyword", "long")
     * @param registry      the DataFormatRegistry for capability lookups
     * @param formats       the set of DataFormat instances to consider (primary + secondaries)
     * @return an immutable capability map for this field type
     */
    public static Map<DataFormat, Set<FieldTypeCapabilities.Capability>> computeCapabilityMap(
        String fieldTypeName,
        DataFormatRegistry registry,
        Set<DataFormat> formats
    ) {
        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> result = new HashMap<>();

        for (FieldTypeCapabilities.Capability capability : FieldTypeCapabilities.Capability.values()) {
            List<DataFormat> supporters = registry.supportsCapability(fieldTypeName, capability);
            // Filter to only formats in our composite (primary + secondaries)
            // First match wins (list is sorted by priority ascending)
            for (DataFormat supporter : supporters) {
                if (formats.contains(supporter)) {
                    result.computeIfAbsent(supporter, k -> new HashSet<>()).add(capability);
                    break;
                }
            }
        }

        // Return immutable copy — only entries with non-empty capability sets exist due to computeIfAbsent logic
        return Map.copyOf(result);
    }

    /**
     * Validates that at least one of the provided data formats supports the given field type.
     * Uses {@link DataFormat#supportedFields()} declarations to check support.
     *
     * @param fieldTypeName the field type name to validate
     * @param registry      the DataFormatRegistry (reserved for future use)
     * @param formats       the set of DataFormat instances to consider
     * @throws MapperParsingException if no format supports the field type
     */
    public static void validateFieldTypeSupported(
        String fieldTypeName,
        DataFormatRegistry registry,
        Set<DataFormat> formats
    ) {
        boolean supported = false;
        for (DataFormat format : formats) {
            for (FieldTypeCapabilities ftc : format.supportedFields()) {
                if (ftc.fieldType().equals(fieldTypeName)) {
                    supported = true;
                    break;
                }
            }
            if (supported) break;
        }
        if (supported == false) {
            throw new MapperParsingException(
                "Field type [" + fieldTypeName + "] is not supported by any registered data format "
                    + formats.stream().map(DataFormat::name).collect(Collectors.toList())
            );
        }
    }
}
