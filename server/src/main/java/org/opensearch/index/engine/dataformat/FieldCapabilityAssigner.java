/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Assigns capability maps to field types based on a prioritized list of configured data formats.
 *
 * <p>Uses a greedy algorithm: each format in priority order claims the capabilities it supports
 * for the field type. If any requested capability remains unowned after the walk, validation fails.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FieldCapabilityAssigner {

    private static final Logger logger = LogManager.getLogger(FieldCapabilityAssigner.class);

    private final List<DataFormat> configuredFormats;

    public FieldCapabilityAssigner(List<DataFormat> configuredFormats) {
        this.configuredFormats = configuredFormats == null ? List.of() : List.copyOf(configuredFormats);
    }

    /**
     * Assigns the capability map on the given field type. Each configured format claims the
     * capabilities it supports; unclaimed capabilities result in a {@link MapperParsingException}.
     */
    public void assign(MappedFieldType fieldType) {
        Set<FieldTypeCapabilities.Capability> requested = fieldType.requestedCapabilities();

        if (requested.isEmpty() || configuredFormats.isEmpty()) {
            fieldType.setCapabilityMap(Map.of());
            return;
        }

        String typeName = fieldType.typeName();
        Set<FieldTypeCapabilities.Capability> remaining = new HashSet<>(requested);
        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> assigned = new LinkedHashMap<>();

        for (DataFormat format : configuredFormats) {
            if (remaining.isEmpty()) {
                break;
            }
            Set<FieldTypeCapabilities.Capability> claimed = claimSupportedCapabilities(format, typeName, remaining);
            if (claimed.isEmpty() == false) {
                assigned.put(format, Set.copyOf(claimed));
                remaining.removeAll(claimed);
            }
        }

        if (remaining.isEmpty() == false) {
            throw new MapperParsingException(
                "Field ["
                    + fieldType.name()
                    + "] of type ["
                    + typeName
                    + "] requires capabilities "
                    + requested
                    + " but configured data formats cannot collectively cover them. Unsupported capabilities: "
                    + remaining
                    + ". Configured formats: "
                    + configuredFormats.stream().map(DataFormat::name).collect(Collectors.toList())
            );
        }
        logger.info("{} assigned to :  {}", fieldType.name(), assigned);
        if (fieldType.name().equals("fieldA_10")) {
            logger.info("{} assigned to :  {} for debug", fieldType.name(), assigned);
        }
        fieldType.setCapabilityMap(Map.copyOf(assigned));
    }

    private static Set<FieldTypeCapabilities.Capability> claimSupportedCapabilities(
        DataFormat format,
        String typeName,
        Set<FieldTypeCapabilities.Capability> required
    ) {
        return format.supportedFields().stream().filter(ftc -> ftc.fieldType().equals(typeName)).findFirst().map(ftc -> {
            Set<FieldTypeCapabilities.Capability> intersection = EnumSet.noneOf(FieldTypeCapabilities.Capability.class);
            for (FieldTypeCapabilities.Capability cap : required) {
                if (ftc.capabilities().contains(cap)) {
                    intersection.add(cap);
                }
            }
            return (Set<FieldTypeCapabilities.Capability>) intersection;
        }).orElse(Set.of());
    }
}
