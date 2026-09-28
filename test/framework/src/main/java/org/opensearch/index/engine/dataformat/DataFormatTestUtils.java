/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.index.mapper.MappedFieldType;

import java.util.Map;

/**
 * Test utilities for the pluggable data format capability system.
 */
public final class DataFormatTestUtils {

    private DataFormatTestUtils() {}

    /**
     * Assigns the capability map on a {@link MappedFieldType} so that the given {@link DataFormat}
     * owns the capabilities declared in its {@link DataFormat#supportedFields()} for the field's type name.
     * Simulates what {@code FieldCapabilityAssigner.assign} does at mapping build time.
     */
    public static void assignTestCapabilities(MappedFieldType fieldType, DataFormat format) {
        format.supportedFields()
            .stream()
            .filter(ftc -> ftc.fieldType().equals(fieldType.typeName()))
            .findFirst()
            .ifPresent(ftc -> fieldType.setCapabilityMap(Map.of(format, ftc.capabilities())));
    }
}
