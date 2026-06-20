/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import java.util.Locale;

/**
 * Strategy for preserving field values when reconstructing source from derived fields.
 * Controls whether to use doc values (sorted/deduplicated) or stored fields (preserves order/duplicates).
 *
 * @opensearch.internal
 */
public enum DerivedSourceKeep {
    /**
     * Use doc values (default). Values are sorted and deduplicated.
     * Most storage-efficient but loses array order and duplicates.
     */
    NONE("none"),

    /**
     * Use stored fields. Preserves array order and duplicate values.
     * Automatically enables store=true. Higher storage overhead.
     */
    ARRAYS("arrays");

    private final String value;

    DerivedSourceKeep(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Parses string value into enum. Case-insensitive.
     * Returns NONE if value is null.
     */
    public static DerivedSourceKeep fromString(String value) {
        if (value == null) {
            return NONE;
        }

        String normalizedValue = value.toLowerCase(Locale.ROOT);
        for (DerivedSourceKeep mode : values()) {
            if (mode.value.equals(normalizedValue)) {
                return mode;
            }
        }

        throw new IllegalArgumentException(
            "Invalid value for derived_source_keep: [" + value + "]. Valid values are: [none, arrays]"
        );
    }

    /**
     * Returns true if this mode requires stored fields to be enabled.
     */
    public boolean requiresStoredFields() {
        return this == ARRAYS;
    }

    @Override
    public String toString() {
        return value;
    }
}
