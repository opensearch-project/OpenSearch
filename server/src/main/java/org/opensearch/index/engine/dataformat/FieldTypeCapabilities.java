/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Set;

/**
 * Represents the field type capabilities for a data format, mapping a field name to its supported capabilities.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FieldTypeCapabilities {
    /**
     * Capabilities that a data format can support.
     */
    @ExperimentalApi
    public enum Capability {
        /** Inverted index based full-text search (BM25, phrase queries) */
        FULL_TEXT_SEARCH,

        /** Column-oriented storage optimized for aggregations and analytics */
        COLUMNAR_STORAGE,

        /** Vector similarity search (kNN, ANN) */
        VECTOR_SEARCH,

        /** Numeric and date range queries via point trees */
        POINT_RANGE,

        /** Original field value retrieval, stored in row wise fashion */
        STORED_FIELDS,

        /** Probabilistic lookup for pruning*/
        BLOOM_FILTER
    }

    private final String fieldType;
    private final Set<Capability> capabilities;

    /**
     * Constructs a FieldTypeCapabilities with the given field name and capabilities.
     *
     * @param fieldType the field name
     * @param capabilities the set of capabilities supported for this field
     */
    public FieldTypeCapabilities(String fieldType, Set<Capability> capabilities) {
        this.fieldType = fieldType;
        this.capabilities = Set.copyOf(capabilities);
    }

    /**
     * Returns the field name.
     *
     * @return the field name
     */
    public String getFieldType() {
        return fieldType;
    }

    /**
     * Returns the set of capabilities supported for this field.
     *
     * @return the capabilities
     */
    public Set<Capability> getCapabilities() {
        return capabilities;
    }
}
