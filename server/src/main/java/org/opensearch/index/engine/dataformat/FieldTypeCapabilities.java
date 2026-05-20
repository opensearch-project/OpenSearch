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
public record FieldTypeCapabilities(String fieldType, Set<Capability> capabilities) {

    public FieldTypeCapabilities {
        capabilities = Set.copyOf(capabilities);
    }

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
}
