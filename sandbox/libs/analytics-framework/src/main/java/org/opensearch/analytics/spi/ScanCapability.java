/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Set;

/**
 * Declares a backend's ability to read data from a storage source.
 *
 * @opensearch.internal
 */
public sealed interface ScanCapability {

    Set<String> formats();

    Set<FieldType> supportedFieldTypes();

    /** Columnar doc values (e.g. Parquet, Lucene doc values). */
    record DocValues(Set<String> formats, Set<FieldType> supportedFieldTypes) implements ScanCapability {
    }

    /** Row-oriented stored fields (e.g. Lucene _source, stored fields). */
    record StoredFields(Set<String> formats, Set<FieldType> supportedFieldTypes) implements ScanCapability {
    }

    /**
     * Inverted index — backend can drive metadata-only operations (count aggregates today,
     * group-by-count and top-K terms in future) by walking the term dictionary without
     * reading row values. Cannot deliver field values for Project/Sort/Filter that need
     * the actual data; consumers must check operation-specific capability when intending
     * to read values.
     *
     * <p>Today's only producer is the Lucene-secondary backend for keyword / text /
     * match_only_text field types — the same set indexed at write time.
     */
    record InvertedIndex(Set<String> formats, Set<FieldType> supportedFieldTypes) implements ScanCapability {
    }
}
