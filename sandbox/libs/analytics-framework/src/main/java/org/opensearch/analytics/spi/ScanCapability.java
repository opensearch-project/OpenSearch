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
     * Index — drives metadata-only ops (count today; group-by-count, top-K terms later)
     * via the index. Cannot deliver row values; consumers needing values must check
     * value-producing caps. Today only Lucene's inverted index satisfies this, for
     * keyword/text/match_only_text.
     */
    record Index(Set<String> formats, Set<FieldType> supportedFieldTypes) implements ScanCapability {
    }
}
