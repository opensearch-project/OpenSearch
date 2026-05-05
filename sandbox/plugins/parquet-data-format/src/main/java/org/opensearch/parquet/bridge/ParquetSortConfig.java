/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.index.IndexSettings;
import org.opensearch.index.IndexSortConfig;
import org.opensearch.search.sort.SortOrder;

import java.util.Collections;
import java.util.List;

/**
 * Encapsulates index sort configuration for the native Parquet writer.
 *
 * <p>Extracts sort columns, sort orders, and null-handling preferences from
 * {@link IndexSettings} and exposes them as typed lists ready for the native bridge.
 */
public record ParquetSortConfig(List<String> sortColumns, List<Boolean> reverseSorts, List<Boolean> nullsFirst) {

    private static final ParquetSortConfig EMPTY = new ParquetSortConfig(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList()
    );

    /**
     * Creates a sort config from index settings.
     *
     * @param indexSettings the index settings to extract sort configuration from
     */
    public ParquetSortConfig(IndexSettings indexSettings) {
        this(
            IndexSortConfig.INDEX_SORT_FIELD_SETTING.get(indexSettings.getSettings()),
            IndexSortConfig.INDEX_SORT_ORDER_SETTING.get(indexSettings.getSettings()).stream().map(o -> o == SortOrder.DESC).toList(),
            IndexSortConfig.INDEX_SORT_MISSING_SETTING.get(indexSettings.getSettings()).stream().map("_first"::equals).toList()
        );
    }

    /**
     * Returns an empty sort config (no sorting).
     */
    public static ParquetSortConfig empty() {
        return EMPTY;
    }
}
