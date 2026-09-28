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
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Encapsulates index sort configuration for the native Parquet writer.
 *
 * <p>Extracts sort columns, sort orders, null-handling preferences, and
 * multi-value reduction modes from {@link IndexSettings} and exposes them as
 * typed lists ready for the native bridge.
 *
 * <p>The {@code maxSortModes} list is parallel with {@code sortColumns}: each
 * entry is {@code true} when the field's multi-value (LIST) reduction mode is
 * {@link MultiValueMode#MAX} and {@code false} when it is {@link MultiValueMode#MIN}.
 * This mirrors OpenSearch's {@link IndexSortConfig#buildIndexSort} semantics:
 * an explicit {@code index.sort.mode} wins, otherwise the mode defaults to MAX
 * for a descending field and MIN for an ascending field.
 */
public record ParquetSortConfig(List<String> sortColumns, List<Boolean> reverseSorts, List<Boolean> nullsFirst, List<
    Boolean> maxSortModes) {

    private static final ParquetSortConfig EMPTY = new ParquetSortConfig(
        Collections.emptyList(),
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
            IndexSortConfig.INDEX_SORT_MISSING_SETTING.get(indexSettings.getSettings()).stream().map("_first"::equals).toList(),
            deriveMaxSortModes(
                IndexSortConfig.INDEX_SORT_FIELD_SETTING.get(indexSettings.getSettings()).size(),
                IndexSortConfig.INDEX_SORT_ORDER_SETTING.get(indexSettings.getSettings()),
                IndexSortConfig.INDEX_SORT_MODE_SETTING.get(indexSettings.getSettings())
            )
        );
    }

    /**
     * Derives one {@code true = MAX / false = MIN} entry per sort field, following
     * {@link IndexSortConfig#buildIndexSort}: an explicit mode wins; otherwise DESC
     * defaults to MAX and ASC (or an omitted order) defaults to MIN.
     */
    static List<Boolean> deriveMaxSortModes(int fieldCount, List<SortOrder> orders, List<MultiValueMode> modes) {
        List<Boolean> result = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            if (i < modes.size()) {
                result.add(modes.get(i) == MultiValueMode.MAX);
            } else {
                result.add(i < orders.size() && orders.get(i) == SortOrder.DESC);
            }
        }
        return result;
    }

    /**
     * Returns an empty sort config (no sorting).
     */
    public static ParquetSortConfig empty() {
        return EMPTY;
    }
}
