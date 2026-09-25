/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSortConfig;
import org.opensearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The {@code index.sort} declared on an index: field names paired with their direction. Lets planner
 * rules ask whether a requested collation is served by the on-disk order, and therefore whether a
 * Sort+Limit over the scan terminates after the first K rows.
 *
 * @param keys the sort keys in declared order
 * @opensearch.internal
 */
public record IndexSort(List<Key> keys) {

    /** One sort key: a physical field name and whether it is sorted descending. */
    public record Key(String field, boolean descending) {
        public Key reversed() {
            return new Key(field, descending == false);
        }
    }

    /** The index sort declared on {@code index}, or empty when it has none. */
    public static Optional<IndexSort> of(IndexMetadata index) {
        Settings settings = index.getSettings();
        List<String> fields = IndexSortConfig.INDEX_SORT_FIELD_SETTING.get(settings);
        if (fields.isEmpty()) {
            return Optional.empty();
        }
        // IndexSortConfig validates that an explicit order list matches the field list in length.
        List<SortOrder> orders = IndexSortConfig.INDEX_SORT_ORDER_SETTING.exists(settings)
            ? IndexSortConfig.INDEX_SORT_ORDER_SETTING.get(settings)
            : null;
        List<Key> keys = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            keys.add(new Key(fields.get(i), orders != null && orders.get(i) == SortOrder.DESC));
        }
        return Optional.of(new IndexSort(List.copyOf(keys)));
    }

    /** The index sort shared by every index, or empty if any lacks one or they differ. */
    public static Optional<IndexSort> commonTo(List<IndexMetadata> indices) {
        Optional<IndexSort> common = Optional.empty();
        for (IndexMetadata index : indices) {
            Optional<IndexSort> sort = of(index);
            if (sort.isEmpty() || (common.isPresent() && common.get().equals(sort.get()) == false)) {
                return Optional.empty();
            }
            common = sort;
        }
        return common;
    }

    /**
     * True when rows read in index order (forwards or backwards) already satisfy {@code collation}:
     * the collation is a prefix of this sort, either as declared or with every direction flipped.
     * Null ordering is not considered.
     */
    public boolean serves(List<Key> collation) {
        if (collation.isEmpty() || collation.size() > keys.size()) {
            return false;
        }
        List<Key> prefix = keys.subList(0, collation.size());
        return prefix.equals(collation) || prefix.stream().map(Key::reversed).toList().equals(collation);
    }
}
