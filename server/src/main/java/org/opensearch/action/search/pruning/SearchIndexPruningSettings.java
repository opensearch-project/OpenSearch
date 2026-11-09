/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import org.opensearch.common.settings.Setting;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.opensearch.common.settings.Setting.Property.Dynamic;
import static org.opensearch.common.settings.Setting.Property.NodeScope;

/**
 * Cluster settings controlling index-level search pruning.
 */
public final class SearchIndexPruningSettings {
    private SearchIndexPruningSettings() {}

    /**
     * Enables coordinator-side index pruning before can-match/query execution.
     */
    public static final Setting<Boolean> ENABLED = Setting.boolSetting("search.index_pruning.enabled", false, Dynamic, NodeScope);

    /**
     * Minimum number of shard groups required before pruning is attempted.
     */
    public static final Setting<Integer> MIN_SHARDS = Setting.intSetting("search.index_pruning.min_shards", 128, 1, Dynamic, NodeScope);

    /**
     * Query fields eligible for pruning.
     */
    public static final Setting<List<String>> FIELDS = Setting.listSetting(
        "search.index_pruning.fields",
        Collections.emptyList(),
        Function.identity(),
        Dynamic,
        NodeScope
    );

    /**
     * Returns all pruning settings for registration with cluster settings.
     */
    public static List<Setting<?>> getSettings() {
        return List.of(ENABLED, MIN_SHARDS, FIELDS);
    }
}
