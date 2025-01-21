/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.annotation.PublicApi;

/**
 * Enum for different Search Phases in OpenSearch
 *
 * @opensearch.api
 */
@PublicApi(since = "2.9.0")
public enum SearchPhaseName {
    DFS_PRE_QUERY("dfs_pre_query", true),
    QUERY("query", true),
    FETCH("fetch", true),
    DFS_QUERY("dfs_query", true),
    EXPAND("expand", true),
    CAN_MATCH("can_match", true),

    // A catch-all for other phase types which shouldn't appear in the search phase stats API.
    OTHER_PHASE_TYPES("other_phase_types", false);

    private final String name;
    private final boolean shouldTrack;

    SearchPhaseName(final String name, final boolean shouldTrack) {
        this.name = name;
        this.shouldTrack = shouldTrack;
    }

    public String getName() {
        return name;
    }

    public boolean shouldTrack() {
        return shouldTrack;
    }
}
