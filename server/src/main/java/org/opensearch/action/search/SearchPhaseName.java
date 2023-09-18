/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import java.util.HashMap;
import java.util.Map;

/**
 * Enum for different Search Phases in OpenSearch
 * @opensearch.internal
 */
public enum SearchPhaseName {
    DFS_PRE_QUERY("dfs"),
    QUERY("query"),
    FETCH("fetch"),
    DFS_QUERY("dfs_query"),
    EXPAND("expand"),
    CAN_MATCH("can_match");

    private static final Map<String, SearchPhaseName> STRING_TO_ENUM = new HashMap<>();

    static {
        for (SearchPhaseName searchPhaseName : values()) {
            STRING_TO_ENUM.put(searchPhaseName.getName(), searchPhaseName);
        }
    }

    private final String name;

    SearchPhaseName(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static SearchPhaseName getSearchPhaseName(String value) {
        return STRING_TO_ENUM.get(value);
    }
}
