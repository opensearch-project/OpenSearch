/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.annotation.PublicApi;

import java.util.HashSet;
import java.util.Set;

/**
 * Enum for different Search Phases in OpenSearch
 *
 * @opensearch.api
 */
@PublicApi(since = "2.9.0")
public enum SearchPhaseName {
    DFS_PRE_QUERY("dfs_pre_query"),
    QUERY("query"),
    FETCH("fetch"),
    DFS_QUERY("dfs_query"),
    EXPAND("expand"),
    CAN_MATCH("can_match");

    private final String name;
    private static final Set<String> PHASE_NAMES = new HashSet<>();
    static {
        for (SearchPhaseName phaseName : SearchPhaseName.values()) {
            PHASE_NAMES.add(phaseName.name);
        }
    }

    SearchPhaseName(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static boolean isValidName(String phaseName) {
        return PHASE_NAMES.contains(phaseName);
    }
}
