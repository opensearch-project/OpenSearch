/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

/**
 * Groups that memory pools belong to for aggregated customer-facing stats.
 * Each pool is assigned to exactly one group at registration time.
 *
 * @opensearch.api
 */
public enum PoolGroup {
    /** Arrow Flight transport pool group. */
    TRANSPORT("transport"),
    /** Query and analytics execution pool group. */
    SEARCH("search"),
    /** Ingest and write path pool group. */
    INDEXING("indexing"),
    /** Background merge operations pool group. */
    MERGE("merge");

    private final String name;

    PoolGroup(String name) {
        this.name = name;
    }

    /** Returns the group name used in stats output. */
    public String getName() {
        return name;
    }
}
