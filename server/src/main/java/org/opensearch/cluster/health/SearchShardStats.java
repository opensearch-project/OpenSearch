/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.health;

/**
 * Statistics for search-only shards in scaled-down indices.
 * Tracks active, relocating, initializing and unassigned shards, along with their aggregated health status.
 * Used by ClusterShardHealth to compute health metrics for indices where indexing shards have been removed.
 */

public class SearchShardStats {
    final boolean hasSearchOnlyShards;
    final int activeShards;
    final int relocatingShards;
    final int initializingShards;
    final int unassignedShards;
    final int delayedUnassignedShards;
    final ClusterHealthStatus status;

    SearchShardStats(
        boolean hasSearchOnlyShards,
        int activeShards,
        int relocatingShards,
        int initializingShards,
        int unassignedShards,
        int delayedUnassignedShards,
        ClusterHealthStatus status
    ) {
        this.hasSearchOnlyShards = hasSearchOnlyShards;
        this.activeShards = activeShards;
        this.relocatingShards = relocatingShards;
        this.initializingShards = initializingShards;
        this.unassignedShards = unassignedShards;
        this.delayedUnassignedShards = delayedUnassignedShards;
        this.status = status;
    }
}
