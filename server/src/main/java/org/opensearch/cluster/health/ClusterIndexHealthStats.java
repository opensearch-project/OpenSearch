/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.health;

import java.util.Map;

/**
 * Container class for health statistics of a cluster index.
 * Aggregates shard health status, counts of shard states (active/relocating/etc),
 * and maintains mapping of shard IDs to their detailed health metrics.
 * Used to track and report both regular index health and scaled-down index health.
 */

public class ClusterIndexHealthStats {
    final ClusterHealthStatus status;
    final int activePrimaryShards;
    final int activeShards;
    final int relocatingShards;
    final int initializingShards;
    final int unassignedShards;
    final int delayedUnassignedShards;
    final Map<Integer, ClusterShardHealth> shards;

    ClusterIndexHealthStats(
        ClusterHealthStatus status,
        int activePrimaryShards,
        int activeShards,
        int relocatingShards,
        int initializingShards,
        int unassignedShards,
        int delayedUnassignedShards,
        Map<Integer, ClusterShardHealth> shards
    ) {
        this.status = status;
        this.activePrimaryShards = activePrimaryShards;
        this.activeShards = activeShards;
        this.relocatingShards = relocatingShards;
        this.initializingShards = initializingShards;
        this.unassignedShards = unassignedShards;
        this.delayedUnassignedShards = delayedUnassignedShards;
        this.shards = shards;
    }
}
