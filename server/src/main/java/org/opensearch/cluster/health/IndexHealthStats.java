/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.health;

/**
 * Accumulator class for tracking health metrics during index status computation.
 * Maintains running counts of shards in different states and overall health status.
 * Used as temporary storage during health calculations before creating final stats.
 */

public class IndexHealthStats {
    ClusterHealthStatus status = ClusterHealthStatus.GREEN;
    int activePrimaryShards = 0;
    int activeShards = 0;
    int relocatingShards = 0;
    int initializingShards = 0;
    int unassignedShards = 0;
    int delayedUnassignedShards = 0;
}
