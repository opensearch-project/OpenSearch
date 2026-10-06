/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;

import java.util.List;

/**
 * Single source of truth for resolving the hash-shuffle partition count.
 *
 * <p>Resolution order:
 * <ol>
 *   <li>{@link AnalyticsSettings#MPP_SHUFFLE_PARTITIONS} cluster setting if positive — operator
 *       override pinning the count.</li>
 *   <li>Otherwise, the maximum of {@code defaultShuffleParallelism(clusterState)} across the
 *       given viable backends. Backends that don't participate in MPP shuffle return
 *       {@code 1}; backends that do return a real count (typically the data-node count). The
 *       max ensures we don't artificially under-size when at least one viable backend supports
 *       shuffle.</li>
 * </ol>
 *
 * <p>Returns {@code 1} if no backend opts in. Callers should treat {@code partitionCount ≤ 1}
 * as "shuffle is disabled for this join" — the {@code OpenSearchHashJoinSplitRule} refuses to
 * fire and the strategy advisor falls back to BROADCAST or COORDINATOR_CENTRIC.
 *
 * @opensearch.internal
 */
public final class MppShufflePartitions {

    private MppShufflePartitions() {}

    /** Resolves the partition count using settings + the registry's view of viable backends. */
    public static int resolve(Settings settings, ClusterState state, CapabilityRegistry registry, List<String> viableBackends) {
        Integer override = AnalyticsSettings.MPP_SHUFFLE_PARTITIONS.get(settings);
        if (override != null && override > 0) {
            return override;
        }
        int max = 1;
        for (String backendId : viableBackends) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(backendId);
            int n = backend.defaultShuffleParallelism(state);
            if (n > max) max = n;
        }
        return max;
    }
}
