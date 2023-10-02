/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.StepListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.RemoteClusterService;

import java.util.Set;
import java.util.function.BiFunction;

/**
 * Helper class for common search functions
 */
public class SearchUtils {

    public SearchUtils() {}

    /**
     * Get connection lookup listener for list of clusters passed
     */
    public static ActionListener<BiFunction<String, String, DiscoveryNode>> getConnectionLookupListener(
        RemoteClusterService remoteClusterService,
        ClusterState state,
        Set<String> clusters
    ) {
        final StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = new StepListener<>();

        if (clusters.isEmpty()) {
            lookupListener.onResponse((cluster, nodeId) -> state.getNodes().get(nodeId));
        } else {
            remoteClusterService.collectNodes(clusters, lookupListener);
        }
        return lookupListener;
    }
}
