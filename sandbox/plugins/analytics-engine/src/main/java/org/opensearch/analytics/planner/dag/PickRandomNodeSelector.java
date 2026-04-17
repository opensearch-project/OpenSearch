/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Selects N random data nodes from the cluster for shuffle partition assignment.
 *
 * <p>TODO: implement ResourceAwareNodeSelector as an alternative that picks nodes
 * based on available memory and disk metrics.
 *
 * @opensearch.internal
 */
public class PickRandomNodeSelector implements NodeSelector {

    private final int count;

    public PickRandomNodeSelector(int count) {
        this.count = count;
    }

    @Override
    public List<DiscoveryNode> select(ClusterState clusterState, @Nullable Object childManifest) {
        // TODO: implement when shuffle aggregates/joins are added.
        throw new UnsupportedOperationException("PickRandomNodeSelector not yet implemented");
    }
}
