/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.discovery;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.coordination.PendingClusterStateStats;
import org.opensearch.cluster.coordination.PublishClusterStateStats;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterStateStats;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Clusterless implementation of Discovery. This is only able to "discover" the local node.
 */
public class LocalDiscovery extends AbstractLifecycleComponent implements Discovery {
    private static final DiscoveryStats EMPTY_STATS = new DiscoveryStats(
        new PendingClusterStateStats(0, 0, 0),
        new PublishClusterStateStats(0, 0, 0),
        new ClusterStateStats()
    );
    private final TransportService transportService;
    private final ClusterApplier clusterApplier;

    public LocalDiscovery(TransportService transportService, ClusterApplier clusterApplier) {
        this.transportService = transportService;
        this.clusterApplier = clusterApplier;
    }

    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener) {
        // In clusterless mode, we should never be asked to publish a cluster state.
        throw new UnsupportedOperationException("Should not be called in clusterless mode");
    }

    @Override
    protected void doStart() {
        DiscoveryNode localNode = transportService.getLocalNode();
        ClusterState bootstrapClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode).build())
            .build();
        clusterApplier.setInitialState(bootstrapClusterState);
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }

    @Override
    public DiscoveryStats stats() {
        return EMPTY_STATS;
    }

    @Override
    public void startInitialJoin() {

    }

    @Override
    public void setNodeConnectionsService(NodeConnectionsService nodeConnectionsService) {

    }
}
