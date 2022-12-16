/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;

public class ClusterInfoHolder implements ClusterStateListener {

    protected final Logger log = LogManager.getLogger(this.getClass());
    private volatile DiscoveryNodes nodes = null;
    private volatile Boolean isLocalNodeElectedClusterManager = null;
    private volatile boolean initialized;

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if(nodes == null || event.nodesChanged()) {
            nodes = event.state().nodes();
            if (log.isDebugEnabled()) {
                log.debug("Cluster Info Holder now initialized for 'nodes'");
            }
            initialized = true;
        }

        isLocalNodeElectedClusterManager = event.localNodeClusterManager()?Boolean.TRUE:Boolean.FALSE;
    }

    public Boolean isLocalNodeElectedClusterManager() {
        return isLocalNodeElectedClusterManager;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public Boolean hasNode(DiscoveryNode node) {
        if(nodes == null) {
            if(log.isDebugEnabled()) {
                log.debug("Cluster Info Holder not initialized yet for 'nodes'");
            }
            return null;
        }

        return nodes.nodeExists(node)?Boolean.TRUE:Boolean.FALSE;
    }
}

