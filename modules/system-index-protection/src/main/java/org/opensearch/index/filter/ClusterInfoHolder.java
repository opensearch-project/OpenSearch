/*
 * Copyright 2015-2018 _floragunn_ GmbH
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.filter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;

public class ClusterInfoHolder implements ClusterStateListener {

    protected final Logger log = LogManager.getLogger(this.getClass());
    private volatile DiscoveryNodes nodes = null;
    private volatile Boolean isLocalNodeElectedClusterManager = null;
    private volatile boolean initialized;
    private final String clusterName;

    public ClusterInfoHolder(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (nodes == null || event.nodesChanged()) {
            nodes = event.state().nodes();
            if (log.isDebugEnabled()) {
                log.debug("Cluster Info Holder now initialized for 'nodes'");
            }
            initialized = true;
        }

        isLocalNodeElectedClusterManager = event.localNodeClusterManager() ? Boolean.TRUE : Boolean.FALSE;
    }

    public Boolean isLocalNodeElectedClusterManager() {
        return isLocalNodeElectedClusterManager;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public Version getMinNodeVersion() {
        if (nodes == null) {
            if (log.isDebugEnabled()) {
                log.debug("Cluster Info Holder not initialized yet for 'nodes'");
            }
            return null;
        }

        return nodes.getMinNodeVersion();
    }

    public Boolean hasNode(DiscoveryNode node) {
        if (nodes == null) {
            if (log.isDebugEnabled()) {
                log.debug("Cluster Info Holder not initialized yet for 'nodes'");
            }
            return null;
        }

        return nodes.nodeExists(node) ? Boolean.TRUE : Boolean.FALSE;
    }

    public String getClusterName() {
        return this.clusterName;
    }
}
