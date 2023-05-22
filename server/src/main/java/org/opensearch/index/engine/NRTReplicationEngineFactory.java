/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;

/**
 * Engine Factory implementation used with Segment Replication that wires up replica shards with an ${@link NRTReplicationEngine}
 * and primary with an ${@link InternalEngine}
 *
 * @opensearch.internal
 */
public class NRTReplicationEngineFactory implements EngineFactory {

    private final ClusterService clusterService;

    public NRTReplicationEngineFactory(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public NRTReplicationEngineFactory() {
        this.clusterService = null;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        if (config.isReadOnlyReplica()) {
            return new NRTReplicationEngine(config);
        }
        if (clusterService != null) {
            DiscoveryNodes nodes = this.clusterService.state().nodes();
            config.setClusterMinVersion(nodes.getMinNodeVersion());
        }
        return new InternalEngine(config);
    }
}
