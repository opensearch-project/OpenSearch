/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.ArrayList;
import java.util.List;

/**
 * SegmentReplicationUpgradeListener is used to upgrade the opensearch version used by all primaries of a cluster when
 * segment replication is enabled and a rolling upgrade is completed (while in mixed cluster state, the primaries use lower codec
 * version on their primaries and this needs to be reset once upgrade is complete).
 */
public class SegmentReplicationUpgradeListener implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationUpgradeListener.class);

    private final IndicesService indicesService;

    public SegmentReplicationUpgradeListener(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesChanged()) {
            List<IndexShard> indexShardList = new ArrayList<>();
            DiscoveryNodes nodes = event.state().nodes();
            if (nodes.getMinNodeVersion().equals(nodes.getMaxNodeVersion())) {
                for (IndexService indexService : indicesService) {
                    for (IndexShard indexShard : indexService) {
                        try {
                            if (indexShard.indexSettings().isSegRepEnabled()
                                && indexShard.indexSettings().getNumberOfReplicas() > 0
                                && indexShard.routingEntry().primary()
                                && (indexShard.getEngine().config().getClusterMinVersion() != nodes.getMaxNodeVersion())) {
                                indexShardList.add(indexShard);
                            }
                        } catch (AlreadyClosedException e) {
                            logger.warn("Index shard [{}] engine is already closed.", indexShard.shardId());
                        }
                    }
                }
            }
            try {
                if (indexShardList.isEmpty() == false) {
                    for (IndexShard indexShard : indexShardList) {
                        indexShard.resetEngine();
                    }
                }
            } catch (Exception e) {
                logger.error("Received unexpected exception: [{}]", e.getMessage());
            }
        }

    }

}
