/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.shard;

import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;

import java.util.List;
import java.util.function.Consumer;

/**
 * When a new {@link ReplicationGroup} is generated, the relevant changes of {@link IndexShard}
 *
 * @opensearch.internal
 */
public class IndexShardReplicationGroupListener implements Consumer<ReplicationGroup> {
    private final IndexShard indexShard;
    private volatile long replicationGroupVersion = -1;

    public IndexShardReplicationGroupListener(IndexShard indexShard) {
        this.indexShard = indexShard;
    }

    @Override
    public void accept(ReplicationGroup replicationGroup) {
        if (isNewerVersion(replicationGroup)) {
            synchronized (this) {
                if (isNewerVersion(replicationGroup)) {
                    replicationGroupVersion = replicationGroup.getVersion();
                    DiscoveryNodes discoveryNodes = indexShard.getDiscoveryNodes();
                    List<ShardRouting> replicaShards = replicationGroup.getRoutingTable().replicaShards();
                    indexShard.setActiveReplicaNodes(
                        replicaShards.stream().filter(ShardRouting::active).map(s -> discoveryNodes.get(s.currentNodeId())).toList()
                    );
                }
            }
        }
    }

    private boolean isNewerVersion(ReplicationGroup replicationGroup) {
        // Relative comparison to mitigate long overflow
        return replicationGroup.getVersion() - replicationGroupVersion > 0;
    }
}
