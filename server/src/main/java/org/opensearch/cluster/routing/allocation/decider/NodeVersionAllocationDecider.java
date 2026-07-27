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

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * An allocation decider that prevents relocation or allocation from nodes
 * that might not be Lucene format compatible. If we relocate from a node that writes
 * segments in a newer Lucene major/minor version than the node we relocate to understands,
 * this might cause {@link org.apache.lucene.index.IndexFormatTooNewException} on the lowest
 * level since it might have already written segments that use a new postings format or codec
 * that is not available on the target node. Nodes on the same Lucene major/minor version (e.g.
 * differing only by an OpenSearch patch release) are always considered compatible, since a
 * Lucene patch release does not change the on-disk segment/codec format.
 *
 * @opensearch.internal
 */
public class NodeVersionAllocationDecider extends AllocationDecider {

    public static final String NAME = "node_version";

    private final ReplicationType replicationType;

    public NodeVersionAllocationDecider(Settings settings) {
        replicationType = IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(settings);
    }

    /**
     * Returns true if a node running {@code target} can read segments written by a node running
     * {@code source}, based on the actual Lucene version of each (rather than the raw OpenSearch
     * version id). This is the case when {@code target}'s Lucene major.minor is the same as or
     * newer than {@code source}'s -- in particular this allows OpenSearch patch-level differences
     * that share the same Lucene major.minor, since Lucene patch releases do not change the
     * segment/codec format.
     */
    private static boolean isLuceneVersionCompatible(Version target, Version source) {
        org.apache.lucene.util.Version targetLucene = target.luceneVersion;
        org.apache.lucene.util.Version sourceLucene = source.luceneVersion;
        return targetLucene.major > sourceLucene.major
            || (targetLucene.major == sourceLucene.major && targetLucene.minor >= sourceLucene.minor);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            if (replicationType == ReplicationType.SEGMENT) {
                List<ShardRouting> replicas = allocation.routingNodes()
                    .assignedShards(shardRouting.shardId())
                    .stream()
                    .filter(shr -> !shr.primary() && shr.active())
                    .collect(Collectors.toList());
                for (ShardRouting replica : replicas) {
                    // can not allocate if target node version > any existing replica version, unless they are
                    // still Lucene-compatible (e.g. an OpenSearch patch-level difference)
                    RoutingNode replicaNode = allocation.routingNodes().node(replica.currentNodeId());
                    if (node.node().getVersion().after(replicaNode.node().getVersion())
                        && isLuceneVersionCompatible(node.node().getVersion(), replicaNode.node().getVersion()) == false) {
                        return allocation.decision(
                            Decision.NO,
                            NAME,
                            "When segment replication is enabled, cannot relocate primary shard to a node with version [%s] if it has a replica on older, Lucene-incompatible version [%s]",
                            node.node().getVersion(),
                            replicaNode.node().getVersion()
                        );
                    }
                }
            }
            if (shardRouting.currentNodeId() == null) {
                if (shardRouting.recoverySource() != null && shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                    // restoring from a snapshot - check that the node can handle the version
                    return isVersionCompatible((SnapshotRecoverySource) shardRouting.recoverySource(), node, allocation);
                } else {
                    // existing or fresh primary on the node
                    return allocation.decision(Decision.YES, NAME, "the primary shard is new or already existed on the node");
                }
            } else {
                // relocating primary, only migrate to newer host
                return isVersionCompatibleRelocatePrimary(allocation.routingNodes(), shardRouting.currentNodeId(), node, allocation);
            }
        } else {
            final ShardRouting primary = allocation.routingNodes().activePrimary(shardRouting.shardId());
            // check that active primary has a newer version so that peer recovery works
            if (primary != null) {
                return isVersionCompatibleAllocatingReplica(allocation.routingNodes(), primary.currentNodeId(), node, allocation);
            } else {
                // ReplicaAfterPrimaryActiveAllocationDecider should prevent this case from occurring
                return allocation.decision(Decision.YES, NAME, "no active primary shard yet");
            }
        }
    }

    private Decision isVersionCompatibleRelocatePrimary(
        final RoutingNodes routingNodes,
        final String sourceNodeId,
        final RoutingNode target,
        final RoutingAllocation allocation
    ) {
        final RoutingNode source = routingNodes.node(sourceNodeId);
        if (target.node().getVersion().onOrAfter(source.node().getVersion())
            || isLuceneVersionCompatible(target.node().getVersion(), source.node().getVersion())) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "can relocate primary shard from a node with version [%s] to a node with equal-or-newer, Lucene-compatible version [%s]",
                source.node().getVersion(),
                target.node().getVersion()
            );
        } else {
            return allocation.decision(
                Decision.NO,
                NAME,
                "cannot relocate primary shard from a node with version [%s] to a node with older, Lucene-incompatible version [%s]",
                source.node().getVersion(),
                target.node().getVersion()
            );
        }
    }

    private Decision isVersionCompatibleAllocatingReplica(
        final RoutingNodes routingNodes,
        final String sourceNodeId,
        final RoutingNode target,
        final RoutingAllocation allocation
    ) {
        final RoutingNode source = routingNodes.node(sourceNodeId);
        if (target.node().getVersion().onOrAfter(source.node().getVersion())
            || isLuceneVersionCompatible(target.node().getVersion(), source.node().getVersion())) {
            /* we can allocate if we can recover from a node that is younger or on the same version, or if the
             * target's Lucene version can still read the source's segments (e.g. an OpenSearch patch-level
             * difference that shares the same Lucene major.minor). If the primary is already running a newer
             * Lucene major/minor that won't work due to possible differences in the lucene index format etc. */
            return allocation.decision(
                Decision.YES,
                NAME,
                "can allocate replica shard to a node with version [%s] since this is equal-or-newer than, or Lucene-compatible with, the primary version [%s]",
                target.node().getVersion(),
                source.node().getVersion()
            );
        } else {
            return allocation.decision(
                Decision.NO,
                NAME,
                "cannot allocate replica shard to a node with version [%s] since this is older than, and Lucene-incompatible with, the primary version [%s]",
                target.node().getVersion(),
                source.node().getVersion()
            );
        }
    }

    private Decision isVersionCompatible(
        SnapshotRecoverySource recoverySource,
        final RoutingNode target,
        final RoutingAllocation allocation
    ) {
        if (target.node().getVersion().onOrAfter(recoverySource.version())
            || isLuceneVersionCompatible(target.node().getVersion(), recoverySource.version())) {
            /* we can allocate if we can restore from a snapshot that is older or on the same version, or if the
             * target's Lucene version can still read the snapshot's segments. */
            return allocation.decision(
                Decision.YES,
                NAME,
                "node version [%s] is the same or newer than, or Lucene-compatible with, snapshot version [%s]",
                target.node().getVersion(),
                recoverySource.version()
            );
        } else {
            return allocation.decision(
                Decision.NO,
                NAME,
                "node version [%s] is older than, and Lucene-incompatible with, the snapshot version [%s]",
                target.node().getVersion(),
                recoverySource.version()
            );
        }
    }
}
