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

package org.opensearch.cluster;

import org.opensearch.cluster.metadata.IndexGraveyard;
import org.opensearch.cluster.metadata.IndexGraveyard.IndexGraveyardDiff;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.index.Index;
import org.opensearch.gateway.GatewayService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An event received by the local node, signaling that the cluster state has changed.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterChangedEvent {

    private final String source;

    private final ClusterState previousState;

    private final ClusterState state;

    private final DiscoveryNodes.Delta nodesDelta;

    public ClusterChangedEvent(String source, ClusterState state, ClusterState previousState) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(previousState, "previousState must not be null");
        this.source = source;
        this.state = state;
        this.previousState = previousState;
        this.nodesDelta = state.nodes().delta(previousState.nodes());
    }

    /**
     * The source that caused this cluster event to be raised.
     */
    public String source() {
        return this.source;
    }

    /**
     * The new cluster state that caused this change event.
     */
    public ClusterState state() {
        return this.state;
    }

    /**
     * The previous cluster state for this change event.
     */
    public ClusterState previousState() {
        return this.previousState;
    }

    /**
     * Returns <code>true</code> iff the routing tables (for all indices) have
     * changed between the previous cluster state and the current cluster state.
     * Note that this is an object reference equality test, not an equals test.
     */
    public boolean routingTableChanged() {
        return state.routingTable() != previousState.routingTable();
    }

    /**
     * Returns <code>true</code> iff the routing table has changed for the given index.
     * Note that this is an object reference equality test, not an equals test.
     */
    public boolean indexRoutingTableChanged(String index) {
        Objects.requireNonNull(index, "index must not be null");
        if (!state.routingTable().hasIndex(index) && !previousState.routingTable().hasIndex(index)) {
            return false;
        }
        if (state.routingTable().hasIndex(index) && previousState.routingTable().hasIndex(index)) {
            return state.routingTable().index(index) != previousState.routingTable().index(index);
        }
        return true;
    }

    /**
     * Returns the indices created in this event
     */
    public List<String> indicesCreated() {
        if (!metadataChanged()) {
            return Collections.emptyList();
        }
        List<String> created = null;
        for (final String index : state.metadata().indices().keySet()) {
            if (!previousState.metadata().hasIndex(index)) {
                if (created == null) {
                    created = new ArrayList<>();
                }
                created.add(index);
            }
        }
        return created == null ? Collections.<String>emptyList() : created;
    }

    /**
     * Returns the indices deleted in this event
     */
    public List<Index> indicesDeleted() {
        if (previousState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // working off of a non-initialized previous state, so use the tombstones for index deletions
            return indicesDeletedFromTombstones();
        } else {
            // examine the diffs in index metadata between the previous and new cluster states to get the deleted indices
            return indicesDeletedFromClusterState();
        }
    }

    /**
     * Returns <code>true</code> iff the metadata for the cluster has changed between
     * the previous cluster state and the new cluster state. Note that this is an object
     * reference equality test, not an equals test.
     */
    public boolean metadataChanged() {
        return state.metadata() != previousState.metadata();
    }

    /**
     * Returns a set of custom meta data types when any custom metadata for the cluster has changed
     * between the previous cluster state and the new cluster state. custom meta data types are
     * returned iff they have been added, updated or removed between the previous and the current state
     */
    public Set<String> changedCustomMetadataSet() {
        Set<String> result = new HashSet<>();
        Map<String, Metadata.Custom> currentCustoms = state.metadata().customs();
        Map<String, Metadata.Custom> previousCustoms = previousState.metadata().customs();
        if (currentCustoms.equals(previousCustoms) == false) {
            for (Map.Entry<String, Metadata.Custom> currentCustomMetadata : currentCustoms.entrySet()) {
                // new custom md added or existing custom md changed
                if (previousCustoms.containsKey(currentCustomMetadata.getKey()) == false
                    || currentCustomMetadata.getValue().equals(previousCustoms.get(currentCustomMetadata.getKey())) == false) {
                    result.add(currentCustomMetadata.getKey());
                }
            }
            // existing custom md deleted
            for (Map.Entry<String, Metadata.Custom> previousCustomMetadata : previousCustoms.entrySet()) {
                if (currentCustoms.containsKey(previousCustomMetadata.getKey()) == false) {
                    result.add(previousCustomMetadata.getKey());
                }
            }
        }
        return result;
    }

    /**
     * Returns <code>true</code> iff the {@link IndexMetadata} for a given index
     * has changed between the previous cluster state and the new cluster state.
     * Note that this is an object reference equality test, not an equals test.
     */
    public static boolean indexMetadataChanged(IndexMetadata metadata1, IndexMetadata metadata2) {
        assert metadata1 != null && metadata2 != null;
        // no need to check on version, since disco modules will make sure to use the
        // same instance if its a version match
        return metadata1 != metadata2;
    }

    /**
     * Returns <code>true</code> iff the cluster level blocks have changed between cluster states.
     * Note that this is an object reference equality test, not an equals test.
     */
    public boolean blocksChanged() {
        return state.blocks() != previousState.blocks();
    }

    /**
     * Returns <code>true</code> iff the local node is the mater node of the cluster.
     */
    public boolean localNodeClusterManager() {
        return state.nodes().isLocalNodeElectedClusterManager();
    }

    /**
     * Returns the {@link org.opensearch.cluster.node.DiscoveryNodes.Delta} between
     * the previous cluster state and the new cluster state.
     */
    public DiscoveryNodes.Delta nodesDelta() {
        return this.nodesDelta;
    }

    /**
     * Returns <code>true</code> iff nodes have been removed from the cluster since the last cluster state.
     */
    public boolean nodesRemoved() {
        return nodesDelta.removed();
    }

    /**
     * Returns <code>true</code> iff nodes have been added from the cluster since the last cluster state.
     */
    public boolean nodesAdded() {
        return nodesDelta.added();
    }

    /**
     * Returns <code>true</code> iff nodes have been changed (added or removed) from the cluster since the last cluster state.
     */
    public boolean nodesChanged() {
        return nodesRemoved() || nodesAdded();
    }

    /**
     * Determines whether or not the current cluster state represents an entirely
     * new cluster, either when a node joins a cluster for the first time or when
     * the node receives a cluster state update from a brand new cluster (different
     * UUID from the previous cluster), which will happen when a cluster-manager node is
     * elected that has never been part of the cluster before.
     */
    public boolean isNewCluster() {
        final String prevClusterUUID = previousState.metadata().clusterUUID();
        final String currClusterUUID = state.metadata().clusterUUID();
        return prevClusterUUID.equals(currClusterUUID) == false;
    }

    // Get the deleted indices by comparing the index metadatas in the previous and new cluster states.
    // If an index exists in the previous cluster state, but not in the new cluster state, it must have been deleted.
    private List<Index> indicesDeletedFromClusterState() {
        // If the new cluster state has a new cluster UUID, the likely scenario is that a node was elected cluster-manager
        // that has had its data directory wiped out, in which case we don't want to delete the indices and lose data;
        // rather we want to import them as dangling indices instead. So we check here if the cluster UUID differs from the previous
        // cluster UUID, in which case, we don't want to delete indices that the cluster-manager erroneously believes shouldn't exist.
        // See test DiscoveryWithServiceDisruptionsIT.testIndicesDeleted()
        // See discussion on https://github.com/elastic/elasticsearch/pull/9952 and
        // https://github.com/elastic/elasticsearch/issues/11665
        if (metadataChanged() == false || isNewCluster()) {
            return Collections.emptyList();
        }
        Set<Index> deleted = null;
        final Metadata previousMetadata = previousState.metadata();
        final Metadata currentMetadata = state.metadata();

        for (final IndexMetadata index : previousMetadata.indices().values()) {
            IndexMetadata current = currentMetadata.index(index.getIndex());
            if (current == null) {
                if (deleted == null) {
                    deleted = new HashSet<>();
                }
                deleted.add(index.getIndex());
            }
        }

        final IndexGraveyard currentGraveyard = currentMetadata.indexGraveyard();
        final IndexGraveyard previousGraveyard = previousMetadata.indexGraveyard();

        // Look for new entries in the index graveyard, where there's no corresponding index in the
        // previous metadata. This indicates that a dangling index has been explicitly deleted, so
        // each node should make sure to delete any related data.
        if (currentGraveyard != previousGraveyard) {
            final IndexGraveyardDiff indexGraveyardDiff = (IndexGraveyardDiff) currentGraveyard.diff(previousGraveyard);

            final List<IndexGraveyard.Tombstone> added = indexGraveyardDiff.getAdded();

            if (added.isEmpty() == false) {
                if (deleted == null) {
                    deleted = new HashSet<>();
                }
                for (IndexGraveyard.Tombstone tombstone : added) {
                    deleted.add(tombstone.getIndex());
                }
            }
        }

        return deleted == null ? Collections.<Index>emptyList() : new ArrayList<>(deleted);
    }

    private List<Index> indicesDeletedFromTombstones() {
        // We look at the full tombstones list to see which indices need to be deleted. In the case of
        // a valid previous cluster state, indicesDeletedFromClusterState() will be used to get the deleted
        // list, so a diff doesn't make sense here. When a node (re)joins the cluster, its possible for it
        // to re-process the same deletes or process deletes about indices it never knew about. This is not
        // an issue because there are safeguards in place in the delete store operation in case the index
        // folder doesn't exist on the file system.
        List<IndexGraveyard.Tombstone> tombstones = state.metadata().indexGraveyard().getTombstones();
        return tombstones.stream().map(IndexGraveyard.Tombstone::getIndex).collect(Collectors.toList());
    }

}
