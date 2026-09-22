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

import org.opensearch.cluster.node.DiscoveryNode;

import java.util.function.Predicate;

/**
 * Utility class to build a predicate that accepts cluster state changes
 *
 * @opensearch.internal
 */
public final class ClusterManagerNodeChangePredicate {

    private ClusterManagerNodeChangePredicate() {

    }

    /**
     * Builds a predicate that accepts a cluster state only if it was generated after the current node
     * (re-)joined the cluster manager. Extracts version and cluster-manager ID from the given state
     * and delegates to {@link #build(long, String)}.
     */
    public static Predicate<ClusterState> build(ClusterState currentState) {
        final long currentVersion = currentState.version();
        final DiscoveryNode clusterManagerNode = currentState.nodes().getClusterManagerNode();
        final String currentMasterId = clusterManagerNode == null ? null : clusterManagerNode.getEphemeralId();
        return build(currentVersion, currentMasterId);
    }

    /**
     * Builds a predicate that accepts a cluster state only if the cluster manager has changed
     * or the state version has increased beyond the provided version. Accepts pre-extracted
     * values to avoid retaining the full {@link ClusterState} in lambda closures.
     *
     * @param currentVersion  the cluster state version to compare against
     * @param currentMasterId the ephemeral ID of the current cluster manager node, or null if none
     * @return predicate that returns true when cluster manager changes or version increases
     */
    public static Predicate<ClusterState> build(long currentVersion, String currentMasterId) {
        return newState -> {
            final DiscoveryNode newClusterManager = newState.nodes().getClusterManagerNode();
            final boolean accept;
            if (newClusterManager == null) {
                accept = false;
            } else if (newClusterManager.getEphemeralId().equals(currentMasterId) == false) {
                accept = true;
            } else {
                accept = newState.version() > currentVersion;
            }
            return accept;
        };
    }
}
