/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.Murmur3HashFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Maps a throttle bucket to the node that owns its cluster-level counter, using a consistent-hash ring. Every node
 * builds the same ring from the same {@link DiscoveryNodes}, so any coordinator finds the owner without a lookup, and a
 * node joining or leaving remaps only about {@code 1/N} of buckets.
 * <ul>
 *   <li><b>Data-capable nodes only</b> ({@link DiscoveryNode#isDataNode()}); dedicated cluster-manager nodes are
 *       excluded because a cluster may run few or none.</li>
 *   <li><b>Version-filtered:</b> nodes older than {@link #MIN_OWNER_VERSION} lack the handler and are excluded, so
 *       during a rolling upgrade every bucket lands on a node that can answer.</li>
 *   <li><b>{@link #VIRTUAL_NODES} virtual nodes</b> per node spread buckets evenly and redistribute a departing node's
 *       share across all survivors.</li>
 *   <li><b>Empty ring:</b> with no eligible node, {@link #ownerFor} is empty and the shared tier is unavailable.</li>
 * </ul>
 * <p>
 * Accepted rebalance effect: when a bucket moves, the former owner keeps the permits of requests already in flight
 * (and refuses new ones) while the new owner counts from zero, so the cluster can briefly admit up to ~2x
 * {@code shared_limit} until they drain. Conversely, a node that regains a bucket counts its retained records until
 * they drain.
 */
final class ThrottleOwnerSelector {

    /** First version that runs the shared-throttle handler and may therefore own buckets. */
    private static final Version MIN_OWNER_VERSION = Version.V_3_10_0;

    private static final int VIRTUAL_NODES = 128;

    // Ring position -> node id.
    private final NavigableMap<Integer, String> ring;
    private final Map<String, DiscoveryNode> nodesById;

    private ThrottleOwnerSelector(NavigableMap<Integer, String> ring, Map<String, DiscoveryNode> nodesById) {
        this.ring = ring;
        this.nodesById = nodesById;
    }

    /** Builds a ring from the eligible nodes (see the class javadoc). */
    static ThrottleOwnerSelector fromDiscoveryNodes(DiscoveryNodes discoveryNodes) {
        final NavigableMap<Integer, String> ring = new TreeMap<>();
        final Map<String, DiscoveryNode> nodesById = new HashMap<>();
        for (DiscoveryNode node : eligibleNodeSet(discoveryNodes)) {
            nodesById.put(node.getId(), node);
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                // On a hash collision keep the smaller node id, so every node resolves the point the same way.
                ring.merge(
                    Murmur3HashFunction.hash(node.getId() + "#" + i),
                    node.getId(),
                    (existingOwner, candidateOwner) -> existingOwner.compareTo(candidateOwner) <= 0 ? existingOwner : candidateOwner
                );
            }
        }
        return new ThrottleOwnerSelector(ring, nodesById);
    }

    /**
     * @return the node that owns the given bucket, or empty if the ring has no eligible node (shared tier unavailable).
     */
    Optional<DiscoveryNode> ownerFor(String bucketKey) {
        if (ring.isEmpty()) {
            return Optional.empty();
        }
        final int hash = Murmur3HashFunction.hash(bucketKey);
        // First ring point at or after the hash, wrapping around.
        Map.Entry<Integer, String> point = ring.ceilingEntry(hash);
        if (point == null) {
            point = ring.firstEntry();
        }
        return Optional.ofNullable(nodesById.get(point.getValue()));
    }

    /**
     * The eligible nodes in this snapshot, for deciding whether to rebuild. {@link DiscoveryNode#equals} compares by
     * ephemeral id, so a same-id restart is detected and the stale node replaced.
     */
    Set<DiscoveryNode> eligibleNodeSet() {
        return new HashSet<>(nodesById.values());
    }

    /** The eligible owner set for {@code discoveryNodes}, computed without building a ring. */
    static Set<DiscoveryNode> eligibleNodeSet(DiscoveryNodes discoveryNodes) {
        Set<DiscoveryNode> eligible = new HashSet<>();
        for (DiscoveryNode node : discoveryNodes.getDataNodes().values()) {
            if (node.getVersion().onOrAfter(MIN_OWNER_VERSION)) {
                eligible.add(node);
            }
        }
        return eligible;
    }
}
