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
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ThrottleOwnerSelectorTests extends OpenSearchTestCase {

    private static DiscoveryNode dataNode(String id, Version version) {
        return new DiscoveryNode(
            "name_" + id,
            id,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            version
        );
    }

    private static DiscoveryNode managerOnlyNode(String id) {
        return nodeWithRoles(id, Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE));
    }

    private static DiscoveryNode nodeWithRoles(String id, Set<DiscoveryNodeRole> roles) {
        return new DiscoveryNode("name_" + id, id, buildNewFakeTransportAddress(), Collections.emptyMap(), roles, Version.CURRENT);
    }

    private static DiscoveryNodes nodesOf(DiscoveryNode... nodes) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (DiscoveryNode node : nodes) {
            builder.add(node);
        }
        builder.localNodeId(nodes[0].getId());
        return builder.build();
    }

    public void testEmptyRingWhenNoDataNodes() {
        ThrottleOwnerSelector selector = ThrottleOwnerSelector.fromDiscoveryNodes(nodesOf(managerOnlyNode("m1")));
        assertFalse("empty ring must yield no owner (shared tier unavailable)", selector.ownerFor("group-a:group").isPresent());
    }

    public void testClusterManagerOnlyNodesExcluded() {
        ThrottleOwnerSelector selector = ThrottleOwnerSelector.fromDiscoveryNodes(
            nodesOf(dataNode("d1", Version.CURRENT), managerOnlyNode("m1"))
        );
        // Only the data node is eligible; the manager-only node must never be an owner.
        for (int i = 0; i < 200; i++) {
            assertEquals("d1", selector.ownerFor("bucket-" + i).orElseThrow().getId());
        }
    }

    public void testWarmAndSearchNodesEligibleAndCoordinatingOnlyExcluded() {
        DiscoveryNode warm = nodeWithRoles("warm", Set.of(DiscoveryNodeRole.WARM_ROLE));
        DiscoveryNode search = nodeWithRoles("search", Set.of(DiscoveryNodeRole.SEARCH_ROLE));
        DiscoveryNode coordinatingOnly = nodeWithRoles("coord", Collections.emptySet());
        ThrottleOwnerSelector selector = ThrottleOwnerSelector.fromDiscoveryNodes(nodesOf(coordinatingOnly, warm, search));
        // Warm and search nodes can hold data, so they may own buckets; a no-role (coordinating-only) node never does.
        assertEquals(Set.of(warm, search), selector.eligibleNodeSet());
        for (int i = 0; i < 200; i++) {
            assertNotEquals("coord", selector.ownerFor("bucket-" + i).orElseThrow().getId());
        }
    }

    public void testOldVersionNodesExcludedFromRing() {
        DiscoveryNode newNode = dataNode("new", Version.CURRENT);
        DiscoveryNode oldNode = dataNode("old", Version.V_3_9_0);
        ThrottleOwnerSelector selector = ThrottleOwnerSelector.fromDiscoveryNodes(nodesOf(newNode, oldNode));
        assertEquals(1, selector.eligibleNodeSet().size());
        for (int i = 0; i < 200; i++) {
            assertEquals("only the upgraded node may own buckets", "new", selector.ownerFor("bucket-" + i).orElseThrow().getId());
        }
    }

    public void testDeterministicForSameNodeSet() {
        DiscoveryNodes nodes = nodesOf(dataNode("a", Version.CURRENT), dataNode("b", Version.CURRENT), dataNode("c", Version.CURRENT));
        ThrottleOwnerSelector s1 = ThrottleOwnerSelector.fromDiscoveryNodes(nodes);
        ThrottleOwnerSelector s2 = ThrottleOwnerSelector.fromDiscoveryNodes(nodes);
        for (int i = 0; i < 500; i++) {
            String key = "bucket-" + i;
            assertEquals(s1.ownerFor(key).orElseThrow().getId(), s2.ownerFor(key).orElseThrow().getId());
        }
    }

    public void testVirtualNodeHashCollisionIsDeterministic() {
        String bucketKey = "node-1396#121";
        assertEquals(Murmur3HashFunction.hash(bucketKey), Murmur3HashFunction.hash("node-2422#63"));

        // The ring iterates a HashSet of nodes hashed by their random ephemeral ids, so builder insertion order alone does
        // not control which colliding node is merged first. Rebuild with many fresh instances (new ephemeral ids, both
        // insertion orders) so both merge orders occur, and require the same tie-break winner every time.
        for (int i = 0; i < 64; i++) {
            DiscoveryNode first = dataNode("node-1396", Version.CURRENT);
            DiscoveryNode second = dataNode("node-2422", Version.CURRENT);
            DiscoveryNodes nodes = i % 2 == 0 ? nodesOf(first, second) : nodesOf(second, first);
            assertEquals("node-1396", ThrottleOwnerSelector.fromDiscoveryNodes(nodes).ownerFor(bucketKey).orElseThrow().getId());
        }
    }

    public void testDistributionAcrossNodesIsReasonablyBalanced() {
        ThrottleOwnerSelector selector = ThrottleOwnerSelector.fromDiscoveryNodes(
            nodesOf(dataNode("a", Version.CURRENT), dataNode("b", Version.CURRENT), dataNode("c", Version.CURRENT))
        );
        Map<String, Integer> counts = new HashMap<>();
        int n = 3000;
        for (int i = 0; i < n; i++) {
            String owner = selector.ownerFor("bucket-" + i).orElseThrow().getId();
            counts.merge(owner, 1, Integer::sum);
        }
        // With 128 virtual nodes each, every physical node should get a non-trivial share (well above 10%).
        assertEquals(3, counts.size());
        for (int c : counts.values()) {
            assertTrue("each node should own a meaningful share, saw " + counts, c > n / 10);
        }
    }

    public void testRebalanceOnlyRemapsSmallFraction() {
        DiscoveryNode a = dataNode("a", Version.CURRENT);
        DiscoveryNode b = dataNode("b", Version.CURRENT);
        DiscoveryNode c = dataNode("c", Version.CURRENT);
        DiscoveryNode d = dataNode("d", Version.CURRENT);
        ThrottleOwnerSelector before = ThrottleOwnerSelector.fromDiscoveryNodes(nodesOf(a, b, c, d));
        ThrottleOwnerSelector after = ThrottleOwnerSelector.fromDiscoveryNodes(nodesOf(a, b, c)); // d leaves

        int n = 5000;
        int remapped = 0;
        for (int i = 0; i < n; i++) {
            String key = "bucket-" + i;
            String ownerBefore = before.ownerFor(key).orElseThrow().getId();
            String ownerAfter = after.ownerFor(key).orElseThrow().getId();
            if (ownerBefore.equals(ownerAfter) == false) {
                remapped++;
            }
        }
        // Removing 1 of 4 nodes should remap roughly 1/4 of keys. Allow generous slack, but it must be far from "all".
        double fraction = (double) remapped / n;
        assertTrue("remap fraction should be well under half, was " + fraction, fraction < 0.45);
        // Keys not owned by the departed node 'd' should mostly keep their owner.
        for (int i = 0; i < n; i++) {
            String key = "bucket-" + i;
            if (before.ownerFor(key).orElseThrow().getId().equals("d") == false) {
                assertEquals(
                    "survivor-owned keys must not move",
                    before.ownerFor(key).orElseThrow().getId(),
                    after.ownerFor(key).orElseThrow().getId()
                );
            }
        }
    }
}
