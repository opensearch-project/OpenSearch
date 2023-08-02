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

package org.opensearch.cluster.node;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

public class DiscoveryNodesTests extends OpenSearchTestCase {

    public void testResolveNodeByIdOrName() {
        DiscoveryNodes discoveryNodes = buildDiscoveryNodes();
        DiscoveryNode[] nodes = discoveryNodes.getNodes().values().toArray(new DiscoveryNode[0]);
        DiscoveryNode node = randomFrom(nodes);
        DiscoveryNode resolvedNode = discoveryNodes.resolveNode(randomBoolean() ? node.getId() : node.getName());
        assertThat(resolvedNode.getId(), equalTo(node.getId()));
    }

    public void testResolveNodeByAttribute() {
        DiscoveryNodes discoveryNodes = buildDiscoveryNodes();
        NodeSelector nodeSelector = randomFrom(NodeSelector.values());
        Set<String> matchingNodeIds = nodeSelector.matchingNodeIds(discoveryNodes);
        try {
            DiscoveryNode resolvedNode = discoveryNodes.resolveNode(nodeSelector.selector);
            assertThat(matchingNodeIds.size(), equalTo(1));
            assertThat(resolvedNode.getId(), equalTo(matchingNodeIds.iterator().next()));
        } catch (IllegalArgumentException e) {
            if (matchingNodeIds.size() == 0) {
                assertThat(e.getMessage(), equalTo("failed to resolve [" + nodeSelector.selector + "], no matching nodes"));
            } else if (matchingNodeIds.size() > 1) {
                assertThat(e.getMessage(), containsString("where expected to be resolved to a single node"));
            } else {
                fail("resolveNode shouldn't have failed for [" + nodeSelector.selector + "]");
            }
        }
    }

    public void testResolveNodesNull() {
        DiscoveryNodes discoveryNodes = buildDiscoveryNodes();

        // if assertions are enabled (should be the case for tests, but not in production), resolving null throws
        expectThrows(AssertionError.class, () -> discoveryNodes.resolveNodes(Collections.singletonList(null).toArray(new String[0])));
        expectThrows(AssertionError.class, () -> discoveryNodes.resolveNodes(null, "someNode"));
        expectThrows(AssertionError.class, () -> discoveryNodes.resolveNodes("someNode", null, "someOtherNode"));
    }

    public void testAll() {
        final DiscoveryNodes discoveryNodes = buildDiscoveryNodes();

        final String[] allNodes = StreamSupport.stream(discoveryNodes.spliterator(), false)
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);
        assertThat(discoveryNodes.resolveNodes(), arrayContainingInAnyOrder(allNodes));
        assertThat(discoveryNodes.resolveNodes(new String[0]), arrayContainingInAnyOrder(allNodes));
        assertThat(discoveryNodes.resolveNodes("_all"), arrayContainingInAnyOrder(allNodes));

        final String[] nonClusterManagerNodes = StreamSupport.stream(discoveryNodes.getNodes().values().spliterator(), false)
            .filter(n -> n.isClusterManagerNode() == false)
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);
        assertThat(discoveryNodes.resolveNodes("_all", "cluster_manager:false"), arrayContainingInAnyOrder(nonClusterManagerNodes));

        assertThat(discoveryNodes.resolveNodes("cluster_manager:false", "_all"), arrayContainingInAnyOrder(allNodes));
    }

    public void testCoordinatorOnlyNodes() {
        final DiscoveryNodes discoveryNodes = buildDiscoveryNodes();

        final String[] coordinatorOnlyNodes = StreamSupport.stream(discoveryNodes.getNodes().values().spliterator(), false)
            .filter(n -> n.isDataNode() == false && n.isIngestNode() == false && n.isClusterManagerNode() == false)
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);

        final String[] nonCoordinatorOnlyNodes = StreamSupport.stream(discoveryNodes.getNodes().values().spliterator(), false)
            .filter(n -> n.isClusterManagerNode() || n.isDataNode() || n.isIngestNode())
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);

        assertThat(discoveryNodes.resolveNodes("coordinating_only:true"), arrayContainingInAnyOrder(coordinatorOnlyNodes));
        assertThat(
            discoveryNodes.resolveNodes("_all", "data:false", "ingest:false", "cluster_manager:false"),
            arrayContainingInAnyOrder(coordinatorOnlyNodes)
        );
        assertThat(discoveryNodes.resolveNodes("_all", "coordinating_only:false"), arrayContainingInAnyOrder(nonCoordinatorOnlyNodes));
    }

    public void testResolveNodesIds() {
        DiscoveryNodes discoveryNodes = buildDiscoveryNodes();

        int numSelectors = randomIntBetween(1, 5);
        Set<String> nodeSelectors = new HashSet<>();
        Set<String> expectedNodeIdsSet = new HashSet<>();
        for (int i = 0; i < numSelectors; i++) {
            NodeSelector nodeSelector = randomFrom(NodeSelector.values());
            if (nodeSelectors.add(nodeSelector.selector)) {
                expectedNodeIdsSet.addAll(nodeSelector.matchingNodeIds(discoveryNodes));
            }
        }
        int numNodeIds = randomIntBetween(0, 3);
        String[] nodeIds = discoveryNodes.getNodes().keySet().toArray(new String[0]);
        for (int i = 0; i < numNodeIds; i++) {
            String nodeId = randomFrom(nodeIds);
            nodeSelectors.add(nodeId);
            expectedNodeIdsSet.add(nodeId);
        }
        int numNodeNames = randomIntBetween(0, 3);
        DiscoveryNode[] nodes = discoveryNodes.getNodes().values().toArray(new DiscoveryNode[0]);
        for (int i = 0; i < numNodeNames; i++) {
            DiscoveryNode discoveryNode = randomFrom(nodes);
            nodeSelectors.add(discoveryNode.getName());
            expectedNodeIdsSet.add(discoveryNode.getId());
        }

        String[] resolvedNodesIds = discoveryNodes.resolveNodes(nodeSelectors.toArray(new String[0]));
        Arrays.sort(resolvedNodesIds);
        String[] expectedNodesIds = expectedNodeIdsSet.toArray(new String[0]);
        Arrays.sort(expectedNodesIds);
        assertThat(resolvedNodesIds, equalTo(expectedNodesIds));
    }

    public void testClusterManagersFirst() {
        final List<DiscoveryNode> inputNodes = randomNodes(10);
        final DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        inputNodes.forEach(discoBuilder::add);
        final List<DiscoveryNode> returnedNodes = discoBuilder.build().clusterManagersFirstStream().collect(Collectors.toList());
        assertEquals(returnedNodes.size(), inputNodes.size());
        assertEquals(new HashSet<>(returnedNodes), new HashSet<>(inputNodes));
        final List<DiscoveryNode> sortedNodes = new ArrayList<>(returnedNodes);
        Collections.sort(sortedNodes, Comparator.comparing(n -> n.isClusterManagerNode() == false));
        assertEquals(sortedNodes, returnedNodes);
    }

    public void testDeltaListsMultipleNodes() {
        final List<DiscoveryNode> discoveryNodes = randomNodes(3);

        final DiscoveryNodes nodes0 = DiscoveryNodes.builder().add(discoveryNodes.get(0)).build();
        final DiscoveryNodes nodes01 = DiscoveryNodes.builder(nodes0).add(discoveryNodes.get(1)).build();
        final DiscoveryNodes nodes012 = DiscoveryNodes.builder(nodes01).add(discoveryNodes.get(2)).build();

        assertThat(nodes01.delta(nodes0).shortSummary(), equalTo("added {" + discoveryNodes.get(1) + "}"));
        assertThat(
            nodes012.delta(nodes0).shortSummary(),
            oneOf(
                "added {" + discoveryNodes.get(1) + "," + discoveryNodes.get(2) + "}",
                "added {" + discoveryNodes.get(2) + "," + discoveryNodes.get(1) + "}"
            )
        );

        assertThat(nodes0.delta(nodes01).shortSummary(), equalTo("removed {" + discoveryNodes.get(1) + "}"));
        assertThat(
            nodes0.delta(nodes012).shortSummary(),
            oneOf(
                "removed {" + discoveryNodes.get(1) + "," + discoveryNodes.get(2) + "}",
                "removed {" + discoveryNodes.get(2) + "," + discoveryNodes.get(1) + "}"
            )
        );

        final DiscoveryNodes nodes01Local = DiscoveryNodes.builder(nodes01).localNodeId(discoveryNodes.get(1).getId()).build();
        final DiscoveryNodes nodes02Local = DiscoveryNodes.builder(nodes012).localNodeId(discoveryNodes.get(1).getId()).build();

        assertThat(nodes01Local.delta(nodes0).shortSummary(), equalTo(""));
        assertThat(nodes02Local.delta(nodes0).shortSummary(), equalTo("added {" + discoveryNodes.get(2) + "}"));

        assertThat(nodes0.delta(nodes01Local).shortSummary(), equalTo("removed {" + discoveryNodes.get(1) + "}"));
        assertThat(
            nodes0.delta(nodes02Local).shortSummary(),
            oneOf(
                "removed {" + discoveryNodes.get(1) + "," + discoveryNodes.get(2) + "}",
                "removed {" + discoveryNodes.get(2) + "," + discoveryNodes.get(1) + "}"
            )
        );
    }

    public void testDeltas() {
        Set<DiscoveryNode> nodesA = new HashSet<>();
        nodesA.addAll(randomNodes(1 + randomInt(10)));
        Set<DiscoveryNode> nodesB = new HashSet<>();
        nodesB.addAll(randomNodes(1 + randomInt(5)));
        for (DiscoveryNode node : randomSubsetOf(nodesA)) {
            if (randomBoolean()) {
                // change an attribute
                Map<String, String> attrs = new HashMap<>(node.getAttributes());
                attrs.put("new", "new");
                final TransportAddress nodeAddress = node.getAddress();
                node = new DiscoveryNode(
                    node.getName(),
                    node.getId(),
                    node.getEphemeralId(),
                    nodeAddress.address().getHostString(),
                    nodeAddress.getAddress(),
                    nodeAddress,
                    attrs,
                    node.getRoles(),
                    node.getVersion()
                );
            }
            nodesB.add(node);
        }

        DiscoveryNode clusterManagerA = randomBoolean() ? null : RandomPicks.randomFrom(random(), nodesA);
        DiscoveryNode clusterManagerB = randomBoolean() ? null : RandomPicks.randomFrom(random(), nodesB);

        DiscoveryNodes.Builder builderA = DiscoveryNodes.builder();
        nodesA.stream().forEach(builderA::add);
        final String clusterManagerAId = clusterManagerA == null ? null : clusterManagerA.getId();
        builderA.clusterManagerNodeId(clusterManagerAId);
        builderA.localNodeId(RandomPicks.randomFrom(random(), nodesA).getId());

        DiscoveryNodes.Builder builderB = DiscoveryNodes.builder();
        nodesB.stream().forEach(builderB::add);
        final String clusterManagerBId = clusterManagerB == null ? null : clusterManagerB.getId();
        builderB.clusterManagerNodeId(clusterManagerBId);
        builderB.localNodeId(RandomPicks.randomFrom(random(), nodesB).getId());

        final DiscoveryNodes discoNodesA = builderA.build();
        final DiscoveryNodes discoNodesB = builderB.build();
        logger.info("nodes A: {}", discoNodesA);
        logger.info("nodes B: {}", discoNodesB);

        DiscoveryNodes.Delta delta = discoNodesB.delta(discoNodesA);

        if (clusterManagerA == null) {
            assertThat(delta.previousClusterManagerNode(), nullValue());
        } else {
            assertThat(delta.previousClusterManagerNode().getId(), equalTo(clusterManagerAId));
        }
        if (clusterManagerB == null) {
            assertThat(delta.newClusterManagerNode(), nullValue());
        } else {
            assertThat(delta.newClusterManagerNode().getId(), equalTo(clusterManagerBId));
        }

        if (Objects.equals(clusterManagerAId, clusterManagerBId)) {
            assertFalse(delta.clusterManagerNodeChanged());
        } else {
            assertTrue(delta.clusterManagerNodeChanged());
        }

        Set<DiscoveryNode> newNodes = new HashSet<>(nodesB);
        newNodes.removeAll(nodesA);
        assertThat(delta.added(), equalTo(newNodes.isEmpty() == false));
        assertThat(delta.addedNodes(), containsInAnyOrder(newNodes.stream().collect(Collectors.toList()).toArray()));
        assertThat(delta.addedNodes().size(), equalTo(newNodes.size()));

        Set<DiscoveryNode> removedNodes = new HashSet<>(nodesA);
        removedNodes.removeAll(nodesB);
        assertThat(delta.removed(), equalTo(removedNodes.isEmpty() == false));
        assertThat(delta.removedNodes(), containsInAnyOrder(removedNodes.stream().collect(Collectors.toList()).toArray()));
        assertThat(delta.removedNodes().size(), equalTo(removedNodes.size()));
    }

    // Validate using the deprecated 'master' role in the node filter can get correct result.
    public void testDeprecatedMasterNodeFilter() {
        final DiscoveryNodes discoveryNodes = buildDiscoveryNodes();

        final String[] allNodes = StreamSupport.stream(discoveryNodes.spliterator(), false)
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);

        final String[] clusterManagerNodes = StreamSupport.stream(discoveryNodes.getNodes().values().spliterator(), false)
            .filter(n -> n.isClusterManagerNode() == true)
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);

        final String[] nonClusterManagerNodes = StreamSupport.stream(discoveryNodes.getNodes().values().spliterator(), false)
            .filter(n -> n.isClusterManagerNode() == false)
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);

        assertThat(discoveryNodes.resolveNodes("cluster_manager:true"), arrayContainingInAnyOrder(clusterManagerNodes));
        assertThat(discoveryNodes.resolveNodes("master:true"), arrayContainingInAnyOrder(clusterManagerNodes));
        assertThat(discoveryNodes.resolveNodes("_all", "master:false"), arrayContainingInAnyOrder(nonClusterManagerNodes));
        assertThat(discoveryNodes.resolveNodes("master:false", "_all"), arrayContainingInAnyOrder(allNodes));
    }

    private static AtomicInteger idGenerator = new AtomicInteger();

    private static List<DiscoveryNode> randomNodes(final int numNodes) {
        List<DiscoveryNode> nodesList = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            if (frequently()) {
                attributes.put("custom", randomBoolean() ? "match" : randomAlphaOfLengthBetween(3, 5));
            }
            final Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
            if (frequently()) {
                roles.add(new DiscoveryNodeRole("custom_role", "cr") {

                    @Override
                    public Setting<Boolean> legacySetting() {
                        return null;
                    }

                });
            }
            final DiscoveryNode node = newNode(idGenerator.getAndIncrement(), attributes, roles);
            nodesList.add(node);
        }
        return nodesList;
    }

    private static DiscoveryNodes buildDiscoveryNodes() {
        int numNodes = randomIntBetween(1, 10);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        List<DiscoveryNode> nodesList = randomNodes(numNodes);
        for (DiscoveryNode node : nodesList) {
            discoBuilder = discoBuilder.add(node);
        }
        discoBuilder.localNodeId(randomFrom(nodesList).getId());
        discoBuilder.clusterManagerNodeId(randomFrom(nodesList).getId());
        return discoBuilder.build();
    }

    private static DiscoveryNode newNode(int nodeId, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        return new DiscoveryNode("name_" + nodeId, "node_" + nodeId, buildNewFakeTransportAddress(), attributes, roles, Version.CURRENT);
    }

    private enum NodeSelector {
        LOCAL("_local") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                return Collections.singleton(nodes.getLocalNodeId());
            }
        },
        ELECTED_MASTER("_master") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                return Collections.singleton(nodes.getClusterManagerNodeId());
            }
        },
        // TODO: Remove this element after removing DiscoveryNodeRole.MASTER_ROLE
        MASTER_ELIGIBLE(DiscoveryNodeRole.MASTER_ROLE.roleName() + ":true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                Set<String> ids = new HashSet<>();
                nodes.getClusterManagerNodes().keySet().iterator().forEachRemaining(ids::add);
                return ids;
            }
        },
        CLUSTER_MANAGER_ELIGIBLE(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE.roleName() + ":true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                Set<String> ids = new HashSet<>();
                nodes.getClusterManagerNodes().keySet().iterator().forEachRemaining(ids::add);
                return ids;
            }
        },
        DATA(DiscoveryNodeRole.DATA_ROLE.roleName() + ":true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                Set<String> ids = new HashSet<>();
                nodes.getDataNodes().keySet().iterator().forEachRemaining(ids::add);
                return ids;
            }
        },
        INGEST(DiscoveryNodeRole.INGEST_ROLE.roleName() + ":true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                Set<String> ids = new HashSet<>();
                nodes.getIngestNodes().keySet().iterator().forEachRemaining(ids::add);
                return ids;
            }
        },
        COORDINATING_ONLY(DiscoveryNode.COORDINATING_ONLY + ":true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                Set<String> ids = new HashSet<>();
                nodes.getCoordinatingOnlyNodes().keySet().iterator().forEachRemaining(ids::add);
                return ids;
            }
        },
        CUSTOM_ATTRIBUTE("attr:value") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                Set<String> ids = new HashSet<>();
                nodes.getNodes().values().iterator().forEachRemaining(node -> {
                    if ("value".equals(node.getAttributes().get("attr"))) {
                        ids.add(node.getId());
                    }
                });
                return ids;
            }
        },
        CUSTOM_ROLE("custom_role:true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                Set<String> ids = new HashSet<>();
                nodes.getNodes().values().iterator().forEachRemaining(node -> {
                    if (node.getRoles().stream().anyMatch(role -> role.roleName().equals("custom_role"))) {
                        ids.add(node.getId());
                    }
                });
                return ids;
            }
        };

        private final String selector;

        NodeSelector(String selector) {
            this.selector = selector;
        }

        abstract Set<String> matchingNodeIds(DiscoveryNodes nodes);
    }

    public void testMaxMinNodeVersion() {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        discoBuilder.add(
            new DiscoveryNode(
                "name_" + 1,
                "node_" + 1,
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES)),
                LegacyESVersion.fromString("5.1.0")
            )
        );
        discoBuilder.add(
            new DiscoveryNode(
                "name_" + 2,
                "node_" + 2,
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES)),
                LegacyESVersion.fromString("6.3.0")
            )
        );
        discoBuilder.add(
            new DiscoveryNode(
                "name_" + 3,
                "node_" + 3,
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES)),
                Version.fromString("1.1.0")
            )
        );
        discoBuilder.localNodeId("name_1");
        discoBuilder.clusterManagerNodeId("name_2");
        DiscoveryNodes build = discoBuilder.build();
        assertEquals(Version.fromString("1.1.0"), build.getMaxNodeVersion());
        assertEquals(LegacyESVersion.fromString("5.1.0"), build.getMinNodeVersion());
    }

    private DiscoveryNode buildDiscoveryNodeFromExisting(DiscoveryNode existing, Version newVersion) {
        return new DiscoveryNode(
            existing.getName(),
            existing.getId(),
            existing.getEphemeralId(),
            existing.getHostName(),
            existing.getHostAddress(),
            existing.getAddress(),
            existing.getAttributes(),
            existing.getRoles(),
            newVersion
        );
    }
}
