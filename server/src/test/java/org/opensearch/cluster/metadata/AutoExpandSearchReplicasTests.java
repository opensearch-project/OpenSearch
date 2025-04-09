/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.Settings;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

public class AutoExpandSearchReplicasTests extends OpenSearchAllocationTestCase {

    public void testParseAutoExpandSearchReplicaSettings() {
        AutoExpandSearchReplicas autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(
            Settings.builder().put("index.auto_expand_search_replicas", "0-5").build()
        );
        assertEquals(0, autoExpandSearchReplicas.getMinSearchReplicas());
        assertEquals(5, autoExpandSearchReplicas.getMaxSearchReplicas());

        autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(
            Settings.builder().put("index.auto_expand_search_replicas", "0-all").build()
        );
        assertEquals(0, autoExpandSearchReplicas.getMinSearchReplicas());
        assertEquals(Integer.MAX_VALUE, autoExpandSearchReplicas.getMaxSearchReplicas());

        autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(
            Settings.builder().put("index.auto_expand_search_replicas", "1-all").build()
        );
        assertEquals(1, autoExpandSearchReplicas.getMinSearchReplicas());
        assertEquals(Integer.MAX_VALUE, autoExpandSearchReplicas.getMaxSearchReplicas());
    }

    public void testInvalidValues() {
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> {
            AutoExpandSearchReplicas.SETTING.get(Settings.builder().put("index.auto_expand_search_replicas", "boom").build());
        });
        assertEquals("failed to parse [index.auto_expand_search_replicas] from value: [boom] at index -1", throwable.getMessage());

        throwable = assertThrows(IllegalArgumentException.class, () -> {
            AutoExpandSearchReplicas.SETTING.get(Settings.builder().put("index.auto_expand_search_replicas", "1-boom").build());
        });
        assertEquals("failed to parse [index.auto_expand_search_replicas] from value: [1-boom] at index 1", throwable.getMessage());
        assertEquals("For input string: \"boom\"", throwable.getCause().getMessage());

        throwable = assertThrows(IllegalArgumentException.class, () -> {
            AutoExpandSearchReplicas.SETTING.get(Settings.builder().put("index.auto_expand_search_replicas", "boom-1").build());
        });
        assertEquals("failed to parse [index.auto_expand_search_replicas] from value: [boom-1] at index 4", throwable.getMessage());
        assertEquals("For input string: \"boom\"", throwable.getCause().getMessage());

        throwable = assertThrows(IllegalArgumentException.class, () -> {
            AutoExpandSearchReplicas.SETTING.get(Settings.builder().put("index.auto_expand_search_replicas", "2-1").build());
        });
        assertEquals(
            "[index.auto_expand_search_replicas] minSearchReplicas must be =< maxSearchReplicas but wasn't 2 > 1",
            throwable.getMessage()
        );
    }

    public void testCalculateNumberOfSearchReplicas() {
        // when the number of matching search nodes is lesser than the maximum value of auto-expand
        AutoExpandSearchReplicas autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(
            Settings.builder().put("index.auto_expand_search_replicas", "0-all").build()
        );
        assertEquals(OptionalInt.of(5), autoExpandSearchReplicas.calculateNumberOfSearchReplicas(5));

        // when the number of matching search nodes is equal to the maximum value of auto-expand
        autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(
            Settings.builder().put("index.auto_expand_search_replicas", "0-5").build()
        );
        assertEquals(OptionalInt.of(5), autoExpandSearchReplicas.calculateNumberOfSearchReplicas(5));

        // when the number of matching search nodes is equal to the minimum value of auto-expand
        autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(
            Settings.builder().put("index.auto_expand_search_replicas", "0-5").build()
        );
        assertEquals(OptionalInt.of(0), autoExpandSearchReplicas.calculateNumberOfSearchReplicas(0));

        // when the number of matching search nodes is greater than the maximum value of auto-expand
        autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(
            Settings.builder().put("index.auto_expand_search_replicas", "0-5").build()
        );
        assertEquals(OptionalInt.of(5), autoExpandSearchReplicas.calculateNumberOfSearchReplicas(8));

        // when the number of matching search nodes is lesser than the minimum value of auto-expand,
        // then the number of search replicas remains unchanged
        autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(
            Settings.builder().put("index.auto_expand_search_replicas", "2-5").build()
        );
        assertEquals(OptionalInt.empty(), autoExpandSearchReplicas.calculateNumberOfSearchReplicas(1));
    }

    public void testGetAutoExpandReplicaChanges() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT).put("index.auto_expand_search_replicas", "0-all"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .numberOfSearchReplicas(1)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Collections.emptyMap(), SEARCH_ROLE, Version.CURRENT))
                    .add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Collections.emptyMap(), SEARCH_ROLE, Version.CURRENT))
                    .add(new DiscoveryNode("node3", buildNewFakeTransportAddress(), Collections.emptyMap(), SEARCH_ROLE, Version.CURRENT))
                    .build()
            )
            .build();

        RoutingAllocation allocation = new RoutingAllocation(
            yesAllocationDeciders(),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            System.nanoTime()
        );

        assertEquals(Map.of(3, List.of("test")), AutoExpandSearchReplicas.getAutoExpandSearchReplicaChanges(metadata, allocation));
    }
}
