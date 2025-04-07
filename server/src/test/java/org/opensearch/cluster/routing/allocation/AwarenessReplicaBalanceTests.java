/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.AutoExpandReplicas;
import org.opensearch.cluster.metadata.AutoExpandSearchReplicas;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

import java.util.Optional;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_SEARCH_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.hamcrest.Matchers.equalTo;

public class AwarenessReplicaBalanceTests extends OpenSearchAllocationTestCase {

    private static final ClusterSettings EMPTY_CLUSTER_SETTINGS = new ClusterSettings(
        Settings.EMPTY,
        ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
    );

    public void testNoForcedAwarenessAttribute() {
        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "rack_id")
            .put(SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(SETTING_AUTO_EXPAND_SEARCH_REPLICAS, "0-1")
            .build();
        AutoExpandReplicas autoExpandReplica = AutoExpandReplicas.SETTING.get(settings);
        AwarenessReplicaBalance awarenessReplicaBalance = new AwarenessReplicaBalance(settings, EMPTY_CLUSTER_SETTINGS);
        assertThat(awarenessReplicaBalance.maxAwarenessAttributes(), equalTo(1));

        assertEquals(awarenessReplicaBalance.validate(0, autoExpandReplica), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(1, autoExpandReplica), Optional.empty());

        AutoExpandSearchReplicas autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(settings);
        assertEquals(awarenessReplicaBalance.validate(0, autoExpandSearchReplicas), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(1, autoExpandSearchReplicas), Optional.empty());
    }

    public void testForcedAwarenessAttribute() {
        // When auto expand replica settings is as per zone awareness
        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone, rack")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a, b")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "rack.values", "c, d, e")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), true)
            .put(SETTING_AUTO_EXPAND_REPLICAS, "0-2")
            .put(SETTING_AUTO_EXPAND_SEARCH_REPLICAS, "0-3")
            .build();

        AwarenessReplicaBalance awarenessReplicaBalance = new AwarenessReplicaBalance(settings, EMPTY_CLUSTER_SETTINGS);
        AutoExpandReplicas autoExpandReplica = AutoExpandReplicas.SETTING.get(settings);
        assertThat(awarenessReplicaBalance.maxAwarenessAttributes(), equalTo(3));
        assertEquals(awarenessReplicaBalance.validate(2, autoExpandReplica), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(1, autoExpandReplica), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(0, autoExpandReplica), Optional.empty());

        AutoExpandSearchReplicas autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(settings);
        assertEquals(awarenessReplicaBalance.validate(1, autoExpandSearchReplicas), Optional.empty());

        // When auto expand replica settings is passed as max cap
        settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone, rack")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a, b")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "rack.values", "c, d, e")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), true)
            .put(SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .put(SETTING_AUTO_EXPAND_SEARCH_REPLICAS, "0-all")
            .build();

        awarenessReplicaBalance = new AwarenessReplicaBalance(settings, EMPTY_CLUSTER_SETTINGS);
        autoExpandReplica = AutoExpandReplicas.SETTING.get(settings);

        assertThat(awarenessReplicaBalance.maxAwarenessAttributes(), equalTo(3));
        assertEquals(awarenessReplicaBalance.validate(2, autoExpandReplica), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(1, autoExpandReplica), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(0, autoExpandReplica), Optional.empty());

        autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(settings);
        assertEquals(awarenessReplicaBalance.validate(1, autoExpandSearchReplicas), Optional.empty());

        // when auto expand is not valid set as per zone awareness
        settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone, rack")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a, b")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "rack.values", "c, d, e")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), true)
            .put(SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(SETTING_AUTO_EXPAND_SEARCH_REPLICAS, "0-1")
            .build();

        awarenessReplicaBalance = new AwarenessReplicaBalance(settings, EMPTY_CLUSTER_SETTINGS);
        autoExpandReplica = AutoExpandReplicas.SETTING.get(settings);

        assertEquals(
            awarenessReplicaBalance.validate(1, autoExpandReplica),
            Optional.of("expected max cap on auto expand to be a multiple of total awareness attributes [3]")
        );
        assertEquals(
            awarenessReplicaBalance.validate(2, autoExpandReplica),
            Optional.of("expected max cap on auto expand to be a multiple of total awareness attributes [3]")
        );

        autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(settings);
        assertEquals(
            awarenessReplicaBalance.validate(1, autoExpandSearchReplicas),
            Optional.of("expected max cap on auto expand search replicas to be a multiple of total awareness attributes [3]")
        );
        assertEquals(
            awarenessReplicaBalance.validate(2, autoExpandSearchReplicas),
            Optional.of("expected max cap on auto expand search replicas to be a multiple of total awareness attributes [3]")
        );

        // When auto expand replica is not present
        settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone, rack")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a, b")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "rack.values", "c, d, e")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), true)
            .build();

        awarenessReplicaBalance = new AwarenessReplicaBalance(settings, EMPTY_CLUSTER_SETTINGS);
        autoExpandReplica = AutoExpandReplicas.SETTING.get(settings);

        assertEquals(awarenessReplicaBalance.validate(2, autoExpandReplica), Optional.empty());
        assertEquals(
            awarenessReplicaBalance.validate(1, autoExpandReplica),
            Optional.of("expected total copies needs to be a multiple of total awareness attributes [3]")
        );
        assertEquals(
            awarenessReplicaBalance.validate(0, autoExpandReplica),
            Optional.of("expected total copies needs to be a multiple of total awareness attributes [3]")
        );

        autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(settings);
        assertEquals(awarenessReplicaBalance.validate(3, autoExpandSearchReplicas), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(0, autoExpandSearchReplicas), Optional.empty());
        assertEquals(
            awarenessReplicaBalance.validate(2, autoExpandSearchReplicas),
            Optional.of("total search replicas needs to be a multiple of total awareness attributes [3]")
        );
        assertEquals(
            awarenessReplicaBalance.validate(1, autoExpandSearchReplicas),
            Optional.of("total search replicas needs to be a multiple of total awareness attributes [3]")
        );
    }

    public void testForcedAwarenessAttributeDisabled() {
        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone, rack")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), true)
            .put(SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, "0-1")
            .build();

        AwarenessReplicaBalance awarenessReplicaBalance = new AwarenessReplicaBalance(settings, EMPTY_CLUSTER_SETTINGS);
        AutoExpandReplicas autoExpandReplica = AutoExpandReplicas.SETTING.get(settings);

        assertThat(awarenessReplicaBalance.maxAwarenessAttributes(), equalTo(1));
        assertEquals(awarenessReplicaBalance.validate(0, autoExpandReplica), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(1, autoExpandReplica), Optional.empty());

        AutoExpandSearchReplicas autoExpandSearchReplicas = AutoExpandSearchReplicas.SETTING.get(settings);
        assertEquals(awarenessReplicaBalance.validate(0, autoExpandSearchReplicas), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(1, autoExpandSearchReplicas), Optional.empty());
    }

}
