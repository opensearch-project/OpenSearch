/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;

public class AwarenessReplicaBalanceTests extends OpenSearchAllocationTestCase {

    private static final ClusterSettings EMPTY_CLUSTER_SETTINGS = new ClusterSettings(
        Settings.EMPTY,
        ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
    );

    public void testNoForcedAwarenessAttribute() {
        Settings settings = Settings.builder().put("cluster.routing.allocation.awareness.attributes", "rack_id").build();

        AwarenessReplicaBalance awarenessReplicaBalance = new AwarenessReplicaBalance(settings, EMPTY_CLUSTER_SETTINGS);
        assertThat(awarenessReplicaBalance.maxAwarenessAttributes(), equalTo(1));

        assertEquals(awarenessReplicaBalance.validate(0,-1), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(1, -1), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(0,0), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(0, 1), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(1,0), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(1, 1), Optional.empty());
    }

    public void testForcedAwarenessAttribute() {
        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone, rack")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a, b")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "rack.values", "c, d, e")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), true)
            .build();

        AwarenessReplicaBalance awarenessReplicaBalance = new AwarenessReplicaBalance(settings, EMPTY_CLUSTER_SETTINGS);
        assertThat(awarenessReplicaBalance.maxAwarenessAttributes(), equalTo(3));
        assertEquals(awarenessReplicaBalance.validate(2, -1), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(1, 2), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(0, 2), Optional.empty());
        assertEquals(
            awarenessReplicaBalance.validate(1, -1),
            Optional.of("expected total copies needs to be a multiple of total awareness attributes [3]")
        );
        assertEquals(
            awarenessReplicaBalance.validate(1, 1),
            Optional.of("expected max cap on auto expand to be a multiple of total awareness attributes [3]")
        );

    }

    public void testForcedAwarenessAttributeDisabled() {
        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone, rack")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), true)
            .build();

        AwarenessReplicaBalance awarenessReplicaBalance = new AwarenessReplicaBalance(settings, EMPTY_CLUSTER_SETTINGS);
        assertThat(awarenessReplicaBalance.maxAwarenessAttributes(), equalTo(1));
        assertEquals(awarenessReplicaBalance.validate(0, -1), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(1, -1), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(0, 0), Optional.empty());
        assertEquals(awarenessReplicaBalance.validate(0, 1), Optional.empty());
    }

}
