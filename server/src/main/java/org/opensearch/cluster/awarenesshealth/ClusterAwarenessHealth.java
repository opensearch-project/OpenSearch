/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.awarenesshealth;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

/**
 * Cluster state Awareness health information
 */
public class ClusterAwarenessHealth implements Writeable {

    private final ClusterAwarenessAttributesHealth clusterAwarenessAttributesHealth;

    /**
     * Creates cluster awareness health from cluster state.
     *
     * @param clusterState           The current cluster state. Must not be null.
     * @param clusterSettings        the current cluster settings.
     * @param awarenessAttributeName Name of awareness attribute for which we need to see the health
     */
    public ClusterAwarenessHealth(ClusterState clusterState, ClusterSettings clusterSettings, String awarenessAttributeName) {
        // This property will govern if we need to show unassigned shard info or not
        boolean displayUnassignedShardLevelInfo = canCalcUnassignedShards(clusterSettings, awarenessAttributeName);
        clusterAwarenessAttributesHealth = new ClusterAwarenessAttributesHealth(
            awarenessAttributeName,
            displayUnassignedShardLevelInfo,
            clusterState
        );
    }

    public ClusterAwarenessHealth(final StreamInput in) throws IOException {
        clusterAwarenessAttributesHealth = new ClusterAwarenessAttributesHealth(in);
    }

    public ClusterAwarenessHealth(String awarenessAttribute) {
        this.clusterAwarenessAttributesHealth = new ClusterAwarenessAttributesHealth(awarenessAttribute, Collections.emptyMap());
    }

    private boolean canCalcUnassignedShards(ClusterSettings clusterSettings, String awarenessAttributeName) {
        // Getting the replicaEnforcement settings as both are necessary for replica enforcement.
        boolean allocationAwarenessBalance = clusterSettings.get(
            AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING
        );
        Settings forcedAwarenessSettings = clusterSettings.get(
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING
        );

        boolean forcedZoneSettingsExists = false;

        if (!forcedAwarenessSettings.isEmpty()) {
            // We will only mark true if particular awareness attribute exists
            if (forcedAwarenessSettings.hasValue(awarenessAttributeName + ".values")) {
                forcedZoneSettingsExists = true;
            }
        }
        return allocationAwarenessBalance && forcedZoneSettingsExists;
    }

    public ClusterAwarenessAttributesHealth getAwarenessAttributeHealth() {
        return this.clusterAwarenessAttributesHealth;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        clusterAwarenessAttributesHealth.writeTo(out);
    }

    @Override
    public String toString() {
        return "ClusterStateHealth{"
            + ", clusterAwarenessAttributeHealth.awarenessAttributeName"
            + (clusterAwarenessAttributesHealth == null ? "null" : clusterAwarenessAttributesHealth.getAwarenessAttributeName())
            + ", clusterAwarenessAttributeHealth.size="
            + (clusterAwarenessAttributesHealth == null ? "null" : clusterAwarenessAttributesHealth.getAwarenessAttributeHealthMap().size())
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterAwarenessHealth that = (ClusterAwarenessHealth) o;
        return Objects.equals(clusterAwarenessAttributesHealth, that.clusterAwarenessAttributesHealth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterAwarenessAttributesHealth);
    }
}
