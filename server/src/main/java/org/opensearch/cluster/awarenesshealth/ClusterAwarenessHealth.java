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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Cluster state Awareness health information
 */
public class ClusterAwarenessHealth implements Writeable, ToXContentFragment, Iterable<ClusterAwarenessAttributesHealth> {

    private static final String AWARENESS_ATTRIBUTE = "awareness_attributes";
    private final Map<String, ClusterAwarenessAttributesHealth> clusterAwarenessAttributesHealthMap;

    /**
     * Creates cluster awareness health from cluster state.
     *
     * @param clusterState           The current cluster state. Must not be null.
     * @param clusterSettings        the current cluster settings.
     * @param awarenessAttributeName Name of awareness attribute for which we need to see the health
     */
    public ClusterAwarenessHealth(ClusterState clusterState, ClusterSettings clusterSettings, String awarenessAttributeName) {
        // This property will govern if we need to show unassigned shard info or not
        boolean displayUnassignedShardLevelInfo;
        ClusterAwarenessAttributesHealth clusterAwarenessAttributesHealth;
        clusterAwarenessAttributesHealthMap = new HashMap<>();
        List<String> awarenessAttributeList = getAwarenessAttributeList(awarenessAttributeName, clusterSettings);
        for (String awarenessAttribute : awarenessAttributeList) {
            displayUnassignedShardLevelInfo = canCalcUnassignedShards(clusterSettings, awarenessAttribute);
            clusterAwarenessAttributesHealth = new ClusterAwarenessAttributesHealth(
                awarenessAttribute,
                displayUnassignedShardLevelInfo,
                clusterState
            );
            clusterAwarenessAttributesHealthMap.put(awarenessAttribute, clusterAwarenessAttributesHealth);
        }
    }

    public ClusterAwarenessHealth(final StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size > 0) {
            clusterAwarenessAttributesHealthMap = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                ClusterAwarenessAttributesHealth clusterAwarenessAttributesHealth = new ClusterAwarenessAttributesHealth(in);
                clusterAwarenessAttributesHealthMap.put(
                    clusterAwarenessAttributesHealth.getAwarenessAttributeName(),
                    clusterAwarenessAttributesHealth
                );
            }
        } else {
            clusterAwarenessAttributesHealthMap = Collections.emptyMap();
        }
    }

    private List<String> getAwarenessAttributeList(String awarenessAttributeName, ClusterSettings clusterSettings) {
        // Helper function to check if we need health for all or for one awareness attribute.
        boolean displayAllAwarenessAttribute = awarenessAttributeName == null || awarenessAttributeName.isBlank();
        List<String> awarenessAttributeList = new ArrayList<>();
        if (!displayAllAwarenessAttribute) {
            awarenessAttributeList.add(awarenessAttributeName);
        } else {
            awarenessAttributeList = clusterSettings.get(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING);
        }
        return awarenessAttributeList;
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

    public Map<String, ClusterAwarenessAttributesHealth> getClusterAwarenessAttributesHealthMap() {
        return clusterAwarenessAttributesHealthMap;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        int size = clusterAwarenessAttributesHealthMap.size();
        out.writeVInt(size);
        if (size > 0) {
            for (ClusterAwarenessAttributesHealth awarenessAttributeValueHealth : this) {
                awarenessAttributeValueHealth.writeTo(out);
            }
        }
    }

    @Override
    public String toString() {
        return "ClusterAwarenessHealth{"
            + "clusterAwarenessHealth.clusterAwarenessAttributesHealthMap.size="
            + (clusterAwarenessAttributesHealthMap == null ? "null" : clusterAwarenessAttributesHealthMap.size())
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterAwarenessHealth that = (ClusterAwarenessHealth) o;
        return clusterAwarenessAttributesHealthMap.size() == that.clusterAwarenessAttributesHealthMap.size();
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterAwarenessAttributesHealthMap);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(AWARENESS_ATTRIBUTE);
        for (ClusterAwarenessAttributesHealth awarenessAttributeValueHealth : this) {
            awarenessAttributeValueHealth.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public Iterator<ClusterAwarenessAttributesHealth> iterator() {
        return clusterAwarenessAttributesHealthMap.values().iterator();
    }
}
