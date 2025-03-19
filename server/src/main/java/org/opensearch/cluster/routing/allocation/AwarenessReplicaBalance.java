/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.metadata.AutoExpandReplicas;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.Math.max;
import static org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING;
import static org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING;

/**
 * This {@link AwarenessReplicaBalance} gives total unique values of awareness attributes
 * It takes in effect only iff cluster.routing.allocation.awareness.attributes and
 * cluster.routing.allocation.awareness.force.zone.values both are specified.
 * <p>
 * This is used in enforcing total copy of shard is a maximum of unique values of awareness attributes
 * Helps in balancing shards across all awareness attributes and ensuring high availability of data.
 */
public class AwarenessReplicaBalance {
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.awareness.balance",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile List<String> awarenessAttributes;

    private volatile Map<String, List<String>> forcedAwarenessAttributes;

    private volatile Boolean awarenessBalance;

    public AwarenessReplicaBalance(Settings settings, ClusterSettings clusterSettings) {
        this.awarenessAttributes = CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING, this::setAwarenessAttributes);
        setForcedAwarenessAttributes(CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING,
            this::setForcedAwarenessAttributes
        );
        setAwarenessBalance(CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING, this::setAwarenessBalance);

    }

    private void setAwarenessBalance(Boolean awarenessBalance) {
        this.awarenessBalance = awarenessBalance;
    }

    private void setForcedAwarenessAttributes(Settings forceSettings) {
        Map<String, List<String>> forcedAwarenessAttributes = new HashMap<>();
        Map<String, Settings> forceGroups = forceSettings.getAsGroups();
        for (Map.Entry<String, Settings> entry : forceGroups.entrySet()) {
            List<String> aValues = entry.getValue().getAsList("values");
            if (aValues.size() > 0) {
                forcedAwarenessAttributes.put(entry.getKey(), aValues);
            }
        }
        this.forcedAwarenessAttributes = forcedAwarenessAttributes;
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    /*
    For a cluster having zone as awareness attribute , it will return the size of zones if set it forced awareness attributes

    If there are multiple forced awareness attributes, it will return size of the largest list, as all copies of data
    is supposed to get distributed amongst those.

    cluster.routing.allocation.awareness.attributes: rack_id , zone
    cluster.routing.allocation.awareness.force.zone.values: zone1, zone2
    cluster.routing.allocation.awareness.force.rack_id.values: rack_id1, rack_id2, rack_id3

    In this case,  awareness attributes would be 3.
     */
    public int maxAwarenessAttributes() {
        int awarenessAttributes = 1;
        if (this.awarenessBalance == false) {
            return awarenessAttributes;
        }
        for (String awarenessAttribute : this.awarenessAttributes) {
            if (forcedAwarenessAttributes.containsKey(awarenessAttribute)) {
                awarenessAttributes = max(awarenessAttributes, forcedAwarenessAttributes.get(awarenessAttribute).size());
            }
        }
        return awarenessAttributes;
    }

    public Optional<String> validateReplicas(int replicaCount, AutoExpandReplicas autoExpandReplica) {
        if (autoExpandReplica.isEnabled()) {
            if ((autoExpandReplica.getMaxReplicas() != Integer.MAX_VALUE)
                && ((autoExpandReplica.getMaxReplicas() + 1) % maxAwarenessAttributes() != 0)) {
                String errorMessage = "expected max cap on auto expand to be a multiple of total awareness attributes ["
                    + maxAwarenessAttributes()
                    + "]";
                return Optional.of(errorMessage);
            }
        } else {
            if ((replicaCount + 1) % maxAwarenessAttributes() != 0) {
                String errorMessage = "expected total copies needs to be a multiple of total awareness attributes ["
                    + maxAwarenessAttributes()
                    + "]";
                return Optional.of(errorMessage);
            }
        }
        return Optional.empty();
    }

    public Optional<String> validateSearchReplicas(int searchReplicaCount, AutoExpandReplicas autoExpandReplica) {
        if (autoExpandReplica.isEnabled()) {
            // TODO: For now Search replicas do not support auto expand, when we add support update this validation
        } else {
            if (searchReplicaCount > 0 && searchReplicaCount % maxAwarenessAttributes() != 0) {
                String errorMessage = "total search replicas needs to be a multiple of total awareness attributes ["
                    + maxAwarenessAttributes()
                    + "]";
                return Optional.of(errorMessage);
            }
        }
        return Optional.empty();
    }
}
