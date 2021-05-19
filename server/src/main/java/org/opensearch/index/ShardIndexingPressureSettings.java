/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.Iterator;

/**
 * This class contains all the settings which are required and owned by {TODO link ShardIndexingPressure}. These will be
 * referenced/used in ShardIndexingPressure, as well as its dependent components, i.e.
 * {TODO link ShardIndexingPressureMemoryManager} and {TODO link ShardIndexingPressureStore}
 */
public final class ShardIndexingPressureSettings {

    public static final String SHARD_INDEXING_PRESSURE_ENABLED_ATTRIBUTE_KEY = "shard_indexing_pressure_enabled";

    public static final Setting<Boolean> SHARD_INDEXING_PRESSURE_ENABLED =
        Setting.boolSetting("shard_indexing_pressure.enabled", false, Setting.Property.Dynamic, Setting.Property.NodeScope);

    /**
     * Feature level setting to operate in shadow-mode or in enforced-mode. If enforced field is set to true, shard level
     * rejection will be performed, otherwise only rejection metrics will be populated.
     */
    public static final Setting<Boolean> SHARD_INDEXING_PRESSURE_ENFORCED =
        Setting.boolSetting("shard_indexing_pressure.enforced", false, Setting.Property.Dynamic, Setting.Property.NodeScope);

    /**
     * This represents the window size of last N request sampled and considered for throughput evaluation.
     */
    public static final Setting<Integer> REQUEST_SIZE_WINDOW =
        Setting.intSetting("shard_indexing_pressure.secondary_parameter.throughput.request_size_window", 2000,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * This represents the base limit set for the utilization of every shard. Will be initilized as 1/1000th bytes of node limits.
     */
    public static final Setting<Double> SHARD_MIN_LIMIT =
        Setting.doubleSetting("shard_indexing_pressure.primary_parameter.shard.min_limit", 0.001d, 0.0d,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    private volatile boolean shardIndexingPressureEnabled;
    private volatile boolean shardIndexingPressureEnforced;
    private volatile long shardPrimaryAndCoordinatingBaseLimits;
    private volatile long shardReplicaBaseLimits;
    private volatile int requestSizeWindow;
    private volatile double shardMinLimit;
    private final long primaryAndCoordinatingNodeLimits;
    private static ClusterService clusterService;

    public ShardIndexingPressureSettings(ClusterService clusterService, Settings settings, long primaryAndCoordinatingLimits) {
        ShardIndexingPressureSettings.clusterService = clusterService;
        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        this.shardIndexingPressureEnabled = SHARD_INDEXING_PRESSURE_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SHARD_INDEXING_PRESSURE_ENABLED, this::setShardIndexingPressureEnabled);

        this.shardIndexingPressureEnforced = SHARD_INDEXING_PRESSURE_ENFORCED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SHARD_INDEXING_PRESSURE_ENFORCED, this::setShardIndexingPressureEnforced);

        this.requestSizeWindow = REQUEST_SIZE_WINDOW.get(settings).intValue();
        clusterSettings.addSettingsUpdateConsumer(REQUEST_SIZE_WINDOW, this::setRequestSizeWindow);

        this.primaryAndCoordinatingNodeLimits = primaryAndCoordinatingLimits;

        this.shardMinLimit = SHARD_MIN_LIMIT.get(settings).floatValue();
        this.shardPrimaryAndCoordinatingBaseLimits = (long) (primaryAndCoordinatingLimits * shardMinLimit);
        this.shardReplicaBaseLimits = (long) (shardPrimaryAndCoordinatingBaseLimits * 1.5);
        clusterSettings.addSettingsUpdateConsumer(SHARD_MIN_LIMIT, this::setShardMinLimit);
    }

    public static boolean isShardIndexingPressureAttributeEnabled() {
        Iterator<DiscoveryNode> nodes = clusterService.state().getNodes().getNodes().valuesIt();
        while (nodes.hasNext()) {
            if (Boolean.parseBoolean(nodes.next().getAttributes().get(SHARD_INDEXING_PRESSURE_ENABLED_ATTRIBUTE_KEY)) == false) {
                return false;
            }
        }
        return true;
    }

    private void setShardIndexingPressureEnabled(Boolean shardIndexingPressureEnableValue) {
        this.shardIndexingPressureEnabled = shardIndexingPressureEnableValue;
    }

    private void setShardIndexingPressureEnforced(Boolean shardIndexingPressureEnforcedValue) {
        this.shardIndexingPressureEnforced = shardIndexingPressureEnforcedValue;
    }

    private void setRequestSizeWindow(int requestSizeWindow) {
        this.requestSizeWindow = requestSizeWindow;
    }

    private void setShardMinLimit(double shardMinLimit) {
        this.shardMinLimit = shardMinLimit;

        //Updating the dependent value once when the dynamic settings update
        this.setShardPrimaryAndCoordinatingBaseLimits();
        this.setShardReplicaBaseLimits();
    }

    private void setShardPrimaryAndCoordinatingBaseLimits() {
        shardPrimaryAndCoordinatingBaseLimits = (long) (primaryAndCoordinatingNodeLimits * shardMinLimit);
    }

    private void setShardReplicaBaseLimits() {
        shardReplicaBaseLimits = (long) (shardPrimaryAndCoordinatingBaseLimits * 1.5);
    }

    public boolean isShardIndexingPressureEnabled() {
        return shardIndexingPressureEnabled;
    }

    public boolean isShardIndexingPressureEnforced() {
        return shardIndexingPressureEnforced;
    }

    public int getRequestSizeWindow() {
        return requestSizeWindow;
    }

    public long getShardPrimaryAndCoordinatingBaseLimits() {
        return shardPrimaryAndCoordinatingBaseLimits;
    }

    public long getShardReplicaBaseLimits() {
        return shardReplicaBaseLimits;
    }

    public long getNodePrimaryAndCoordinatingLimits() {
        return primaryAndCoordinatingNodeLimits;
    }

    public long getNodeReplicaLimits() {
        return (long) (primaryAndCoordinatingNodeLimits * 1.5);
    }
}
