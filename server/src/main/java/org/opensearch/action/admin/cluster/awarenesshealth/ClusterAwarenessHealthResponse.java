/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.awarenesshealth;

import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.awarenesshealth.ClusterAwarenessAttributesHealth;
import org.opensearch.cluster.awarenesshealth.ClusterAwarenessHealth;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.health.ClusterStateHealth;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Transport response for Cluster Awareness Health
 *
 * @opensearch.internal
 */
public class ClusterAwarenessHealthResponse extends ActionResponse implements StatusToXContentObject {

    private static final String CLUSTER_NAME = "cluster_name";
    private static final String STATUS = "status";
    private static final String NUMBER_OF_NODES = "number_of_nodes";
    private static final String NUMBER_OF_DATA_NODES = "number_of_data_nodes";
    private static final String DISCOVERED_CLUSTER_MANAGER = "discovered_cluster_manager";
    private static final String NUMBER_OF_PENDING_TASKS = "number_of_pending_tasks";
    private static final String NUMBER_OF_IN_FLIGHT_FETCH = "number_of_in_flight_fetch";
    private static final String DELAYED_UNASSIGNED_SHARDS = "delayed_unassigned_shards";
    private static final String TASK_MAX_WAIT_TIME_IN_QUEUE = "task_max_waiting_in_queue";
    private static final String TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS = "task_max_waiting_in_queue_millis";
    private static final String ACTIVE_SHARDS_PERCENT_AS_NUMBER = "active_shards_percent_as_number";
    private static final String ACTIVE_SHARDS_PERCENT = "active_shards_percent";
    private static final String ACTIVE_PRIMARY_SHARDS = "active_primary_shards";
    private static final String ACTIVE_SHARDS = "active_shards";
    private static final String RELOCATING_SHARDS = "relocating_shards";
    private static final String INITIALIZING_SHARDS = "initializing_shards";
    private static final String UNASSIGNED_SHARDS = "unassigned_shards";

    private final String clusterName;
    private final ClusterAwarenessHealth clusterAwarenessHealth;
    private final int numberOfPendingTasks;
    private final int numberOfInflightFetches;
    private final TimeValue maxWaitTime;
    private final int delayedUnassignedShards;
    private final ClusterStateHealth clusterStateHealth;

    public ClusterAwarenessHealthResponse(StreamInput in) throws IOException {
        super(in);
        clusterName = in.readString();
        numberOfPendingTasks = in.readVInt();
        numberOfInflightFetches = in.readVInt();
        delayedUnassignedShards = in.readVInt();
        maxWaitTime = in.readTimeValue();
        clusterStateHealth = new ClusterStateHealth(in);
        clusterAwarenessHealth = new ClusterAwarenessHealth(in);
    }

    public ClusterAwarenessHealthResponse(
        ClusterState clusterState,
        ClusterSettings clusterSettings,
        String awarenessAttributeName,
        int numberOfPendingTasks,
        TimeValue maxWaitTime,
        int numberOfInflightFetches,
        int delayedUnassignedShards
    ) {
        this.clusterName = clusterState.getClusterName().value();
        this.numberOfPendingTasks = numberOfPendingTasks;
        this.maxWaitTime = maxWaitTime;
        this.numberOfInflightFetches = numberOfInflightFetches;
        this.delayedUnassignedShards = delayedUnassignedShards;
        this.clusterStateHealth = new ClusterStateHealth(clusterState);
        this.clusterAwarenessHealth = new ClusterAwarenessHealth(clusterState, clusterSettings, awarenessAttributeName);
    }

    // Constructor used by Unit tests
    ClusterAwarenessHealthResponse(
        String clusterName,
        int activeShards,
        int unassignedShards,
        int initializingShards,
        int relocatingShards,
        int numberOfDataNodes,
        int numberOfNodes,
        int numberOfPendingTasks,
        int numberOfInflightFetches,
        TimeValue maxWaitTime,
        int activePrimaryShards,
        double activeShardsPercent,
        int delayedUnassignedShards,
        boolean hasDiscoveredClusterManager,
        String awarenessAttribute,
        ClusterHealthStatus clusterHealthStatus,
        Map<String, ClusterIndexHealth> indices
    ) {
        this.clusterName = clusterName;
        this.numberOfPendingTasks = numberOfPendingTasks;
        this.numberOfInflightFetches = numberOfInflightFetches;
        this.maxWaitTime = maxWaitTime;
        this.delayedUnassignedShards = delayedUnassignedShards;
        this.clusterStateHealth = new ClusterStateHealth(
            activePrimaryShards,
            activeShards,
            relocatingShards,
            initializingShards,
            unassignedShards,
            numberOfNodes,
            numberOfDataNodes,
            hasDiscoveredClusterManager,
            activeShardsPercent,
            clusterHealthStatus,
            indices
        );
        this.clusterAwarenessHealth = new ClusterAwarenessHealth(awarenessAttribute);
    }

    // Defined for Unit Tests
    public static ClusterAwarenessHealthResponse readResponse(StreamInput in) throws IOException {
        return new ClusterAwarenessHealthResponse(in);
    }

    public String getClusterName() {
        return clusterName;
    }

    public int getActiveShards() {
        return clusterStateHealth.getActiveShards();
    }

    public int getUnassignedShards() {
        return clusterStateHealth.getUnassignedShards();
    }

    public int getInitializingShards() {
        return clusterStateHealth.getInitializingShards();
    }

    public int getRelocatingShards() {
        return clusterStateHealth.getRelocatingShards();
    }

    public int getNumberOfDataNodes() {
        return clusterStateHealth.getNumberOfDataNodes();
    }

    public int getNumberOfNodes() {
        return clusterStateHealth.getNumberOfNodes();
    }

    public int getActivePrimaryShards() {
        return clusterStateHealth.getActivePrimaryShards();
    }

    public double getActiveShardsPercent() {
        return clusterStateHealth.getActiveShardsPercent();
    }

    public ClusterHealthStatus getStatus() {
        return clusterStateHealth.getStatus();
    }

    public boolean hasDiscoveredClusterManager() {
        return clusterStateHealth.hasDiscoveredClusterManager();
    }

    public int getNumberOfPendingTasks() {
        return numberOfPendingTasks;
    }

    public TimeValue getMaxWaitTime() {
        return maxWaitTime;
    }

    public int getNumberOfInflightFetches() {
        return numberOfInflightFetches;
    }

    public ClusterAwarenessAttributesHealth getAwarenessAttributeHealth() {
        return clusterAwarenessHealth.getAwarenessAttributeHealth();
    }

    public int getDelayedUnassignedShards() {
        return delayedUnassignedShards;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLUSTER_NAME, getClusterName());
        builder.field(STATUS, getStatus().name().toLowerCase(Locale.ROOT));
        builder.field(NUMBER_OF_NODES, getNumberOfNodes());
        builder.field(NUMBER_OF_DATA_NODES, getNumberOfDataNodes());
        builder.field(DISCOVERED_CLUSTER_MANAGER, hasDiscoveredClusterManager());
        builder.field(ACTIVE_PRIMARY_SHARDS, getActivePrimaryShards());
        builder.field(ACTIVE_SHARDS, getActiveShards());
        builder.field(RELOCATING_SHARDS, getRelocatingShards());
        builder.field(INITIALIZING_SHARDS, getInitializingShards());
        builder.field(UNASSIGNED_SHARDS, getUnassignedShards());
        builder.field(DELAYED_UNASSIGNED_SHARDS, getDelayedUnassignedShards());
        builder.field(NUMBER_OF_PENDING_TASKS, getNumberOfPendingTasks());
        builder.field(NUMBER_OF_IN_FLIGHT_FETCH, getNumberOfInflightFetches());
        builder.humanReadableField(TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS, TASK_MAX_WAIT_TIME_IN_QUEUE, getMaxWaitTime());
        builder.percentageField(ACTIVE_SHARDS_PERCENT_AS_NUMBER, ACTIVE_SHARDS_PERCENT, getActiveShardsPercent());
        getAwarenessAttributeHealth().toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(clusterName);
        out.writeVInt(numberOfPendingTasks);
        out.writeVInt(numberOfInflightFetches);
        out.writeVInt(delayedUnassignedShards);
        out.writeTimeValue(maxWaitTime);
        clusterStateHealth.writeTo(out);
        clusterAwarenessHealth.writeTo(out);
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }
}
