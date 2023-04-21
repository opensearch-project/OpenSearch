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

package org.opensearch.action.admin.cluster.health;

import org.opensearch.Version;
import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.awarenesshealth.ClusterAwarenessHealth;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.health.ClusterStateHealth;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Transport response for Cluster Health
 *
 * @opensearch.internal
 */
public class ClusterHealthResponse extends ActionResponse implements StatusToXContentObject {
    private static final String CLUSTER_NAME = "cluster_name";
    private static final String STATUS = "status";
    private static final String TIMED_OUT = "timed_out";
    private static final String NUMBER_OF_NODES = "number_of_nodes";
    private static final String NUMBER_OF_DATA_NODES = "number_of_data_nodes";
    @Deprecated
    private static final String DISCOVERED_MASTER = "discovered_master";
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
    private static final String INDICES = "indices";

    private static final ConstructingObjectParser<ClusterHealthResponse, Void> PARSER = new ConstructingObjectParser<>(
        "cluster_health_response",
        true,
        parsedObjects -> {
            int i = 0;
            // ClusterStateHealth fields
            int numberOfNodes = (int) parsedObjects[i++];
            int numberOfDataNodes = (int) parsedObjects[i++];
            boolean hasDiscoveredMaster = Boolean.TRUE.equals(parsedObjects[i++]);
            boolean hasDiscoveredClusterManager = Boolean.TRUE.equals(parsedObjects[i++]);
            int activeShards = (int) parsedObjects[i++];
            int relocatingShards = (int) parsedObjects[i++];
            int activePrimaryShards = (int) parsedObjects[i++];
            int initializingShards = (int) parsedObjects[i++];
            int unassignedShards = (int) parsedObjects[i++];
            double activeShardsPercent = (double) parsedObjects[i++];
            String statusStr = (String) parsedObjects[i++];
            ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
            @SuppressWarnings("unchecked")
            List<ClusterIndexHealth> indexList = (List<ClusterIndexHealth>) parsedObjects[i++];
            final Map<String, ClusterIndexHealth> indices;
            if (indexList == null || indexList.isEmpty()) {
                indices = emptyMap();
            } else {
                indices = new HashMap<>(indexList.size());
                for (ClusterIndexHealth indexHealth : indexList) {
                    indices.put(indexHealth.getIndex(), indexHealth);
                }
            }
            ClusterStateHealth stateHealth = new ClusterStateHealth(
                activePrimaryShards,
                activeShards,
                relocatingShards,
                initializingShards,
                unassignedShards,
                numberOfNodes,
                numberOfDataNodes,
                hasDiscoveredClusterManager || hasDiscoveredMaster,
                activeShardsPercent,
                status,
                indices
            );
            // ClusterHealthResponse fields
            String clusterName = (String) parsedObjects[i++];
            int numberOfPendingTasks = (int) parsedObjects[i++];
            int numberOfInFlightFetch = (int) parsedObjects[i++];
            int delayedUnassignedShards = (int) parsedObjects[i++];
            long taskMaxWaitingTimeMillis = (long) parsedObjects[i++];
            boolean timedOut = (boolean) parsedObjects[i];
            return new ClusterHealthResponse(
                clusterName,
                numberOfPendingTasks,
                numberOfInFlightFetch,
                delayedUnassignedShards,
                TimeValue.timeValueMillis(taskMaxWaitingTimeMillis),
                timedOut,
                stateHealth
            );
        }
    );

    private static final ObjectParser.NamedObjectParser<ClusterIndexHealth, Void> INDEX_PARSER = (
        XContentParser parser,
        Void context,
        String index) -> ClusterIndexHealth.innerFromXContent(parser, index);

    static {
        // ClusterStateHealth fields
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_NODES));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_DATA_NODES));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(DISCOVERED_MASTER));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(DISCOVERED_CLUSTER_MANAGER));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(RELOCATING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_PRIMARY_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(INITIALIZING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(UNASSIGNED_SHARDS));
        PARSER.declareDouble(constructorArg(), new ParseField(ACTIVE_SHARDS_PERCENT_AS_NUMBER));
        PARSER.declareString(constructorArg(), new ParseField(STATUS));
        // Can be absent if LEVEL == 'cluster'
        PARSER.declareNamedObjects(optionalConstructorArg(), INDEX_PARSER, new ParseField(INDICES));

        // ClusterHealthResponse fields
        PARSER.declareString(constructorArg(), new ParseField(CLUSTER_NAME));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_PENDING_TASKS));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_IN_FLIGHT_FETCH));
        PARSER.declareInt(constructorArg(), new ParseField(DELAYED_UNASSIGNED_SHARDS));
        PARSER.declareLong(constructorArg(), new ParseField(TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS));
        PARSER.declareBoolean(constructorArg(), new ParseField(TIMED_OUT));
    }

    private String clusterName;
    private int numberOfPendingTasks = 0;
    private int numberOfInFlightFetch = 0;
    private int delayedUnassignedShards = 0;
    private TimeValue taskMaxWaitingTime = TimeValue.timeValueMillis(0);
    private boolean timedOut = false;
    private ClusterStateHealth clusterStateHealth;
    private ClusterHealthStatus clusterHealthStatus;
    private ClusterAwarenessHealth clusterAwarenessHealth;

    public ClusterHealthResponse() {}

    public ClusterHealthResponse(StreamInput in) throws IOException {
        super(in);
        clusterName = in.readString();
        clusterHealthStatus = ClusterHealthStatus.fromValue(in.readByte());
        clusterStateHealth = new ClusterStateHealth(in);
        numberOfPendingTasks = in.readInt();
        timedOut = in.readBoolean();
        numberOfInFlightFetch = in.readInt();
        delayedUnassignedShards = in.readInt();
        taskMaxWaitingTime = in.readTimeValue();
        if (in.getVersion().onOrAfter(Version.V_2_5_0)) {
            if (in.readBoolean()) {
                clusterAwarenessHealth = new ClusterAwarenessHealth(in);
            }
        }
    }

    /** needed for plugins BWC */
    public ClusterHealthResponse(String clusterName, String[] concreteIndices, ClusterState clusterState) {
        this(clusterName, concreteIndices, clusterState, -1, -1, -1, TimeValue.timeValueHours(0));
    }

    public ClusterHealthResponse(
        String clusterName,
        String[] concreteIndices,
        ClusterState clusterState,
        int numberOfPendingTasks,
        int numberOfInFlightFetch,
        int delayedUnassignedShards,
        TimeValue taskMaxWaitingTime
    ) {
        this.clusterName = clusterName;
        this.numberOfPendingTasks = numberOfPendingTasks;
        this.numberOfInFlightFetch = numberOfInFlightFetch;
        this.delayedUnassignedShards = delayedUnassignedShards;
        this.taskMaxWaitingTime = taskMaxWaitingTime;
        this.clusterStateHealth = new ClusterStateHealth(clusterState, concreteIndices);
        this.clusterHealthStatus = clusterStateHealth.getStatus();
    }

    // Awareness Attribute health
    public ClusterHealthResponse(
        String clusterName,
        ClusterState clusterState,
        ClusterSettings clusterSettings,
        String[] concreteIndices,
        String awarenessAttributeName,
        int numberOfPendingTasks,
        int numberOfInFlightFetch,
        int delayedUnassignedShards,
        TimeValue taskMaxWaitingTime
    ) {
        this(
            clusterName,
            concreteIndices,
            clusterState,
            numberOfPendingTasks,
            numberOfInFlightFetch,
            delayedUnassignedShards,
            taskMaxWaitingTime
        );
        this.clusterAwarenessHealth = new ClusterAwarenessHealth(clusterState, clusterSettings, awarenessAttributeName);
    }

    /**
     * For XContent Parser and serialization tests
     */
    ClusterHealthResponse(
        String clusterName,
        int numberOfPendingTasks,
        int numberOfInFlightFetch,
        int delayedUnassignedShards,
        TimeValue taskMaxWaitingTime,
        boolean timedOut,
        ClusterStateHealth clusterStateHealth
    ) {
        this.clusterName = clusterName;
        this.numberOfPendingTasks = numberOfPendingTasks;
        this.numberOfInFlightFetch = numberOfInFlightFetch;
        this.delayedUnassignedShards = delayedUnassignedShards;
        this.taskMaxWaitingTime = taskMaxWaitingTime;
        this.timedOut = timedOut;
        this.clusterStateHealth = clusterStateHealth;
        this.clusterHealthStatus = clusterStateHealth.getStatus();
    }

    public String getClusterName() {
        return clusterName;
    }

    // package private for testing
    ClusterStateHealth getClusterStateHealth() {
        return clusterStateHealth;
    }

    public int getActiveShards() {
        return clusterStateHealth.getActiveShards();
    }

    public int getRelocatingShards() {
        return clusterStateHealth.getRelocatingShards();
    }

    public int getActivePrimaryShards() {
        return clusterStateHealth.getActivePrimaryShards();
    }

    public int getInitializingShards() {
        return clusterStateHealth.getInitializingShards();
    }

    public int getUnassignedShards() {
        return clusterStateHealth.getUnassignedShards();
    }

    public int getNumberOfNodes() {
        return clusterStateHealth.getNumberOfNodes();
    }

    public int getNumberOfDataNodes() {
        return clusterStateHealth.getNumberOfDataNodes();
    }

    public boolean hasDiscoveredClusterManager() {
        return clusterStateHealth.hasDiscoveredClusterManager();
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #hasDiscoveredClusterManager()} */
    @Deprecated
    public boolean hasDiscoveredMaster() {
        return hasDiscoveredClusterManager();
    }

    public int getNumberOfPendingTasks() {
        return this.numberOfPendingTasks;
    }

    public int getNumberOfInFlightFetch() {
        return this.numberOfInFlightFetch;
    }

    /**
     * The number of unassigned shards that are currently being delayed (for example,
     * due to node leaving the cluster and waiting for a timeout for the node to come
     * back in order to allocate the shards back to it).
     */
    public int getDelayedUnassignedShards() {
        return this.delayedUnassignedShards;
    }

    /**
     * {@code true} if the waitForXXX has timeout out and did not match.
     */
    public boolean isTimedOut() {
        return this.timedOut;
    }

    public void setTimedOut(boolean timedOut) {
        this.timedOut = timedOut;
    }

    public ClusterHealthStatus getStatus() {
        return clusterHealthStatus;
    }

    /**
     * Allows to explicitly override the derived cluster health status.
     *
     * @param status The override status. Must not be null.
     */
    public void setStatus(ClusterHealthStatus status) {
        if (status == null) {
            throw new IllegalArgumentException("'status' must not be null");
        }
        this.clusterHealthStatus = status;
    }

    public Map<String, ClusterIndexHealth> getIndices() {
        return clusterStateHealth.getIndices();
    }

    /**
     *
     * @return The maximum wait time of all tasks in the queue
     */
    public TimeValue getTaskMaxWaitingTime() {
        return taskMaxWaitingTime;
    }

    /**
     * The percentage of active shards, should be 100% in a green system
     */
    public double getActiveShardsPercent() {
        return clusterStateHealth.getActiveShardsPercent();
    }

    public ClusterAwarenessHealth getClusterAwarenessHealth() {
        return clusterAwarenessHealth;
    }

    public static ClusterHealthResponse readResponseFrom(StreamInput in) throws IOException {
        return new ClusterHealthResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(clusterName);
        out.writeByte(clusterHealthStatus.value());
        clusterStateHealth.writeTo(out);
        out.writeInt(numberOfPendingTasks);
        out.writeBoolean(timedOut);
        out.writeInt(numberOfInFlightFetch);
        out.writeInt(delayedUnassignedShards);
        out.writeTimeValue(taskMaxWaitingTime);
        if (out.getVersion().onOrAfter(Version.V_2_5_0)) {
            if (clusterAwarenessHealth != null) {
                out.writeBoolean(true);
                clusterAwarenessHealth.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }

    @Override
    public RestStatus status() {
        return isTimedOut() ? RestStatus.REQUEST_TIMEOUT : RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLUSTER_NAME, getClusterName());
        builder.field(STATUS, getStatus().name().toLowerCase(Locale.ROOT));
        builder.field(TIMED_OUT, isTimedOut());
        builder.field(NUMBER_OF_NODES, getNumberOfNodes());
        builder.field(NUMBER_OF_DATA_NODES, getNumberOfDataNodes());
        builder.field(DISCOVERED_MASTER, hasDiscoveredClusterManager());  // the field will be removed in a future major version
        builder.field(DISCOVERED_CLUSTER_MANAGER, hasDiscoveredClusterManager());
        builder.field(ACTIVE_PRIMARY_SHARDS, getActivePrimaryShards());
        builder.field(ACTIVE_SHARDS, getActiveShards());
        builder.field(RELOCATING_SHARDS, getRelocatingShards());
        builder.field(INITIALIZING_SHARDS, getInitializingShards());
        builder.field(UNASSIGNED_SHARDS, getUnassignedShards());
        builder.field(DELAYED_UNASSIGNED_SHARDS, getDelayedUnassignedShards());
        builder.field(NUMBER_OF_PENDING_TASKS, getNumberOfPendingTasks());
        builder.field(NUMBER_OF_IN_FLIGHT_FETCH, getNumberOfInFlightFetch());
        builder.humanReadableField(TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS, TASK_MAX_WAIT_TIME_IN_QUEUE, getTaskMaxWaitingTime());
        builder.percentageField(ACTIVE_SHARDS_PERCENT_AS_NUMBER, ACTIVE_SHARDS_PERCENT, getActiveShardsPercent());

        String level = params.param("level", "cluster");
        boolean outputIndices = "indices".equals(level) || "shards".equals(level);
        boolean outputAwarenessHealth = "awareness_attributes".equals(level);

        if (outputIndices) {
            builder.startObject(INDICES);
            for (ClusterIndexHealth indexHealth : clusterStateHealth.getIndices().values()) {
                indexHealth.toXContent(builder, params);
            }
            builder.endObject();
        }

        if (outputAwarenessHealth && clusterAwarenessHealth != null) {
            clusterAwarenessHealth.toXContent(builder, params);
        }

        builder.endObject();
        return builder;
    }

    public static ClusterHealthResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterHealthResponse that = (ClusterHealthResponse) o;
        return Objects.equals(clusterName, that.clusterName)
            && numberOfPendingTasks == that.numberOfPendingTasks
            && numberOfInFlightFetch == that.numberOfInFlightFetch
            && delayedUnassignedShards == that.delayedUnassignedShards
            && Objects.equals(taskMaxWaitingTime, that.taskMaxWaitingTime)
            && timedOut == that.timedOut
            && Objects.equals(clusterStateHealth, that.clusterStateHealth)
            && clusterHealthStatus == that.clusterHealthStatus;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            clusterName,
            numberOfPendingTasks,
            numberOfInFlightFetch,
            delayedUnassignedShards,
            taskMaxWaitingTime,
            timedOut,
            clusterStateHealth,
            clusterHealthStatus
        );
    }
}
