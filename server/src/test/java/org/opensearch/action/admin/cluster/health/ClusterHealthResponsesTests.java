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
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.health.ClusterIndexHealthTests;
import org.opensearch.cluster.health.ClusterStateHealth;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.AbstractSerializingTestCase;
import org.opensearch.test.OpenSearchTestCase;

import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ClusterHealthResponsesTests extends AbstractSerializingTestCase<ClusterHealthResponse> {
    private final ClusterHealthRequest.Level level = randomFrom(ClusterHealthRequest.Level.values());

    public void testIsTimeout() {
        ClusterHealthResponse res = new ClusterHealthResponse();
        for (int i = 0; i < 5; i++) {
            res.setTimedOut(randomBoolean());
            if (res.isTimedOut()) {
                assertEquals(RestStatus.REQUEST_TIMEOUT, res.status());
            } else {
                assertEquals(RestStatus.OK, res.status());
            }
        }
    }

    public void testClusterHealth() throws IOException {
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).build();
        int pendingTasks = randomIntBetween(0, 200);
        int inFlight = randomIntBetween(0, 200);
        int delayedUnassigned = randomIntBetween(0, 200);
        TimeValue pendingTaskInQueueTime = TimeValue.timeValueMillis(randomIntBetween(1000, 100000));
        ClusterHealthResponse clusterHealth = new ClusterHealthResponse(
            "bla",
            new String[] { Metadata.ALL },
            clusterState,
            pendingTasks,
            inFlight,
            delayedUnassigned,
            pendingTaskInQueueTime
        );
        clusterHealth = maybeSerialize(clusterHealth);
        assertClusterHealth(clusterHealth);
        assertThat(clusterHealth.getNumberOfPendingTasks(), Matchers.equalTo(pendingTasks));
        assertThat(clusterHealth.getNumberOfInFlightFetch(), Matchers.equalTo(inFlight));
        assertThat(clusterHealth.getDelayedUnassignedShards(), Matchers.equalTo(delayedUnassigned));
        assertThat(clusterHealth.getTaskMaxWaitingTime().millis(), is(pendingTaskInQueueTime.millis()));
        assertThat(clusterHealth.getActiveShardsPercent(), is(allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(100.0))));
    }

    public void testClusterHealthVerifyClusterManagerNodeDiscovery() throws IOException {
        DiscoveryNode localNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        // set the node information to verify cluster_manager_node discovery in ClusterHealthResponse
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).clusterManagerNodeId(localNode.getId()))
            .build();
        int pendingTasks = randomIntBetween(0, 200);
        int inFlight = randomIntBetween(0, 200);
        int delayedUnassigned = randomIntBetween(0, 200);
        TimeValue pendingTaskInQueueTime = TimeValue.timeValueMillis(randomIntBetween(1000, 100000));
        ClusterHealthResponse clusterHealth = new ClusterHealthResponse(
            "bla",
            new String[] { Metadata.ALL },
            clusterState,
            pendingTasks,
            inFlight,
            delayedUnassigned,
            pendingTaskInQueueTime
        );
        clusterHealth = maybeSerialize(clusterHealth);
        assertThat(clusterHealth.getClusterStateHealth().hasDiscoveredClusterManager(), Matchers.equalTo(true));
        assertClusterHealth(clusterHealth);
    }

    private void assertClusterHealth(ClusterHealthResponse clusterHealth) {
        ClusterStateHealth clusterStateHealth = clusterHealth.getClusterStateHealth();

        assertThat(clusterHealth.getActiveShards(), Matchers.equalTo(clusterStateHealth.getActiveShards()));
        assertThat(clusterHealth.getRelocatingShards(), Matchers.equalTo(clusterStateHealth.getRelocatingShards()));
        assertThat(clusterHealth.getActivePrimaryShards(), Matchers.equalTo(clusterStateHealth.getActivePrimaryShards()));
        assertThat(clusterHealth.getInitializingShards(), Matchers.equalTo(clusterStateHealth.getInitializingShards()));
        assertThat(clusterHealth.getUnassignedShards(), Matchers.equalTo(clusterStateHealth.getUnassignedShards()));
        assertThat(clusterHealth.getNumberOfNodes(), Matchers.equalTo(clusterStateHealth.getNumberOfNodes()));
        assertThat(clusterHealth.getNumberOfDataNodes(), Matchers.equalTo(clusterStateHealth.getNumberOfDataNodes()));
        assertThat(clusterHealth.hasDiscoveredClusterManager(), Matchers.equalTo(clusterStateHealth.hasDiscoveredClusterManager()));
    }

    ClusterHealthResponse maybeSerialize(ClusterHealthResponse clusterHealth) throws IOException {
        if (randomBoolean()) {
            BytesStreamOutput out = new BytesStreamOutput();
            clusterHealth.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            clusterHealth = ClusterHealthResponse.readResponseFrom(in);
        }
        return clusterHealth;
    }

    public void testParseFromXContentWithDiscoveredClusterManagerField() throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                "{\"cluster_name\":\"535799904437:7-1-3-node\",\"status\":\"green\","
                    + "\"timed_out\":false,\"number_of_nodes\":6,\"number_of_data_nodes\":3,\"discovered_cluster_manager\":true,"
                    + "\"active_primary_shards\":4,\"active_shards\":5,\"relocating_shards\":0,\"initializing_shards\":0,"
                    + "\"unassigned_shards\":0,\"delayed_unassigned_shards\":0,\"number_of_pending_tasks\":0,"
                    + "\"number_of_in_flight_fetch\":0,\"task_max_waiting_in_queue_millis\":0,"
                    + "\"active_shards_percent_as_number\":100}"
            )
        ) {

            ClusterHealthResponse clusterHealth = ClusterHealthResponse.fromXContent(parser);
            assertNotNull(clusterHealth);
            assertThat(clusterHealth.getClusterName(), Matchers.equalTo("535799904437:7-1-3-node"));
            assertThat(clusterHealth.getNumberOfNodes(), Matchers.equalTo(6));
            assertThat(clusterHealth.hasDiscoveredClusterManager(), Matchers.equalTo(true));
        }
    }

    public void testParseFromXContentWithoutDiscoveredClusterManagerField() throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                "{\"cluster_name\":\"535799904437:7-1-3-node\",\"status\":\"green\","
                    + "\"timed_out\":false,\"number_of_nodes\":6,\"number_of_data_nodes\":3,"
                    + "\"active_primary_shards\":4,\"active_shards\":5,\"relocating_shards\":0,\"initializing_shards\":0,"
                    + "\"unassigned_shards\":0,\"delayed_unassigned_shards\":0,\"number_of_pending_tasks\":0,"
                    + "\"number_of_in_flight_fetch\":0,\"task_max_waiting_in_queue_millis\":0,"
                    + "\"active_shards_percent_as_number\":100}"
            )
        ) {
            ClusterHealthResponse clusterHealth = ClusterHealthResponse.fromXContent(parser);
            assertNotNull(clusterHealth);
            assertThat(clusterHealth.getClusterName(), Matchers.equalTo("535799904437:7-1-3-node"));
            assertThat(clusterHealth.getNumberOfNodes(), Matchers.equalTo(6));
            assertThat(clusterHealth.hasDiscoveredClusterManager(), Matchers.equalTo(false));
        }
    }

    /**
     * Validate the ClusterHealthResponse can be parsed from JsonXContent that contains the deprecated "discovered_master" field.
     * As of 2.0, to support inclusive language, "discovered_master" field will be replaced by "discovered_cluster_manager".
     */
    public void testParseFromXContentWithDeprecatedDiscoveredMasterField() throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                "{\"cluster_name\":\"opensearch-cluster\",\"status\":\"green\",\"timed_out\":false,"
                    + "\"number_of_nodes\":6,\"number_of_data_nodes\":3,\"discovered_cluster_manager\":true,\"discovered_master\":true,"
                    + "\"active_primary_shards\":4,\"active_shards\":5,\"relocating_shards\":0,\"initializing_shards\":0,"
                    + "\"unassigned_shards\":0,\"delayed_unassigned_shards\":0,\"number_of_pending_tasks\":0,"
                    + "\"number_of_in_flight_fetch\":0,\"task_max_waiting_in_queue_millis\":0,"
                    + "\"active_shards_percent_as_number\":100}"
            )
        ) {
            ClusterHealthResponse clusterHealth = ClusterHealthResponse.fromXContent(parser);
            assertThat(clusterHealth.hasDiscoveredClusterManager(), Matchers.equalTo(true));
        }

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                "{\"cluster_name\":\"opensearch-cluster\",\"status\":\"green\","
                    + "\"timed_out\":false,\"number_of_nodes\":6,\"number_of_data_nodes\":3,\"discovered_master\":true,"
                    + "\"active_primary_shards\":4,\"active_shards\":5,\"relocating_shards\":0,\"initializing_shards\":0,"
                    + "\"unassigned_shards\":0,\"delayed_unassigned_shards\":0,\"number_of_pending_tasks\":0,"
                    + "\"number_of_in_flight_fetch\":0,\"task_max_waiting_in_queue_millis\":0,"
                    + "\"active_shards_percent_as_number\":100}"
            )
        ) {
            ClusterHealthResponse clusterHealth = ClusterHealthResponse.fromXContent(parser);
            assertThat(clusterHealth.hasDiscoveredClusterManager(), Matchers.equalTo(true));
        }
    }

    @Override
    protected ClusterHealthResponse doParseInstance(XContentParser parser) {
        return ClusterHealthResponse.fromXContent(parser);
    }

    @Override
    protected ClusterHealthResponse createTestInstance() {
        int indicesSize = randomInt(20);
        Map<String, ClusterIndexHealth> indices = new HashMap<>(indicesSize);
        if (ClusterHealthRequest.Level.INDICES.equals(level) || ClusterHealthRequest.Level.SHARDS.equals(level)) {
            for (int i = 0; i < indicesSize; i++) {
                String indexName = randomAlphaOfLengthBetween(1, 5) + i;
                indices.put(indexName, ClusterIndexHealthTests.randomIndexHealth(indexName, level));
            }
        }
        ClusterStateHealth stateHealth = new ClusterStateHealth(
            randomInt(100),
            randomInt(100),
            randomInt(100),
            randomInt(100),
            randomInt(100),
            randomInt(100),
            randomInt(100),
            randomBoolean(),
            randomDoubleBetween(0d, 100d, true),
            randomFrom(ClusterHealthStatus.values()),
            indices
        );

        return new ClusterHealthResponse(
            randomAlphaOfLengthBetween(1, 10),
            randomInt(100),
            randomInt(100),
            randomInt(100),
            TimeValue.timeValueMillis(randomInt(10000)),
            randomBoolean(),
            stateHealth
        );
    }

    @Override
    protected Writeable.Reader<ClusterHealthResponse> instanceReader() {
        return ClusterHealthResponse::new;
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap("level", level.name().toLowerCase(Locale.ROOT)));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    // Ignore all paths which looks like "indices.RANDOMINDEXNAME.shards"
    private static final Pattern SHARDS_IN_XCONTENT = Pattern.compile("^indices\\.\\w+\\.shards$");

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> "indices".equals(field) || SHARDS_IN_XCONTENT.matcher(field).find();
    }

    @Override
    protected ClusterHealthResponse mutateInstance(ClusterHealthResponse instance) {
        String mutate = randomFrom(
            "clusterName",
            "numberOfPendingTasks",
            "numberOfInFlightFetch",
            "delayedUnassignedShards",
            "taskMaxWaitingTime",
            "timedOut",
            "clusterStateHealth"
        );
        switch (mutate) {
            case "clusterName":
                return new ClusterHealthResponse(
                    instance.getClusterName() + randomAlphaOfLengthBetween(2, 5),
                    instance.getNumberOfPendingTasks(),
                    instance.getNumberOfInFlightFetch(),
                    instance.getDelayedUnassignedShards(),
                    instance.getTaskMaxWaitingTime(),
                    instance.isTimedOut(),
                    instance.getClusterStateHealth()
                );
            case "numberOfPendingTasks":
                return new ClusterHealthResponse(
                    instance.getClusterName(),
                    instance.getNumberOfPendingTasks() + between(1, 10),
                    instance.getNumberOfInFlightFetch(),
                    instance.getDelayedUnassignedShards(),
                    instance.getTaskMaxWaitingTime(),
                    instance.isTimedOut(),
                    instance.getClusterStateHealth()
                );
            case "numberOfInFlightFetch":
                return new ClusterHealthResponse(
                    instance.getClusterName(),
                    instance.getNumberOfPendingTasks(),
                    instance.getNumberOfInFlightFetch() + between(1, 10),
                    instance.getDelayedUnassignedShards(),
                    instance.getTaskMaxWaitingTime(),
                    instance.isTimedOut(),
                    instance.getClusterStateHealth()
                );
            case "delayedUnassignedShards":
                return new ClusterHealthResponse(
                    instance.getClusterName(),
                    instance.getNumberOfPendingTasks(),
                    instance.getNumberOfInFlightFetch(),
                    instance.getDelayedUnassignedShards() + between(1, 10),
                    instance.getTaskMaxWaitingTime(),
                    instance.isTimedOut(),
                    instance.getClusterStateHealth()
                );
            case "taskMaxWaitingTime":

                return new ClusterHealthResponse(
                    instance.getClusterName(),
                    instance.getNumberOfPendingTasks(),
                    instance.getNumberOfInFlightFetch(),
                    instance.getDelayedUnassignedShards(),
                    new TimeValue(instance.getTaskMaxWaitingTime().millis() + between(1, 10)),
                    instance.isTimedOut(),
                    instance.getClusterStateHealth()
                );
            case "timedOut":
                return new ClusterHealthResponse(
                    instance.getClusterName(),
                    instance.getNumberOfPendingTasks(),
                    instance.getNumberOfInFlightFetch(),
                    instance.getDelayedUnassignedShards(),
                    instance.getTaskMaxWaitingTime(),
                    instance.isTimedOut() == false,
                    instance.getClusterStateHealth()
                );
            case "clusterStateHealth":
                ClusterStateHealth state = instance.getClusterStateHealth();
                ClusterStateHealth newState = new ClusterStateHealth(
                    state.getActivePrimaryShards() + between(1, 10),
                    state.getActiveShards(),
                    state.getRelocatingShards(),
                    state.getInitializingShards(),
                    state.getUnassignedShards(),
                    state.getNumberOfNodes(),
                    state.getNumberOfDataNodes(),
                    state.hasDiscoveredClusterManager(),
                    state.getActiveShardsPercent(),
                    state.getStatus(),
                    state.getIndices()
                );
                return new ClusterHealthResponse(
                    instance.getClusterName(),
                    instance.getNumberOfPendingTasks(),
                    instance.getNumberOfInFlightFetch(),
                    instance.getDelayedUnassignedShards(),
                    instance.getTaskMaxWaitingTime(),
                    instance.isTimedOut(),
                    newState
                );
            default:
                throw new UnsupportedOperationException();
        }
    }
}
