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

package org.opensearch.cluster;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.rollover.RolloverInfo;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.TestCustomMetadata;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ClusterStateTests extends OpenSearchTestCase {

    public void testSupersedes() {
        final Version version = Version.CURRENT;
        final DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), version);
        final DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), version);
        final DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).add(node2).build();
        ClusterName name = CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY);
        ClusterState noClusterManager1 = ClusterState.builder(name).version(randomInt(5)).nodes(nodes).build();
        ClusterState noClusterManager2 = ClusterState.builder(name).version(randomInt(5)).nodes(nodes).build();
        ClusterState withClusterManager1a = ClusterState.builder(name)
            .version(randomInt(5))
            .nodes(DiscoveryNodes.builder(nodes).clusterManagerNodeId(node1.getId()))
            .build();
        ClusterState withClusterManager1b = ClusterState.builder(name)
            .version(randomInt(5))
            .nodes(DiscoveryNodes.builder(nodes).clusterManagerNodeId(node1.getId()))
            .build();
        ClusterState withClusterManager2 = ClusterState.builder(name)
            .version(randomInt(5))
            .nodes(DiscoveryNodes.builder(nodes).clusterManagerNodeId(node2.getId()))
            .build();

        // states with no cluster-manager should never supersede anything
        assertFalse(noClusterManager1.supersedes(noClusterManager2));
        assertFalse(noClusterManager1.supersedes(withClusterManager1a));

        // states should never supersede states from another cluster-manager
        assertFalse(withClusterManager1a.supersedes(withClusterManager2));
        assertFalse(withClusterManager1a.supersedes(noClusterManager1));

        // state from the same cluster-manager compare by version
        assertThat(
            withClusterManager1a.supersedes(withClusterManager1b),
            equalTo(withClusterManager1a.version() > withClusterManager1b.version())
        );
    }

    public void testBuilderRejectsNullCustom() {
        final ClusterState.Builder builder = ClusterState.builder(ClusterName.DEFAULT);
        final String key = randomAlphaOfLength(10);
        assertThat(expectThrows(NullPointerException.class, () -> builder.putCustom(key, null)).getMessage(), containsString(key));
    }

    public void testBuilderRejectsNullInCustoms() {
        final ClusterState.Builder builder = ClusterState.builder(ClusterName.DEFAULT);
        final String key = randomAlphaOfLength(10);
        final Map<String, ClusterState.Custom> mapBuilder = new HashMap<>();
        mapBuilder.put(key, null);
        final Map<String, ClusterState.Custom> map = Collections.unmodifiableMap(mapBuilder);
        assertThat(expectThrows(NullPointerException.class, () -> builder.customs(map)).getMessage(), containsString(key));
    }

    public void testToXContent() throws IOException {
        final ClusterState clusterState = buildClusterState();

        IndexRoutingTable index = clusterState.getRoutingTable().getIndicesRouting().get("index");

        String ephemeralId = clusterState.getNodes().get("nodeId1").getEphemeralId();
        String allocationId = index.getShards().get(1).getAllAllocationIds().iterator().next();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        clusterState.toXContent(builder, new ToXContent.MapParams(singletonMap(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API)));
        builder.endObject();

        assertEquals(
            "{\n"
                + "  \"cluster_uuid\" : \"clusterUUID\",\n"
                + "  \"version\" : 0,\n"
                + "  \"state_uuid\" : \"stateUUID\",\n"
                + "  \"master_node\" : \"clusterManagerNodeId\",\n"
                + "  \"cluster_manager_node\" : \"clusterManagerNodeId\",\n"
                + "  \"blocks\" : {\n"
                + "    \"global\" : {\n"
                + "      \"1\" : {\n"
                + "        \"description\" : \"description\",\n"
                + "        \"retryable\" : true,\n"
                + "        \"disable_state_persistence\" : true,\n"
                + "        \"levels\" : [\n"
                + "          \"read\",\n"
                + "          \"write\",\n"
                + "          \"metadata_read\",\n"
                + "          \"metadata_write\",\n"
                + "          \"create_index\"\n"
                + "        ]\n"
                + "      }\n"
                + "    },\n"
                + "    \"indices\" : {\n"
                + "      \"index\" : {\n"
                + "        \"2\" : {\n"
                + "          \"description\" : \"description2\",\n"
                + "          \"retryable\" : false,\n"
                + "          \"levels\" : [\n"
                + "            \"read\",\n"
                + "            \"write\",\n"
                + "            \"metadata_read\",\n"
                + "            \"metadata_write\",\n"
                + "            \"create_index\"\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"nodes\" : {\n"
                + "    \"nodeId1\" : {\n"
                + "      \"name\" : \"\",\n"
                + "      \"ephemeral_id\" : \""
                + ephemeralId
                + "\",\n"
                + "      \"transport_address\" : \"127.0.0.1:111\",\n"
                + "      \"attributes\" : { }\n"
                + "    }\n"
                + "  },\n"
                + "  \"metadata\" : {\n"
                + "    \"cluster_uuid\" : \"clusterUUID\",\n"
                + "    \"cluster_uuid_committed\" : false,\n"
                + "    \"cluster_coordination\" : {\n"
                + "      \"term\" : 1,\n"
                + "      \"last_committed_config\" : [\n"
                + "        \"commitedConfigurationNodeId\"\n"
                + "      ],\n"
                + "      \"last_accepted_config\" : [\n"
                + "        \"acceptedConfigurationNodeId\"\n"
                + "      ],\n"
                + "      \"voting_config_exclusions\" : [\n"
                + "        {\n"
                + "          \"node_id\" : \"exlucdedNodeId\",\n"
                + "          \"node_name\" : \"excludedNodeName\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"templates\" : {\n"
                + "      \"template\" : {\n"
                + "        \"order\" : 0,\n"
                + "        \"index_patterns\" : [\n"
                + "          \"pattern1\",\n"
                + "          \"pattern2\"\n"
                + "        ],\n"
                + "        \"settings\" : {\n"
                + "          \"index\" : {\n"
                + "            \"version\" : {\n"
                + "              \"created\" : \""
                + Version.CURRENT.id
                + "\"\n"
                + "            }\n"
                + "          }\n"
                + "        },\n"
                + "        \"mappings\" : {\n"
                + "          \"key1\" : { }\n"
                + "        },\n"
                + "        \"aliases\" : { }\n"
                + "      }\n"
                + "    },\n"
                + "    \"indices\" : {\n"
                + "      \"index\" : {\n"
                + "        \"version\" : 1,\n"
                + "        \"mapping_version\" : 1,\n"
                + "        \"settings_version\" : 1,\n"
                + "        \"aliases_version\" : 1,\n"
                + "        \"routing_num_shards\" : 1,\n"
                + "        \"state\" : \"open\",\n"
                + "        \"settings\" : {\n"
                + "          \"index\" : {\n"
                + "            \"number_of_shards\" : \"1\",\n"
                + "            \"number_of_replicas\" : \"2\",\n"
                + "            \"version\" : {\n"
                + "              \"created\" : \""
                + Version.CURRENT.id
                + "\"\n"
                + "            }\n"
                + "          }\n"
                + "        },\n"
                + "        \"mappings\" : {\n"
                + "          \"type\" : {\n"
                + "            \"type1\" : {\n"
                + "              \"key\" : \"value\"\n"
                + "            }\n"
                + "          }\n"
                + "        },\n"
                + "        \"aliases\" : [\n"
                + "          \"alias\"\n"
                + "        ],\n"
                + "        \"primary_terms\" : {\n"
                + "          \"0\" : 1\n"
                + "        },\n"
                + "        \"in_sync_allocations\" : {\n"
                + "          \"0\" : [\n"
                + "            \"allocationId\"\n"
                + "          ]\n"
                + "        },\n"
                + "        \"rollover_info\" : {\n"
                + "          \"rolloveAlias\" : {\n"
                + "            \"met_conditions\" : { },\n"
                + "            \"time\" : 1\n"
                + "          }\n"
                + "        },\n"
                + "        \"system\" : false\n"
                + "      }\n"
                + "    },\n"
                + "    \"index-graveyard\" : {\n"
                + "      \"tombstones\" : [ ]\n"
                + "    }\n"
                + "  },\n"
                + "  \"routing_table\" : {\n"
                + "    \"indices\" : {\n"
                + "      \"index\" : {\n"
                + "        \"shards\" : {\n"
                + "          \"1\" : [\n"
                + "            {\n"
                + "              \"state\" : \"STARTED\",\n"
                + "              \"primary\" : true,\n"
                + "              \"searchOnly\" : false,\n"
                + "              \"node\" : \"nodeId2\",\n"
                + "              \"relocating_node\" : null,\n"
                + "              \"shard\" : 1,\n"
                + "              \"index\" : \"index\",\n"
                + "              \"allocation_id\" : {\n"
                + "                \"id\" : \""
                + allocationId
                + "\"\n"
                + "              }\n"
                + "            }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"routing_nodes\" : {\n"
                + "    \"unassigned\" : [ ],\n"
                + "    \"nodes\" : {\n"
                + "      \"nodeId2\" : [\n"
                + "        {\n"
                + "          \"state\" : \"STARTED\",\n"
                + "          \"primary\" : true,\n"
                + "          \"searchOnly\" : false,\n"
                + "          \"node\" : \"nodeId2\",\n"
                + "          \"relocating_node\" : null,\n"
                + "          \"shard\" : 1,\n"
                + "          \"index\" : \"index\",\n"
                + "          \"allocation_id\" : {\n"
                + "            \"id\" : \""
                + allocationId
                + "\"\n"
                + "          }\n"
                + "        }\n"
                + "      ],\n"
                + "      \"nodeId1\" : [ ]\n"
                + "    }\n"
                + "  }\n"
                + "}",
            builder.toString()
        );

    }

    public void testToXContent_FlatSettingTrue_ReduceMappingFalse() throws IOException {
        Map<String, String> mapParams = new HashMap<String, String>() {
            {
                put("flat_settings", "true");
                put("reduce_mappings", "false");
                put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API);
            }
        };

        final ClusterState clusterState = buildClusterState();
        IndexRoutingTable index = clusterState.getRoutingTable().getIndicesRouting().get("index");

        String ephemeralId = clusterState.getNodes().get("nodeId1").getEphemeralId();
        String allocationId = index.getShards().get(1).getAllAllocationIds().iterator().next();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        clusterState.toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals(
            "{\n"
                + "  \"cluster_uuid\" : \"clusterUUID\",\n"
                + "  \"version\" : 0,\n"
                + "  \"state_uuid\" : \"stateUUID\",\n"
                + "  \"master_node\" : \"clusterManagerNodeId\",\n"
                + "  \"cluster_manager_node\" : \"clusterManagerNodeId\",\n"
                + "  \"blocks\" : {\n"
                + "    \"global\" : {\n"
                + "      \"1\" : {\n"
                + "        \"description\" : \"description\",\n"
                + "        \"retryable\" : true,\n"
                + "        \"disable_state_persistence\" : true,\n"
                + "        \"levels\" : [\n"
                + "          \"read\",\n"
                + "          \"write\",\n"
                + "          \"metadata_read\",\n"
                + "          \"metadata_write\",\n"
                + "          \"create_index\"\n"
                + "        ]\n"
                + "      }\n"
                + "    },\n"
                + "    \"indices\" : {\n"
                + "      \"index\" : {\n"
                + "        \"2\" : {\n"
                + "          \"description\" : \"description2\",\n"
                + "          \"retryable\" : false,\n"
                + "          \"levels\" : [\n"
                + "            \"read\",\n"
                + "            \"write\",\n"
                + "            \"metadata_read\",\n"
                + "            \"metadata_write\",\n"
                + "            \"create_index\"\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"nodes\" : {\n"
                + "    \"nodeId1\" : {\n"
                + "      \"name\" : \"\",\n"
                + "      \"ephemeral_id\" : \""
                + ephemeralId
                + "\",\n"
                + "      \"transport_address\" : \"127.0.0.1:111\",\n"
                + "      \"attributes\" : { }\n"
                + "    }\n"
                + "  },\n"
                + "  \"metadata\" : {\n"
                + "    \"cluster_uuid\" : \"clusterUUID\",\n"
                + "    \"cluster_uuid_committed\" : false,\n"
                + "    \"cluster_coordination\" : {\n"
                + "      \"term\" : 1,\n"
                + "      \"last_committed_config\" : [\n"
                + "        \"commitedConfigurationNodeId\"\n"
                + "      ],\n"
                + "      \"last_accepted_config\" : [\n"
                + "        \"acceptedConfigurationNodeId\"\n"
                + "      ],\n"
                + "      \"voting_config_exclusions\" : [\n"
                + "        {\n"
                + "          \"node_id\" : \"exlucdedNodeId\",\n"
                + "          \"node_name\" : \"excludedNodeName\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"templates\" : {\n"
                + "      \"template\" : {\n"
                + "        \"order\" : 0,\n"
                + "        \"index_patterns\" : [\n"
                + "          \"pattern1\",\n"
                + "          \"pattern2\"\n"
                + "        ],\n"
                + "        \"settings\" : {\n"
                + "          \"index.version.created\" : \""
                + Version.CURRENT.id
                + "\"\n"
                + "        },\n"
                + "        \"mappings\" : {\n"
                + "          \"key1\" : { }\n"
                + "        },\n"
                + "        \"aliases\" : { }\n"
                + "      }\n"
                + "    },\n"
                + "    \"indices\" : {\n"
                + "      \"index\" : {\n"
                + "        \"version\" : 1,\n"
                + "        \"mapping_version\" : 1,\n"
                + "        \"settings_version\" : 1,\n"
                + "        \"aliases_version\" : 1,\n"
                + "        \"routing_num_shards\" : 1,\n"
                + "        \"state\" : \"open\",\n"
                + "        \"settings\" : {\n"
                + "          \"index.number_of_replicas\" : \"2\",\n"
                + "          \"index.number_of_shards\" : \"1\",\n"
                + "          \"index.version.created\" : \""
                + Version.CURRENT.id
                + "\"\n"
                + "        },\n"
                + "        \"mappings\" : {\n"
                + "          \"type\" : {\n"
                + "            \"type1\" : {\n"
                + "              \"key\" : \"value\"\n"
                + "            }\n"
                + "          }\n"
                + "        },\n"
                + "        \"aliases\" : [\n"
                + "          \"alias\"\n"
                + "        ],\n"
                + "        \"primary_terms\" : {\n"
                + "          \"0\" : 1\n"
                + "        },\n"
                + "        \"in_sync_allocations\" : {\n"
                + "          \"0\" : [\n"
                + "            \"allocationId\"\n"
                + "          ]\n"
                + "        },\n"
                + "        \"rollover_info\" : {\n"
                + "          \"rolloveAlias\" : {\n"
                + "            \"met_conditions\" : { },\n"
                + "            \"time\" : 1\n"
                + "          }\n"
                + "        },\n"
                + "        \"system\" : false\n"
                + "      }\n"
                + "    },\n"
                + "    \"index-graveyard\" : {\n"
                + "      \"tombstones\" : [ ]\n"
                + "    }\n"
                + "  },\n"
                + "  \"routing_table\" : {\n"
                + "    \"indices\" : {\n"
                + "      \"index\" : {\n"
                + "        \"shards\" : {\n"
                + "          \"1\" : [\n"
                + "            {\n"
                + "              \"state\" : \"STARTED\",\n"
                + "              \"primary\" : true,\n"
                + "              \"searchOnly\" : false,\n"
                + "              \"node\" : \"nodeId2\",\n"
                + "              \"relocating_node\" : null,\n"
                + "              \"shard\" : 1,\n"
                + "              \"index\" : \"index\",\n"
                + "              \"allocation_id\" : {\n"
                + "                \"id\" : \""
                + allocationId
                + "\"\n"
                + "              }\n"
                + "            }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"routing_nodes\" : {\n"
                + "    \"unassigned\" : [ ],\n"
                + "    \"nodes\" : {\n"
                + "      \"nodeId2\" : [\n"
                + "        {\n"
                + "          \"state\" : \"STARTED\",\n"
                + "          \"primary\" : true,\n"
                + "          \"searchOnly\" : false,\n"
                + "          \"node\" : \"nodeId2\",\n"
                + "          \"relocating_node\" : null,\n"
                + "          \"shard\" : 1,\n"
                + "          \"index\" : \"index\",\n"
                + "          \"allocation_id\" : {\n"
                + "            \"id\" : \""
                + allocationId
                + "\"\n"
                + "          }\n"
                + "        }\n"
                + "      ],\n"
                + "      \"nodeId1\" : [ ]\n"
                + "    }\n"
                + "  }\n"
                + "}",
            builder.toString()
        );

    }

    public void testToXContent_FlatSettingFalse_ReduceMappingTrue() throws IOException {
        Map<String, String> mapParams = new HashMap<String, String>() {
            {
                put("flat_settings", "false");
                put("reduce_mappings", "true");
                put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API);
            }
        };

        final ClusterState clusterState = buildClusterState();

        IndexRoutingTable index = clusterState.getRoutingTable().getIndicesRouting().get("index");

        String ephemeralId = clusterState.getNodes().get("nodeId1").getEphemeralId();
        String allocationId = index.getShards().get(1).getAllAllocationIds().iterator().next();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        clusterState.toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.endObject();

        assertEquals(
            "{\n"
                + "  \"cluster_uuid\" : \"clusterUUID\",\n"
                + "  \"version\" : 0,\n"
                + "  \"state_uuid\" : \"stateUUID\",\n"
                + "  \"master_node\" : \"clusterManagerNodeId\",\n"
                + "  \"cluster_manager_node\" : \"clusterManagerNodeId\",\n"
                + "  \"blocks\" : {\n"
                + "    \"global\" : {\n"
                + "      \"1\" : {\n"
                + "        \"description\" : \"description\",\n"
                + "        \"retryable\" : true,\n"
                + "        \"disable_state_persistence\" : true,\n"
                + "        \"levels\" : [\n"
                + "          \"read\",\n"
                + "          \"write\",\n"
                + "          \"metadata_read\",\n"
                + "          \"metadata_write\",\n"
                + "          \"create_index\"\n"
                + "        ]\n"
                + "      }\n"
                + "    },\n"
                + "    \"indices\" : {\n"
                + "      \"index\" : {\n"
                + "        \"2\" : {\n"
                + "          \"description\" : \"description2\",\n"
                + "          \"retryable\" : false,\n"
                + "          \"levels\" : [\n"
                + "            \"read\",\n"
                + "            \"write\",\n"
                + "            \"metadata_read\",\n"
                + "            \"metadata_write\",\n"
                + "            \"create_index\"\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"nodes\" : {\n"
                + "    \"nodeId1\" : {\n"
                + "      \"name\" : \"\",\n"
                + "      \"ephemeral_id\" : \""
                + ephemeralId
                + "\",\n"
                + "      \"transport_address\" : \"127.0.0.1:111\",\n"
                + "      \"attributes\" : { }\n"
                + "    }\n"
                + "  },\n"
                + "  \"metadata\" : {\n"
                + "    \"cluster_uuid\" : \"clusterUUID\",\n"
                + "    \"cluster_uuid_committed\" : false,\n"
                + "    \"cluster_coordination\" : {\n"
                + "      \"term\" : 1,\n"
                + "      \"last_committed_config\" : [\n"
                + "        \"commitedConfigurationNodeId\"\n"
                + "      ],\n"
                + "      \"last_accepted_config\" : [\n"
                + "        \"acceptedConfigurationNodeId\"\n"
                + "      ],\n"
                + "      \"voting_config_exclusions\" : [\n"
                + "        {\n"
                + "          \"node_id\" : \"exlucdedNodeId\",\n"
                + "          \"node_name\" : \"excludedNodeName\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"templates\" : {\n"
                + "      \"template\" : {\n"
                + "        \"order\" : 0,\n"
                + "        \"index_patterns\" : [\n"
                + "          \"pattern1\",\n"
                + "          \"pattern2\"\n"
                + "        ],\n"
                + "        \"settings\" : {\n"
                + "          \"index\" : {\n"
                + "            \"version\" : {\n"
                + "              \"created\" : \""
                + Version.CURRENT.id
                + "\"\n"
                + "            }\n"
                + "          }\n"
                + "        },\n"
                + "        \"mappings\" : { },\n"
                + "        \"aliases\" : { }\n"
                + "      }\n"
                + "    },\n"
                + "    \"indices\" : {\n"
                + "      \"index\" : {\n"
                + "        \"version\" : 1,\n"
                + "        \"mapping_version\" : 1,\n"
                + "        \"settings_version\" : 1,\n"
                + "        \"aliases_version\" : 1,\n"
                + "        \"routing_num_shards\" : 1,\n"
                + "        \"state\" : \"open\",\n"
                + "        \"settings\" : {\n"
                + "          \"index\" : {\n"
                + "            \"number_of_shards\" : \"1\",\n"
                + "            \"number_of_replicas\" : \"2\",\n"
                + "            \"version\" : {\n"
                + "              \"created\" : \""
                + Version.CURRENT.id
                + "\"\n"
                + "            }\n"
                + "          }\n"
                + "        },\n"
                + "        \"mappings\" : {\n"
                + "          \"type\" : {\n"
                + "            \"type1\" : {\n"
                + "              \"key\" : \"value\"\n"
                + "            }\n"
                + "          }\n"
                + "        },\n"
                + "        \"aliases\" : [\n"
                + "          \"alias\"\n"
                + "        ],\n"
                + "        \"primary_terms\" : {\n"
                + "          \"0\" : 1\n"
                + "        },\n"
                + "        \"in_sync_allocations\" : {\n"
                + "          \"0\" : [\n"
                + "            \"allocationId\"\n"
                + "          ]\n"
                + "        },\n"
                + "        \"rollover_info\" : {\n"
                + "          \"rolloveAlias\" : {\n"
                + "            \"met_conditions\" : { },\n"
                + "            \"time\" : 1\n"
                + "          }\n"
                + "        },\n"
                + "        \"system\" : false\n"
                + "      }\n"
                + "    },\n"
                + "    \"index-graveyard\" : {\n"
                + "      \"tombstones\" : [ ]\n"
                + "    }\n"
                + "  },\n"
                + "  \"routing_table\" : {\n"
                + "    \"indices\" : {\n"
                + "      \"index\" : {\n"
                + "        \"shards\" : {\n"
                + "          \"1\" : [\n"
                + "            {\n"
                + "              \"state\" : \"STARTED\",\n"
                + "              \"primary\" : true,\n"
                + "              \"searchOnly\" : false,\n"
                + "              \"node\" : \"nodeId2\",\n"
                + "              \"relocating_node\" : null,\n"
                + "              \"shard\" : 1,\n"
                + "              \"index\" : \"index\",\n"
                + "              \"allocation_id\" : {\n"
                + "                \"id\" : \""
                + allocationId
                + "\"\n"
                + "              }\n"
                + "            }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"routing_nodes\" : {\n"
                + "    \"unassigned\" : [ ],\n"
                + "    \"nodes\" : {\n"
                + "      \"nodeId2\" : [\n"
                + "        {\n"
                + "          \"state\" : \"STARTED\",\n"
                + "          \"primary\" : true,\n"
                + "          \"searchOnly\" : false,\n"
                + "          \"node\" : \"nodeId2\",\n"
                + "          \"relocating_node\" : null,\n"
                + "          \"shard\" : 1,\n"
                + "          \"index\" : \"index\",\n"
                + "          \"allocation_id\" : {\n"
                + "            \"id\" : \""
                + allocationId
                + "\"\n"
                + "          }\n"
                + "        }\n"
                + "      ],\n"
                + "      \"nodeId1\" : [ ]\n"
                + "    }\n"
                + "  }\n"
                + "}",
            builder.toString()
        );

    }

    public void testToXContentSameTypeName() throws IOException {
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .stateUUID("stateUUID")
            .metadata(
                Metadata.builder()
                    .clusterUUID("clusterUUID")
                    .coordinationMetadata(CoordinationMetadata.builder().build())
                    .put(
                        IndexMetadata.builder("index")
                            .state(IndexMetadata.State.OPEN)
                            .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id))
                            .putMapping(
                                new MappingMetadata(
                                    "type",
                                    // the type name is the root value,
                                    // the original logic in ClusterState.toXContent will reduce
                                    new HashMap<String, Object>() {
                                        {
                                            put("type", new HashMap<String, Object>() {
                                                {
                                                    put("key", "value");
                                                }
                                            });
                                        }
                                    }
                                )
                            )
                            .numberOfShards(1)
                            .primaryTerm(0, 1L)
                            .numberOfReplicas(2)
                    )
            )
            .build();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        clusterState.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        assertEquals(
            "{\n"
                + "  \"cluster_uuid\" : \"clusterUUID\",\n"
                + "  \"version\" : 0,\n"
                + "  \"state_uuid\" : \"stateUUID\",\n"
                + "  \"master_node\" : null,\n"
                + "  \"cluster_manager_node\" : null,\n"
                + "  \"blocks\" : { },\n"
                + "  \"nodes\" : { },\n"
                + "  \"metadata\" : {\n"
                + "    \"cluster_uuid\" : \"clusterUUID\",\n"
                + "    \"cluster_uuid_committed\" : false,\n"
                + "    \"cluster_coordination\" : {\n"
                + "      \"term\" : 0,\n"
                + "      \"last_committed_config\" : [ ],\n"
                + "      \"last_accepted_config\" : [ ],\n"
                + "      \"voting_config_exclusions\" : [ ]\n"
                + "    },\n"
                + "    \"templates\" : { },\n"
                + "    \"indices\" : {\n"
                + "      \"index\" : {\n"
                + "        \"version\" : 2,\n"
                + "        \"mapping_version\" : 1,\n"
                + "        \"settings_version\" : 1,\n"
                + "        \"aliases_version\" : 1,\n"
                + "        \"routing_num_shards\" : 1,\n"
                + "        \"state\" : \"open\",\n"
                + "        \"settings\" : {\n"
                + "          \"index\" : {\n"
                + "            \"number_of_shards\" : \"1\",\n"
                + "            \"number_of_replicas\" : \"2\",\n"
                + "            \"version\" : {\n"
                + "              \"created\" : \""
                + Version.CURRENT.id
                + "\"\n"
                + "            }\n"
                + "          }\n"
                + "        },\n"
                + "        \"mappings\" : {\n"
                + "          \"type\" : {\n"
                + "            \"key\" : \"value\"\n"
                + "          }\n"
                + "        },\n"
                + "        \"aliases\" : [ ],\n"
                + "        \"primary_terms\" : {\n"
                + "          \"0\" : 1\n"
                + "        },\n"
                + "        \"in_sync_allocations\" : {\n"
                + "          \"0\" : [ ]\n"
                + "        },\n"
                + "        \"rollover_info\" : { },\n"
                + "        \"system\" : false\n"
                + "      }\n"
                + "    },\n"
                + "    \"index-graveyard\" : {\n"
                + "      \"tombstones\" : [ ]\n"
                + "    }\n"
                + "  },\n"
                + "  \"routing_table\" : {\n"
                + "    \"indices\" : { }\n"
                + "  },\n"
                + "  \"routing_nodes\" : {\n"
                + "    \"unassigned\" : [ ],\n"
                + "    \"nodes\" : { }\n"
                + "  }\n"
                + "}",
            builder.toString()
        );
    }

    private ClusterState buildClusterState() throws IOException {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .state(IndexMetadata.State.OPEN)
            .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id))
            .putMapping(new MappingMetadata("type", new HashMap<String, Object>() {
                {
                    put("type1", new HashMap<String, Object>() {
                        {
                            put("key", "value");
                        }
                    });
                }
            }))
            .putAlias(AliasMetadata.builder("alias").indexRouting("indexRouting").build())
            .numberOfShards(1)
            .primaryTerm(0, 1L)
            .putInSyncAllocationIds(0, new HashSet<String>() {
                {
                    add("allocationId");
                }
            })
            .numberOfReplicas(2)
            .putRolloverInfo(new RolloverInfo("rolloveAlias", new ArrayList<>(), 1L))
            .build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .stateUUID("stateUUID")
            .nodes(
                DiscoveryNodes.builder()
                    .clusterManagerNodeId("clusterManagerNodeId")
                    .add(new DiscoveryNode("nodeId1", new TransportAddress(InetAddress.getByName("127.0.0.1"), 111), Version.CURRENT))
                    .build()
            )
            .blocks(
                ClusterBlocks.builder()
                    .addGlobalBlock(
                        new ClusterBlock(1, "description", true, true, true, RestStatus.ACCEPTED, EnumSet.allOf((ClusterBlockLevel.class)))
                    )
                    .addBlocks(indexMetadata)
                    .addIndexBlock(
                        "index",
                        new ClusterBlock(
                            2,
                            "description2",
                            false,
                            false,
                            false,
                            RestStatus.ACCEPTED,
                            EnumSet.allOf((ClusterBlockLevel.class))
                        )
                    )
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .clusterUUID("clusterUUID")
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .term(1)
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(new HashSet<String>() {
                                {
                                    add("commitedConfigurationNodeId");
                                }
                            }))
                            .lastAcceptedConfiguration(new CoordinationMetadata.VotingConfiguration(new HashSet<String>() {
                                {
                                    add("acceptedConfigurationNodeId");
                                }
                            }))
                            .addVotingConfigExclusion(new CoordinationMetadata.VotingConfigExclusion("exlucdedNodeId", "excludedNodeName"))
                            .build()
                    )
                    .persistentSettings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id).build())
                    .transientSettings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id).build())
                    .put(indexMetadata, false)
                    .put(
                        IndexTemplateMetadata.builder("template")
                            .patterns(Arrays.asList("pattern1", "pattern2"))
                            .order(0)
                            .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id))
                            .putMapping("type", "{ \"key1\": {} }")
                            .build()
                    )
            )
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(new Index("index", "indexUUID"))
                            .addIndexShard(
                                new IndexShardRoutingTable.Builder(new ShardId("index", "_na_", 1)).addShard(
                                    TestShardRouting.newShardRouting(
                                        new ShardId("index", "_na_", 1),
                                        "nodeId2",
                                        true,
                                        ShardRoutingState.STARTED
                                    )
                                ).build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();
    }

    public static class CustomMetadata extends TestCustomMetadata {
        public static final String TYPE = "custom_md";

        CustomMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
        }
    }
}
