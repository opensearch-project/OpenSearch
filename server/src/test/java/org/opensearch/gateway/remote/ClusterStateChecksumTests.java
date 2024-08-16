/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ClusterStateChecksumTests extends OpenSearchTestCase {

    public void testClusterStateChecksumEmptyClusterState() {
        ClusterStateChecksum checksum = new ClusterStateChecksum(ClusterState.EMPTY_STATE);
        assertNotNull(checksum);
    }

    public void testClusterStateChecksum() {
        ClusterStateChecksum checksum = new ClusterStateChecksum(generateClusterState());
        assertNotNull(checksum);
        assertTrue(checksum.routingTableChecksum != 0);
        assertTrue(checksum.nodesChecksum != 0);
        assertTrue(checksum.blocksChecksum != 0);
        assertTrue(checksum.clusterStateCustomsChecksum != 0);
        assertTrue(checksum.coordinationMetadataChecksum != 0);
        assertTrue(checksum.settingMetadataChecksum != 0);
        assertTrue(checksum.transientSettingsMetadataChecksum != 0);
        assertTrue(checksum.templatesMetadataChecksum != 0);
        assertTrue(checksum.customMetadataMapChecksum != 0);
        assertTrue(checksum.hashesOfConsistentSettingsChecksum != 0);
        assertTrue(checksum.indicesChecksum != 0);
        assertTrue(checksum.clusterStateChecksum != 0);
    }

    public void testXContentConversion() throws IOException {
        ClusterStateChecksum checksum = new ClusterStateChecksum(generateClusterState());
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        checksum.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final ClusterStateChecksum parsedChecksum = ClusterStateChecksum.fromXContent(parser);
            assertEquals(checksum, parsedChecksum);
        }
    }

    public void testSerialization() throws IOException {
        ClusterStateChecksum checksum = new ClusterStateChecksum(generateClusterState());
        BytesStreamOutput output = new BytesStreamOutput();
        checksum.writeTo(output);

        try (StreamInput in = output.bytes().streamInput()) {
            ClusterStateChecksum deserializedChecksum = new ClusterStateChecksum(in);
            assertEquals(checksum, deserializedChecksum);
        }
    }

    public void testGetMismatchEntities() {
        ClusterState clsState1 = generateClusterState();
        ClusterStateChecksum checksum = new ClusterStateChecksum(clsState1);
        assertTrue(checksum.getMismatchEntities(checksum).isEmpty());

        ClusterStateChecksum checksum2 = new ClusterStateChecksum(clsState1);
        assertTrue(checksum.getMismatchEntities(checksum2).isEmpty());

        ClusterState clsState2 = ClusterState.builder(ClusterName.DEFAULT)
            .routingTable(RoutingTable.builder().build())
            .nodes(DiscoveryNodes.builder().build())
            .blocks(ClusterBlocks.builder().build())
            .customs(Map.of())
            .metadata(Metadata.EMPTY_METADATA)
            .build();
        ClusterStateChecksum checksum3 = new ClusterStateChecksum(clsState2);
        List<String> mismatches = checksum.getMismatchEntities(checksum3);
        assertFalse(mismatches.isEmpty());
        assertEquals(11, mismatches.size());
        assertEquals(ClusterStateChecksum.ROUTING_TABLE_CS, mismatches.get(0));
        assertEquals(ClusterStateChecksum.NODES_CS, mismatches.get(1));
        assertEquals(ClusterStateChecksum.BLOCKS_CS, mismatches.get(2));
        assertEquals(ClusterStateChecksum.CUSTOMS_CS, mismatches.get(3));
        assertEquals(ClusterStateChecksum.COORDINATION_MD_CS, mismatches.get(4));
        assertEquals(ClusterStateChecksum.SETTINGS_MD_CS, mismatches.get(5));
        assertEquals(ClusterStateChecksum.TRANSIENT_SETTINGS_MD_CS, mismatches.get(6));
        assertEquals(ClusterStateChecksum.TEMPLATES_MD_CS, mismatches.get(7));
        assertEquals(ClusterStateChecksum.CUSTOM_MD_CS, mismatches.get(8));
        assertEquals(ClusterStateChecksum.HASHES_MD_CS, mismatches.get(9));
        assertEquals(ClusterStateChecksum.INDICES_CS, mismatches.get(10));
    }

    private ClusterState generateClusterState() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true)
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final Settings settings = Settings.builder().put("mock-settings", true).build();
        final TemplatesMetadata templatesMetadata = TemplatesMetadata.builder()
            .put(IndexTemplateMetadata.builder("template1").settings(idxSettings).patterns(List.of("test*")).build())
            .build();
        final RemoteClusterStateTestUtils.CustomMetadata1 customMetadata1 = new RemoteClusterStateTestUtils.CustomMetadata1(
            "custom-metadata-1"
        );
        RemoteClusterStateTestUtils.TestClusterStateCustom1 clusterStateCustom1 = new RemoteClusterStateTestUtils.TestClusterStateCustom1(
            "custom-1"
        );
        return ClusterState.builder(ClusterName.DEFAULT)
            .version(1L)
            .stateUUID("state-uuid")
            .metadata(
                Metadata.builder()
                    .version(1L)
                    .put(indexMetadata, true)
                    .clusterUUID("cluster-uuid")
                    .coordinationMetadata(coordinationMetadata)
                    .persistentSettings(settings)
                    .transientSettings(settings)
                    .templates(templatesMetadata)
                    .hashesOfConsistentSettings(Map.of("key1", "value1", "key2", "value2"))
                    .putCustom(customMetadata1.getWriteableName(), customMetadata1)
                    .indices(Map.of(indexMetadata.getIndex().getName(), indexMetadata))
                    .build()
            )
            .nodes(DiscoveryNodes.builder().clusterManagerNodeId("test-node").build())
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata).build())
            .customs(Map.of(clusterStateCustom1.getWriteableName(), clusterStateCustom1))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).version(1L).build())
            .build();
    }
}
