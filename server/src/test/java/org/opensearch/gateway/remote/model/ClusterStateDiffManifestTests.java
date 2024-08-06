/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.gateway.remote.ClusterStateDiffManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.opensearch.Version.CURRENT;
import static org.opensearch.cluster.ClusterState.EMPTY_STATE;
import static org.opensearch.cluster.routing.remote.RemoteRoutingTableService.CUSTOM_ROUTING_TABLE_DIFFABLE_VALUE_SERIALIZER;
import static org.opensearch.core.common.transport.TransportAddress.META_ADDRESS;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V3;
import static org.opensearch.gateway.remote.RemoteClusterStateServiceTests.generateClusterStateWithOneIndex;
import static org.opensearch.gateway.remote.RemoteClusterStateServiceTests.nodesWithLocalNodeClusterManager;
import static org.opensearch.gateway.remote.model.RemoteClusterBlocksTests.randomClusterBlocks;

public class ClusterStateDiffManifestTests extends OpenSearchTestCase {

    public void testClusterStateDiffManifest() {
        ClusterState initialState = ClusterState.builder(EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("index-1")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();
        updateAndVerifyState(
            initialState,
            singletonList(
                IndexMetadata.builder("index-2")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build()
            ),
            singletonList("index-1"),
            emptyMap(),
            emptyList(),
            emptyMap(),
            emptyList(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
    }

    public void testClusterStateDiffManifestXContent() throws IOException {
        ClusterState initialState = ClusterState.builder(EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("index-1")
                            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();
        ClusterStateDiffManifest diffManifest = updateAndVerifyState(
            initialState,
            emptyList(),
            singletonList("index-1"),
            emptyMap(),
            emptyList(),
            emptyMap(),
            emptyList(),
            true,
            true,
            true,
            true,
            true,
            true,
            true
        );
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        diffManifest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final ClusterStateDiffManifest parsedManifest = ClusterStateDiffManifest.fromXContent(parser, CODEC_V3);
            assertEquals(diffManifest, parsedManifest);
        }
    }

    public void testClusterStateWithRoutingTableDiffInDiffManifestXContent() throws IOException {
        ClusterState initialState = generateClusterStateWithOneIndex("test-index", 5, 1, true).nodes(nodesWithLocalNodeClusterManager())
            .build();

        ClusterState updatedState = generateClusterStateWithOneIndex("test-index", 5, 2, false).nodes(nodesWithLocalNodeClusterManager())
            .build();

        ClusterStateDiffManifest diffManifest = verifyRoutingTableDiffManifest(initialState, updatedState);
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        diffManifest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final ClusterStateDiffManifest parsedManifest = ClusterStateDiffManifest.fromXContent(parser, CODEC_V3);
            assertEquals(diffManifest, parsedManifest);
        }
    }

    public void testClusterStateWithRoutingTableDiffInDiffManifestXContent1() throws IOException {
        ClusterState initialState = generateClusterStateWithOneIndex("test-index", 5, 1, true).nodes(nodesWithLocalNodeClusterManager())
            .build();

        ClusterState updatedState = generateClusterStateWithOneIndex("test-index-1", 5, 2, false).nodes(nodesWithLocalNodeClusterManager())
            .build();

        ClusterStateDiffManifest diffManifest = verifyRoutingTableDiffManifest(initialState, updatedState);
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        diffManifest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final ClusterStateDiffManifest parsedManifest = ClusterStateDiffManifest.fromXContent(parser, CODEC_V3);
            assertEquals(diffManifest, parsedManifest);
        }
    }

    private ClusterStateDiffManifest verifyRoutingTableDiffManifest(ClusterState previousState, ClusterState currentState) {
        // Create initial and updated IndexRoutingTable maps
        Map<String, IndexRoutingTable> initialRoutingTableMap = previousState.getRoutingTable().indicesRouting();
        Map<String, IndexRoutingTable> updatedRoutingTableMap = currentState.getRoutingTable().indicesRouting();

        DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> routingTableIncrementalDiff = DiffableUtils.diff(
            initialRoutingTableMap,
            updatedRoutingTableMap,
            DiffableUtils.getStringKeySerializer(),
            CUSTOM_ROUTING_TABLE_DIFFABLE_VALUE_SERIALIZER
        );
        ClusterStateDiffManifest manifest = new ClusterStateDiffManifest(
            currentState,
            previousState,
            routingTableIncrementalDiff,
            "indicesRoutingDiffPath"
        );
        assertEquals("indicesRoutingDiffPath", manifest.getIndicesRoutingDiffPath());
        assertEquals(routingTableIncrementalDiff.getUpserts().size(), manifest.getIndicesRoutingUpdated().size());
        assertEquals(routingTableIncrementalDiff.getDeletes().size(), manifest.getIndicesRoutingDeleted().size());
        return manifest;
    }

    private ClusterStateDiffManifest updateAndVerifyState(
        ClusterState initialState,
        List<IndexMetadata> indicesToAdd,
        List<String> indicesToRemove,
        Map<String, Metadata.Custom> customsToAdd,
        List<String> customsToRemove,
        Map<String, ClusterState.Custom> clusterStateCustomsToAdd,
        List<String> clusterStateCustomsToRemove,
        boolean updateCoordinationState,
        boolean updatePersistentSettings,
        boolean updateTemplates,
        boolean updateTransientSettings,
        boolean updateDiscoveryNodes,
        boolean updateClusterBlocks,
        boolean updateHashesOfConsistentSettings
    ) {
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(initialState);
        Metadata.Builder metadataBuilder = Metadata.builder(initialState.metadata());
        for (IndexMetadata indexMetadata : indicesToAdd) {
            metadataBuilder.put(indexMetadata, true);
        }
        indicesToRemove.forEach(metadataBuilder::remove);
        for (String custom : customsToAdd.keySet()) {
            metadataBuilder.putCustom(custom, customsToAdd.get(custom));
        }
        customsToRemove.forEach(metadataBuilder::removeCustom);
        for (String custom : clusterStateCustomsToAdd.keySet()) {
            clusterStateBuilder.putCustom(custom, clusterStateCustomsToAdd.get(custom));
        }
        clusterStateCustomsToRemove.forEach(clusterStateBuilder::removeCustom);
        if (updateCoordinationState) {
            metadataBuilder.coordinationMetadata(
                CoordinationMetadata.builder(initialState.metadata().coordinationMetadata())
                    .addVotingConfigExclusion(new CoordinationMetadata.VotingConfigExclusion("exlucdedNodeId", "excludedNodeName"))
                    .build()
            );
        }
        if (updatePersistentSettings) {
            metadataBuilder.persistentSettings(Settings.builder().put("key", "value").build());
        }
        if (updateTemplates) {
            metadataBuilder.templates(
                TemplatesMetadata.builder()
                    .put(
                        IndexTemplateMetadata.builder("template" + randomAlphaOfLength(3))
                            .patterns(asList("bar-*", "foo-*"))
                            .settings(
                                Settings.builder().put("random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5)).build()
                            )
                            .build()
                    )
                    .build()
            );
        }
        if (updateTransientSettings) {
            metadataBuilder.transientSettings(Settings.builder().put("key", "value").build());
        }
        if (updateDiscoveryNodes) {
            clusterStateBuilder.nodes(
                DiscoveryNodes.builder(initialState.nodes())
                    .add(new DiscoveryNode("new-cluster-manager", new TransportAddress(META_ADDRESS, 9200), CURRENT))
                    .clusterManagerNodeId("new-cluster-manager")
            );
        }
        if (updateHashesOfConsistentSettings) {
            metadataBuilder.hashesOfConsistentSettings(Collections.singletonMap("key", "value"));
        }
        if (updateClusterBlocks) {
            clusterStateBuilder.blocks(randomClusterBlocks());
        }
        ClusterState updatedClusterState = clusterStateBuilder.metadata(metadataBuilder.build()).build();

        ClusterStateDiffManifest manifest = new ClusterStateDiffManifest(updatedClusterState, initialState, null, null);
        assertEquals(indicesToAdd.stream().map(im -> im.getIndex().getName()).collect(toList()), manifest.getIndicesUpdated());
        assertEquals(indicesToRemove, manifest.getIndicesDeleted());
        assertEquals(new ArrayList<>(customsToAdd.keySet()), manifest.getCustomMetadataUpdated());
        assertEquals(customsToRemove, manifest.getCustomMetadataDeleted());
        assertEquals(new ArrayList<>(clusterStateCustomsToAdd.keySet()), manifest.getClusterStateCustomUpdated());
        assertEquals(clusterStateCustomsToRemove, manifest.getClusterStateCustomDeleted());
        assertEquals(updateCoordinationState, manifest.isCoordinationMetadataUpdated());
        assertEquals(updatePersistentSettings, manifest.isSettingsMetadataUpdated());
        assertEquals(updateTemplates, manifest.isTemplatesMetadataUpdated());
        assertEquals(updateTransientSettings, manifest.isTransientSettingsMetadataUpdated());
        assertEquals(updateDiscoveryNodes, manifest.isDiscoveryNodesUpdated());
        assertEquals(updateClusterBlocks, manifest.isClusterBlocksUpdated());
        assertEquals(updateHashesOfConsistentSettings, manifest.isHashesOfConsistentSettingsUpdated());
        return manifest;
    }
}
