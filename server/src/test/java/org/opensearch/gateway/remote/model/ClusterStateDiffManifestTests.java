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
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.gateway.remote.ClusterStateDiffManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.opensearch.Version.CURRENT;
import static org.opensearch.cluster.ClusterState.EMPTY_STATE;
import static org.opensearch.core.common.transport.TransportAddress.META_ADDRESS;
import static org.opensearch.gateway.remote.model.RemoteClusterBlocksTests.randomClusterBlocks;

public class ClusterStateDiffManifestTests extends OpenSearchTestCase {

    public void testClusterStateDiffManifest() {
        ClusterState initialState = ClusterState.builder(EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("index-1").settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            ).build();
        verifyDiff(
            initialState,
            singletonList(IndexMetadata.builder("index-2").settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).build()),
            singletonList("index-1"),
            Collections.emptyMap(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptyList(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
    }

    private void verifyDiff(
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
            metadataBuilder.persistentSettings(
                Settings.builder().put("key", "value").build()
            );
        }
        if (updateTemplates) {
            metadataBuilder.templates(TemplatesMetadata.builder()
                .put(
                    IndexTemplateMetadata.builder("template" + randomAlphaOfLength(3))
                        .patterns(asList("bar-*", "foo-*"))
                        .settings(
                            Settings.builder()
                                .put("random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5))
                                .build()
                        )
                        .build()
                ).build()
            );
        }
        if (updateTransientSettings) {
            metadataBuilder.transientSettings(
                Settings.builder().put("key", "value").build()
            );
        }
        if (updateDiscoveryNodes) {
            clusterStateBuilder.nodes(DiscoveryNodes.builder(initialState.nodes())
                .add(new DiscoveryNode("new-cluster-manager", new TransportAddress(META_ADDRESS, 9200), CURRENT))
                .clusterManagerNodeId("new-cluster-manager"));
        }
        if (updateHashesOfConsistentSettings) {
            metadataBuilder.hashesOfConsistentSettings(Collections.singletonMap("key", "value"));
        }
        if (updateClusterBlocks) {
            clusterStateBuilder.blocks(randomClusterBlocks());
        }
        ClusterState updatedClusterState = clusterStateBuilder.metadata(metadataBuilder.build()).build();

        ClusterStateDiffManifest manifest = new ClusterStateDiffManifest(updatedClusterState, initialState);
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
    }
}
