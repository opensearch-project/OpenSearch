/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.tiering;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.index.IndexModule.INDEX_STORE_LOCALITY_SETTING;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;

public class HotToWarmTieringServiceTests extends OpenSearchSingleNodeTestCase {

    private ClusterService clusterService;
    private HotToWarmTieringService hotToWarmTieringService;

    @Before
    public void beforeTest() {
        clusterService = this.getInstanceFromNode(ClusterService.class);
        hotToWarmTieringService = this.getInstanceFromNode(HotToWarmTieringService.class);
    }

    public void testUpdateIndexMetadataForAcceptedIndices() {
        String indexName = "test_index";
        createIndex(indexName);
        Index index = resolveIndex(indexName);
        final Metadata.Builder metadataBuilder = Metadata.builder(clusterService.state().metadata());
        hotToWarmTieringService.updateIndexMetadataForAcceptedIndex(
            metadataBuilder,
            clusterService.state().metadata().index(index)
        );
        IndexMetadata indexMetadata = metadataBuilder.build().index(indexName);
        assertEquals(
            IndexModule.DataLocalityType.PARTIAL,
            IndexModule.DataLocalityType.getValueOf(indexMetadata.getSettings().get(INDEX_STORE_LOCALITY_SETTING.getKey()))
        );
        assertEquals(IndexModule.TieringState.HOT_TO_WARM.name(), indexMetadata.getSettings().get(INDEX_TIERING_STATE.getKey()));
        Map<String, String> customData = indexMetadata.getCustomData(IndexMetadata.TIERING_CUSTOM_KEY);
        assertNotNull(customData);
        assertNotNull(customData.get(HotToWarmTieringService.TIERING_START_TIME));
    }

    public void testUpdateIndexMetadataForSuccessfulIndex() {
        String indexName = "test_index";
        createIndex(indexName);
        Index index = resolveIndex(indexName);
        final Metadata.Builder metadataBuilder = Metadata.builder(clusterService.state().metadata());
        Map<String, String> customData = new HashMap<>();
        customData.put(HotToWarmTieringService.TIERING_START_TIME, String.valueOf(System.currentTimeMillis()));
        metadataBuilder.put(IndexMetadata.builder(metadataBuilder.getSafe(index)).putCustom(IndexMetadata.TIERING_CUSTOM_KEY, customData));
//        hotToWarmTieringService.updateIndexMetadataForTieredIndex(metadataBuilder, clusterService.state().metadata().index(index));
        IndexMetadata indexMetadata = metadataBuilder.build().index(indexName);
        assertEquals(IndexModule.TieringState.WARM.name(), indexMetadata.getSettings().get(INDEX_TIERING_STATE.getKey()));
        customData = indexMetadata.getCustomData(IndexMetadata.TIERING_CUSTOM_KEY);
        assertNull(customData);
    }
}
