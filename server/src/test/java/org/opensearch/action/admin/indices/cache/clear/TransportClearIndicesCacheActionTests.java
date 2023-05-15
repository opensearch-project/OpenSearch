/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.cache.clear;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.indices.IndicesService;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.EnumSet;

import static org.mockito.Mockito.mock;

public class TransportClearIndicesCacheActionTests extends OpenSearchTestCase {
    private final TransportClearIndicesCacheAction action = new TransportClearIndicesCacheAction(
        mock(ClusterService.class),
        mock(TransportService.class),
        mock(IndicesService.class),
        mock(ActionFilters.class),
        mock(IndexNameExpressionResolver.class)
    );

    private final ClusterBlock writeClusterBlock = new ClusterBlock(
        1,
        "uuid",
        "",
        true,
        true,
        true,
        RestStatus.OK,
        EnumSet.of(ClusterBlockLevel.METADATA_WRITE)
    );

    private final ClusterBlock readClusterBlock = new ClusterBlock(
        1,
        "uuid",
        "",
        true,
        true,
        true,
        RestStatus.OK,
        EnumSet.of(ClusterBlockLevel.METADATA_READ)
    );

    public void testGlobalBlockCheck() {
        ClusterBlocks.Builder builder = ClusterBlocks.builder();
        builder.addGlobalBlock(writeClusterBlock);
        ClusterState metadataWriteBlockedState = ClusterState.builder(ClusterState.EMPTY_STATE).blocks(builder).build();
        assertNull(action.checkGlobalBlock(metadataWriteBlockedState, new ClearIndicesCacheRequest()));

        builder = ClusterBlocks.builder();
        builder.addGlobalBlock(readClusterBlock);
        ClusterState metadataReadBlockedState = ClusterState.builder(ClusterState.EMPTY_STATE).blocks(builder).build();
        assertNotNull(action.checkGlobalBlock(metadataReadBlockedState, new ClearIndicesCacheRequest()));
    }

    public void testIndexBlockCheck() {
        String indexName = "test";
        ClusterBlocks.Builder builder = ClusterBlocks.builder();
        builder.addIndexBlock(indexName, writeClusterBlock);
        ClusterState metadataWriteBlockedState = ClusterState.builder(ClusterState.EMPTY_STATE).blocks(builder).build();
        assertNull(action.checkRequestBlock(metadataWriteBlockedState, new ClearIndicesCacheRequest(), new String[] { indexName }));

        builder = ClusterBlocks.builder();
        builder.addIndexBlock(indexName, readClusterBlock);
        ClusterState metadataReadBlockedState = ClusterState.builder(ClusterState.EMPTY_STATE).blocks(builder).build();
        assertNotNull(action.checkRequestBlock(metadataReadBlockedState, new ClearIndicesCacheRequest(), new String[] { indexName }));
    }
}
