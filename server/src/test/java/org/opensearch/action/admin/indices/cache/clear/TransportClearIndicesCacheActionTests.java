/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.cache.clear;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.filecache.FileCacheTests;
import org.opensearch.indices.IndicesService;
import org.opensearch.node.Node;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportClearIndicesCacheActionTests extends OpenSearchTestCase {

    private final Node testNode = mock(Node.class);
    private final TransportClearIndicesCacheAction action = new TransportClearIndicesCacheAction(
        mock(ClusterService.class),
        mock(TransportService.class),
        mock(IndicesService.class),
        testNode,
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

    public void testOnShardOperation() throws IOException {
        final String indexName = "test";
        final Settings settings = buildEnvSettings(Settings.EMPTY);
        final Environment environment = TestEnvironment.newEnvironment(settings);
        try (final NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, environment)) {
            // Initialize necessary stubs for the filecache clear shard operation
            final ShardId shardId = new ShardId(indexName, indexName, 1);
            final ShardRouting shardRouting = mock(ShardRouting.class);
            when(shardRouting.shardId()).thenReturn(shardId);
            final ShardPath shardPath = ShardPath.loadFileCachePath(nodeEnvironment, shardId);
            final Path cacheEntryPath = shardPath.getDataPath();
            final FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(1024 * 1024, 16, new NoopCircuitBreaker(""));

            when(testNode.fileCache()).thenReturn(fileCache);
            when(testNode.getNodeEnvironment()).thenReturn(nodeEnvironment);

            // Add an entry into the filecache and reduce the ref count
            fileCache.put(cacheEntryPath, new FileCacheTests.StubCachedIndexInput(1));
            fileCache.decRef(cacheEntryPath);

            // Check if the entry exists and reduce the ref count to make it evictable
            assertNotNull(fileCache.get(cacheEntryPath));
            fileCache.decRef(cacheEntryPath);

            ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest();
            clearIndicesCacheRequest.fileCache(true);
            assertEquals(
                TransportBroadcastByNodeAction.EmptyResult.INSTANCE,
                action.shardOperation(clearIndicesCacheRequest, shardRouting)
            );
            assertNull(fileCache.get(cacheEntryPath));
        }
    }

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
