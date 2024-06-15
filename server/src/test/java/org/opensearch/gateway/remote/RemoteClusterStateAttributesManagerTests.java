/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.gateway.remote.model.RemoteClusterBlocks;
import org.opensearch.gateway.remote.model.RemoteDiscoveryNodes;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyList;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.DISCOVERY_NODES;
import static org.opensearch.gateway.remote.model.RemoteClusterBlocks.CLUSTER_BLOCKS;
import static org.opensearch.gateway.remote.model.RemoteClusterBlocks.CLUSTER_BLOCKS_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteClusterBlocksTests.randomClusterBlocks;
import static org.opensearch.gateway.remote.model.RemoteDiscoveryNodes.DISCOVERY_NODES_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteDiscoveryNodesTests.getDiscoveryNodes;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteClusterStateAttributesManagerTests extends OpenSearchTestCase {
    private RemoteClusterStateAttributesManager remoteClusterStateAttributesManager;
    private BlobStoreTransferService blobStoreTransferService;
    private BlobStoreRepository blobStoreRepository;
    private Compressor compressor;
    private ThreadPool threadpool = new TestThreadPool(RemoteClusterStateAttributesManagerTests.class.getName());

    @Before
    public void setup() throws Exception {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(emptyList());
        blobStoreRepository = mock(BlobStoreRepository.class);
        blobStoreTransferService = mock(BlobStoreTransferService.class);
        compressor = new NoneCompressor();

        remoteClusterStateAttributesManager = new RemoteClusterStateAttributesManager(
            "test-cluster",
            blobStoreRepository,
            blobStoreTransferService,
            namedWriteableRegistry,
            threadpool
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadpool.shutdown();
    }

    public void testGetAsyncMetadataReadAction_DiscoveryNodes() throws IOException {
        DiscoveryNodes discoveryNodes = getDiscoveryNodes();
        String fileName = randomAlphaOfLength(10);
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            DISCOVERY_NODES_FORMAT.serialize(discoveryNodes, fileName, compressor).streamInput()
        );
        RemoteDiscoveryNodes remoteObjForDownload = new RemoteDiscoveryNodes(fileName, "cluster-uuid", compressor);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DiscoveryNodes> readDiscoveryNodes = new AtomicReference<>();
        LatchedActionListener<RemoteReadResult> assertingListener = new LatchedActionListener<>(
            ActionListener.wrap(response -> readDiscoveryNodes.set((DiscoveryNodes) response.getObj()), Assert::assertNull),
            latch
        );
        CheckedRunnable<IOException> runnable = remoteClusterStateAttributesManager.getAsyncMetadataReadAction(
            DISCOVERY_NODES,
            remoteObjForDownload,
            assertingListener
        );

        try {
            runnable.run();
            latch.await();
            assertEquals(discoveryNodes.getSize(), readDiscoveryNodes.get().getSize());
            discoveryNodes.getNodes().forEach((nodeId, node) -> assertEquals(readDiscoveryNodes.get().get(nodeId), node));
            assertEquals(discoveryNodes.getClusterManagerNodeId(), readDiscoveryNodes.get().getClusterManagerNodeId());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testGetAsyncMetadataReadAction_ClusterBlocks() throws IOException {
        ClusterBlocks clusterBlocks = randomClusterBlocks();
        String fileName = randomAlphaOfLength(10);
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            CLUSTER_BLOCKS_FORMAT.serialize(clusterBlocks, fileName, compressor).streamInput()
        );
        RemoteClusterBlocks remoteClusterBlocks = new RemoteClusterBlocks(fileName, "cluster-uuid", compressor);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ClusterBlocks> readClusterBlocks = new AtomicReference<>();
        LatchedActionListener<RemoteReadResult> assertingListener = new LatchedActionListener<>(
            ActionListener.wrap(response -> readClusterBlocks.set((ClusterBlocks) response.getObj()), Assert::assertNull),
            latch
        );

        CheckedRunnable<IOException> runnable = remoteClusterStateAttributesManager.getAsyncMetadataReadAction(
            CLUSTER_BLOCKS,
            remoteClusterBlocks,
            assertingListener
        );

        try {
            runnable.run();
            latch.await();
            assertEquals(clusterBlocks.global(), readClusterBlocks.get().global());
            assertEquals(clusterBlocks.indices().keySet(), readClusterBlocks.get().indices().keySet());
            for (String index : clusterBlocks.indices().keySet()) {
                assertEquals(clusterBlocks.indices().get(index), readClusterBlocks.get().indices().get(index));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
