/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterState.Custom;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.TestCapturingListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.gateway.remote.model.RemoteClusterBlocks;
import org.opensearch.gateway.remote.model.RemoteClusterStateCustoms;
import org.opensearch.gateway.remote.model.RemoteDiscoveryNodes;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.opensearch.common.blobstore.stream.write.WritePriority.URGENT;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTE;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.DISCOVERY_NODES;
import static org.opensearch.gateway.remote.RemoteClusterStateTestUtils.TestClusterStateCustom1;
import static org.opensearch.gateway.remote.RemoteClusterStateTestUtils.TestClusterStateCustom2;
import static org.opensearch.gateway.remote.RemoteClusterStateTestUtils.TestClusterStateCustom3;
import static org.opensearch.gateway.remote.RemoteClusterStateTestUtils.TestClusterStateCustom4;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.CLUSTER_STATE_EPHEMERAL_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.CLUSTER_STATE_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.CUSTOM_DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.encodeString;
import static org.opensearch.gateway.remote.model.RemoteClusterBlocks.CLUSTER_BLOCKS;
import static org.opensearch.gateway.remote.model.RemoteClusterBlocks.CLUSTER_BLOCKS_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteClusterBlocksTests.randomClusterBlocks;
import static org.opensearch.gateway.remote.model.RemoteClusterStateCustoms.CLUSTER_STATE_CUSTOM;
import static org.opensearch.gateway.remote.model.RemoteClusterStateCustomsTests.getClusterStateCustom;
import static org.opensearch.gateway.remote.model.RemoteDiscoveryNodes.DISCOVERY_NODES_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteDiscoveryNodesTests.getDiscoveryNodes;
import static org.opensearch.index.remote.RemoteStoreUtils.invertLong;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteClusterStateAttributesManagerTests extends OpenSearchTestCase {
    private RemoteClusterStateAttributesManager remoteClusterStateAttributesManager;
    private BlobStoreTransferService blobStoreTransferService;
    private Compressor compressor;
    private final ThreadPool threadPool = new TestThreadPool(RemoteClusterStateAttributesManagerTests.class.getName());
    private final long VERSION = 7331L;
    private NamedWriteableRegistry namedWriteableRegistry;
    private final String CLUSTER_NAME = "test-cluster";
    private final String CLUSTER_UUID = "test-cluster-uuid";

    @Before
    public void setup() throws Exception {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        namedWriteableRegistry = writableRegistry();
        BlobStoreRepository blobStoreRepository = mock(BlobStoreRepository.class);
        when(blobStoreRepository.basePath()).thenReturn(new BlobPath());
        blobStoreTransferService = mock(BlobStoreTransferService.class);
        compressor = new NoneCompressor();

        remoteClusterStateAttributesManager = new RemoteClusterStateAttributesManager(
            CLUSTER_NAME,
            blobStoreRepository,
            blobStoreTransferService,
            writableRegistry(),
            threadPool
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testGetAsyncWriteRunnable_DiscoveryNodes() throws IOException, InterruptedException {
        DiscoveryNodes discoveryNodes = getDiscoveryNodes();
        RemoteDiscoveryNodes remoteDiscoveryNodes = new RemoteDiscoveryNodes(discoveryNodes, VERSION, CLUSTER_UUID, compressor);
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        final CountDownLatch latch = new CountDownLatch(1);
        final TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        remoteClusterStateAttributesManager.writeAsync(DISCOVERY_NODES, remoteDiscoveryNodes, new LatchedActionListener<>(listener, latch));
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertEquals(DISCOVERY_NODES, listener.getResult().getComponent());
        String uploadedFileName = listener.getResult().getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(RemoteClusterStateUtils.encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(CLUSTER_STATE_EPHEMERAL_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(DISCOVERY_NODES, splitFileName[0]);
        assertEquals(invertLong(VERSION), splitFileName[1]);
        assertEquals(CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetAsyncReadRunnable_DiscoveryNodes() throws IOException, InterruptedException {
        DiscoveryNodes discoveryNodes = getDiscoveryNodes();
        String fileName = randomAlphaOfLength(10);
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            DISCOVERY_NODES_FORMAT.serialize(discoveryNodes, fileName, compressor).streamInput()
        );
        RemoteDiscoveryNodes remoteObjForDownload = new RemoteDiscoveryNodes(fileName, "cluster-uuid", compressor);
        CountDownLatch latch = new CountDownLatch(1);
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();
        remoteClusterStateAttributesManager.readAsync(DISCOVERY_NODES, remoteObjForDownload, new LatchedActionListener<>(listener, latch));
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertEquals(CLUSTER_STATE_ATTRIBUTE, listener.getResult().getComponent());
        assertEquals(DISCOVERY_NODES, listener.getResult().getComponentName());
        DiscoveryNodes readDiscoveryNodes = (DiscoveryNodes) listener.getResult().getObj();
        assertEquals(discoveryNodes.getSize(), readDiscoveryNodes.getSize());
        discoveryNodes.getNodes().forEach((nodeId, node) -> assertEquals(readDiscoveryNodes.get(nodeId), node));
        assertEquals(discoveryNodes.getClusterManagerNodeId(), readDiscoveryNodes.getClusterManagerNodeId());
    }

    public void testGetAsyncWriteRunnable_ClusterBlocks() throws IOException, InterruptedException {
        ClusterBlocks clusterBlocks = randomClusterBlocks();
        RemoteClusterBlocks remoteClusterBlocks = new RemoteClusterBlocks(clusterBlocks, VERSION, CLUSTER_UUID, compressor);
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        final CountDownLatch latch = new CountDownLatch(1);
        final TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        remoteClusterStateAttributesManager.writeAsync(CLUSTER_BLOCKS, remoteClusterBlocks, new LatchedActionListener<>(listener, latch));
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertEquals(CLUSTER_BLOCKS, listener.getResult().getComponent());
        String uploadedFileName = listener.getResult().getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(CLUSTER_STATE_EPHEMERAL_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(CLUSTER_BLOCKS, splitFileName[0]);
        assertEquals(invertLong(VERSION), splitFileName[1]);
        assertEquals(CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetAsyncReadRunnable_ClusterBlocks() throws IOException, InterruptedException {
        ClusterBlocks clusterBlocks = randomClusterBlocks();
        String fileName = randomAlphaOfLength(10);
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            CLUSTER_BLOCKS_FORMAT.serialize(clusterBlocks, fileName, compressor).streamInput()
        );
        RemoteClusterBlocks remoteClusterBlocks = new RemoteClusterBlocks(fileName, "cluster-uuid", compressor);
        CountDownLatch latch = new CountDownLatch(1);
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();

        remoteClusterStateAttributesManager.readAsync(CLUSTER_BLOCKS, remoteClusterBlocks, new LatchedActionListener<>(listener, latch));
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertEquals(CLUSTER_STATE_ATTRIBUTE, listener.getResult().getComponent());
        assertEquals(CLUSTER_BLOCKS, listener.getResult().getComponentName());
        ClusterBlocks readClusterBlocks = (ClusterBlocks) listener.getResult().getObj();
        assertEquals(clusterBlocks.global(), readClusterBlocks.global());
        assertEquals(clusterBlocks.indices().keySet(), readClusterBlocks.indices().keySet());
        for (String index : clusterBlocks.indices().keySet()) {
            assertEquals(clusterBlocks.indices().get(index), readClusterBlocks.indices().get(index));
        }
    }

    public void testGetAsyncWriteRunnable_Custom() throws IOException, InterruptedException {
        Custom custom = getClusterStateCustom();
        RemoteClusterStateCustoms remoteClusterStateCustoms = new RemoteClusterStateCustoms(
            custom,
            custom.getWriteableName(),
            VERSION,
            CLUSTER_UUID,
            compressor,
            namedWriteableRegistry
        );
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));
        final TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        final CountDownLatch latch = new CountDownLatch(1);
        remoteClusterStateAttributesManager.writeAsync(
            CLUSTER_STATE_CUSTOM,
            remoteClusterStateCustoms,
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertEquals(String.join(CUSTOM_DELIMITER, CLUSTER_STATE_CUSTOM, custom.getWriteableName()), listener.getResult().getComponent());
        String uploadedFileName = listener.getResult().getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(5, pathTokens.length);
        assertEquals(encodeString(CLUSTER_NAME), pathTokens[0]);
        assertEquals(CLUSTER_STATE_PATH_TOKEN, pathTokens[1]);
        assertEquals(CLUSTER_UUID, pathTokens[2]);
        assertEquals(CLUSTER_STATE_EPHEMERAL_PATH_TOKEN, pathTokens[3]);
        String[] splitFileName = pathTokens[4].split(DELIMITER);
        assertEquals(4, splitFileName.length);
        assertEquals(String.join(CUSTOM_DELIMITER, CLUSTER_STATE_CUSTOM, custom.getWriteableName()), splitFileName[0]);
        assertEquals(invertLong(VERSION), splitFileName[1]);
        assertEquals(CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION, Integer.parseInt(splitFileName[3]));
    }

    public void testGetAsyncReadRunnable_Custom() throws IOException, InterruptedException {
        Custom custom = getClusterStateCustom();
        String fileName = randomAlphaOfLength(10);
        RemoteClusterStateCustoms remoteClusterStateCustoms = new RemoteClusterStateCustoms(
            fileName,
            custom.getWriteableName(),
            CLUSTER_UUID,
            compressor,
            namedWriteableRegistry
        );
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            remoteClusterStateCustoms.clusterStateCustomsFormat.serialize(custom, fileName, compressor).streamInput()
        );
        TestCapturingListener<RemoteReadResult> capturingListener = new TestCapturingListener<>();
        final CountDownLatch latch = new CountDownLatch(1);
        remoteClusterStateAttributesManager.readAsync(
            CLUSTER_STATE_CUSTOM,
            remoteClusterStateCustoms,
            new LatchedActionListener<>(capturingListener, latch)
        );
        latch.await();
        assertNull(capturingListener.getFailure());
        assertNotNull(capturingListener.getResult());
        assertEquals(custom, capturingListener.getResult().getObj());
        assertEquals(CLUSTER_STATE_ATTRIBUTE, capturingListener.getResult().getComponent());
        assertEquals(CLUSTER_STATE_CUSTOM, capturingListener.getResult().getComponentName());
    }

    public void testGetAsyncWriteRunnable_Exception() throws IOException, InterruptedException {
        DiscoveryNodes discoveryNodes = getDiscoveryNodes();
        RemoteDiscoveryNodes remoteDiscoveryNodes = new RemoteDiscoveryNodes(discoveryNodes, VERSION, CLUSTER_UUID, compressor);

        IOException ioException = new IOException("mock test exception");
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onFailure(ioException);
            return null;
        }).when(blobStoreTransferService)
            .uploadBlob(any(InputStream.class), anyIterable(), anyString(), eq(URGENT), any(ActionListener.class));

        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> capturingListener = new TestCapturingListener<>();
        final CountDownLatch latch = new CountDownLatch(1);
        remoteClusterStateAttributesManager.writeAsync(
            DISCOVERY_NODES,
            remoteDiscoveryNodes,
            new LatchedActionListener<>(capturingListener, latch)
        );
        latch.await();
        assertNull(capturingListener.getResult());
        assertTrue(capturingListener.getFailure() instanceof RemoteStateTransferException);
        assertEquals(ioException, capturingListener.getFailure().getCause());
    }

    public void testGetAsyncReadRunnable_Exception() throws IOException, InterruptedException {
        String fileName = randomAlphaOfLength(10);
        RemoteDiscoveryNodes remoteDiscoveryNodes = new RemoteDiscoveryNodes(fileName, CLUSTER_UUID, compressor);
        Exception ioException = new IOException("mock test exception");
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenThrow(ioException);
        CountDownLatch latch = new CountDownLatch(1);
        TestCapturingListener<RemoteReadResult> capturingListener = new TestCapturingListener<>();
        remoteClusterStateAttributesManager.readAsync(
            DISCOVERY_NODES,
            remoteDiscoveryNodes,
            new LatchedActionListener<>(capturingListener, latch)
        );
        latch.await();
        assertNull(capturingListener.getResult());
        assertEquals(ioException, capturingListener.getFailure().getCause());
        assertTrue(capturingListener.getFailure() instanceof RemoteStateTransferException);
    }

    public void testGetUpdatedCustoms() {
        Map<String, ClusterState.Custom> previousCustoms = Map.of(
            TestClusterStateCustom1.TYPE,
            new TestClusterStateCustom1("data1"),
            TestClusterStateCustom2.TYPE,
            new TestClusterStateCustom2("data2"),
            TestClusterStateCustom3.TYPE,
            new TestClusterStateCustom3("data3")
        );
        ClusterState previousState = ClusterState.builder(new ClusterName("test-cluster")).customs(previousCustoms).build();

        Map<String, Custom> currentCustoms = Map.of(
            TestClusterStateCustom2.TYPE,
            new TestClusterStateCustom2("data2"),
            TestClusterStateCustom3.TYPE,
            new TestClusterStateCustom3("data3-changed"),
            TestClusterStateCustom4.TYPE,
            new TestClusterStateCustom4("data4")
        );

        ClusterState currentState = ClusterState.builder(new ClusterName("test-cluster")).customs(currentCustoms).build();

        DiffableUtils.MapDiff<String, ClusterState.Custom, Map<String, ClusterState.Custom>> customsDiff =
            remoteClusterStateAttributesManager.getUpdatedCustoms(currentState, previousState, false, randomBoolean());
        assertThat(customsDiff.getUpserts(), is(Collections.emptyMap()));
        assertThat(customsDiff.getDeletes(), is(Collections.emptyList()));

        customsDiff = remoteClusterStateAttributesManager.getUpdatedCustoms(currentState, previousState, true, true);
        assertThat(customsDiff.getUpserts(), is(currentCustoms));
        assertThat(customsDiff.getDeletes(), is(Collections.emptyList()));

        Map<String, ClusterState.Custom> expectedCustoms = Map.of(
            TestClusterStateCustom3.TYPE,
            new TestClusterStateCustom3("data3-changed"),
            TestClusterStateCustom4.TYPE,
            new TestClusterStateCustom4("data4")
        );

        customsDiff = remoteClusterStateAttributesManager.getUpdatedCustoms(currentState, previousState, true, false);
        assertThat(customsDiff.getUpserts(), is(expectedCustoms));
        assertThat(customsDiff.getDeletes(), is(List.of(TestClusterStateCustom1.TYPE)));
    }
}
