/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.RepositoryCleanupInProgress;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.DiffableStringMap;
import org.opensearch.cluster.metadata.IndexGraveyard;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.remote.InternalRemoteRoutingTableService;
import org.opensearch.cluster.routing.remote.NoopRemoteRoutingTableService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest;
import org.opensearch.gateway.remote.model.RemoteClusterStateManifestInfo;
import org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata;
import org.opensearch.index.remote.RemoteIndexPathUploader;
import org.opensearch.indices.IndicesModule;
import org.opensearch.repositories.FilterRepository;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumWritableBlobStoreFormat;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.opensearch.common.util.FeatureFlags.REMOTE_PUBLICATION_EXPERIMENTAL;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V1;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V2;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_BLOCKS;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTE;
import static org.opensearch.gateway.remote.RemoteClusterStateTestUtils.CustomMetadata1;
import static org.opensearch.gateway.remote.RemoteClusterStateTestUtils.CustomMetadata2;
import static org.opensearch.gateway.remote.RemoteClusterStateTestUtils.CustomMetadata3;
import static org.opensearch.gateway.remote.RemoteClusterStateTestUtils.TestClusterStateCustom1;
import static org.opensearch.gateway.remote.RemoteClusterStateTestUtils.TestClusterStateCustom2;
import static org.opensearch.gateway.remote.RemoteClusterStateTestUtils.TestClusterStateCustom3;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.CUSTOM_DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.FORMAT_PARAMS;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.getFormattedIndexFileName;
import static org.opensearch.gateway.remote.model.RemoteClusterBlocks.CLUSTER_BLOCKS_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteClusterBlocksTests.randomClusterBlocks;
import static org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.model.RemoteClusterStateCustoms.CLUSTER_STATE_CUSTOM;
import static org.opensearch.gateway.remote.model.RemoteCoordinationMetadata.COORDINATION_METADATA;
import static org.opensearch.gateway.remote.model.RemoteCoordinationMetadata.COORDINATION_METADATA_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadata.CUSTOM_METADATA;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadata.readFrom;
import static org.opensearch.gateway.remote.model.RemoteDiscoveryNodes.DISCOVERY_NODES;
import static org.opensearch.gateway.remote.model.RemoteDiscoveryNodes.DISCOVERY_NODES_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteDiscoveryNodesTests.getDiscoveryNodes;
import static org.opensearch.gateway.remote.model.RemoteGlobalMetadata.GLOBAL_METADATA_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettings.HASHES_OF_CONSISTENT_SETTINGS;
import static org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettings.HASHES_OF_CONSISTENT_SETTINGS_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettingsTests.getHashesOfConsistentSettings;
import static org.opensearch.gateway.remote.model.RemoteIndexMetadata.INDEX;
import static org.opensearch.gateway.remote.model.RemoteIndexMetadata.INDEX_METADATA_FORMAT;
import static org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata.SETTINGS_METADATA_FORMAT;
import static org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata.SETTING_METADATA;
import static org.opensearch.gateway.remote.model.RemoteTemplatesMetadata.TEMPLATES_METADATA;
import static org.opensearch.gateway.remote.model.RemoteTemplatesMetadata.TEMPLATES_METADATA_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteTemplatesMetadataTests.getTemplatesMetadata;
import static org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA;
import static org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_METADATA_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteClusterStateServiceTests extends OpenSearchTestCase {

    private RemoteClusterStateService remoteClusterStateService;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;
    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private RepositoriesService repositoriesService;
    private BlobStoreRepository blobStoreRepository;
    private Compressor compressor;
    private BlobStore blobStore;
    private Settings settings;
    private boolean publicationEnabled;
    private NamedWriteableRegistry namedWriteableRegistry;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    private static final String NODE_ID = "test-node";
    private static final String COORDINATION_METADATA_FILENAME = "coordination-metadata-file__1";
    private static final String PERSISTENT_SETTINGS_FILENAME = "persistent-settings-file__1";
    private static final String TRANSIENT_SETTINGS_FILENAME = "transient-settings-file__1";
    private static final String TEMPLATES_METADATA_FILENAME = "templates-metadata-file__1";
    private static final String DISCOVERY_NODES_FILENAME = "discovery-nodes-file__1";
    private static final String CLUSTER_BLOCKS_FILENAME = "cluster-blocks-file__1";
    private static final String HASHES_OF_CONSISTENT_SETTINGS_FILENAME = "consistent-settings-hashes-file__1";

    @Before
    public void setup() {
        repositoriesServiceSupplier = mock(Supplier.class);
        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);

        String stateRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            "remote_store_repository"
        );
        String stateRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            "remote_store_repository"
        );

        settings = Settings.builder()
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, "remote_store_repository")
            .put(stateRepoTypeAttributeKey, FsRepository.TYPE)
            .put(stateRepoSettingsAttributeKeyPrefix + "location", "randomRepoPath")
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, "routing_repository")
            .build();
        List<NamedWriteableRegistry.Entry> writeableEntries = ClusterModule.getNamedWriteables();
        writeableEntries.add(new NamedWriteableRegistry.Entry(Metadata.Custom.class, CustomMetadata1.TYPE, CustomMetadata1::new));
        writeableEntries.add(new NamedWriteableRegistry.Entry(Metadata.Custom.class, CustomMetadata2.TYPE, CustomMetadata2::new));
        writeableEntries.add(new NamedWriteableRegistry.Entry(Metadata.Custom.class, CustomMetadata3.TYPE, CustomMetadata3::new));
        namedWriteableRegistry = new NamedWriteableRegistry(writeableEntries);

        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );

        compressor = new DeflateCompressor();
        blobStoreRepository = mock(BlobStoreRepository.class);
        blobStore = mock(BlobStore.class);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(repositoriesService.repository("remote_store_repository")).thenReturn(blobStoreRepository);
        when(repositoriesService.repository("routing_repository")).thenReturn(blobStoreRepository);

        when(blobStoreRepository.getNamedXContentRegistry()).thenReturn(xContentRegistry);
        remoteClusterStateService = new RemoteClusterStateService(
            "test-node-id",
            repositoriesServiceSupplier,
            settings,
            clusterService,
            () -> 0L,
            threadPool,
            List.of(new RemoteIndexPathUploader(threadPool, settings, repositoriesServiceSupplier, clusterSettings)),
            namedWriteableRegistry
        );
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
        remoteClusterStateService.close();
        publicationEnabled = false;
        Settings nodeSettings = Settings.builder().build();
        FeatureFlags.initializeFeatureFlags(nodeSettings);
        threadPool.shutdown();
    }

    public void testFailWriteFullMetadataNonClusterManagerNode() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().build();
        final RemoteClusterStateManifestInfo manifestDetails = remoteClusterStateService.writeFullMetadata(
            clusterState,
            randomAlphaOfLength(10)
        );
        Assert.assertThat(manifestDetails, nullValue());
    }

    public void testFailInitializationWhenRemoteStateDisabled() {
        final Settings settings = Settings.builder().build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        assertThrows(
            AssertionError.class,
            () -> new RemoteClusterStateService(
                "test-node-id",
                repositoriesServiceSupplier,
                settings,
                clusterService,
                () -> 0L,
                threadPool,
                List.of(new RemoteIndexPathUploader(threadPool, settings, repositoriesServiceSupplier, clusterSettings)),
                writableRegistry()
            )
        );
    }

    public void testFailInitializeWhenRepositoryNotSet() {
        doThrow(new RepositoryMissingException("repository missing")).when(repositoriesService).repository("remote_store_repository");
        assertThrows(RepositoryMissingException.class, () -> remoteClusterStateService.start());
    }

    public void testFailWriteFullMetadataWhenNotBlobRepository() {
        final FilterRepository filterRepository = mock(FilterRepository.class);
        when(repositoriesService.repository("remote_store_repository")).thenReturn(filterRepository);
        assertThrows(AssertionError.class, () -> remoteClusterStateService.start());
    }

    public void testWriteFullMetadataSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeFullMetadata(clusterState, "prev-cluster-uuid")
            .getClusterMetadataManifest();
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
        assertThat(manifest.getPreviousClusterUUID(), is(expectedManifest.getPreviousClusterUUID()));
        assertThat(manifest.getGlobalMetadataFileName(), nullValue());
        assertThat(manifest.getCoordinationMetadata(), notNullValue());
        assertThat(manifest.getSettingsMetadata(), notNullValue());
        assertThat(manifest.getTemplatesMetadata(), notNullValue());
        assertFalse(manifest.getCustomMetadataMap().isEmpty());
        assertThat(manifest.getCustomMetadataMap().containsKey(CustomMetadata1.TYPE), is(true));
        assertThat(manifest.getClusterBlocksMetadata(), nullValue());
        assertThat(manifest.getDiscoveryNodesMetadata(), nullValue());
        assertThat(manifest.getTransientSettingsMetadata(), nullValue());
        assertThat(manifest.getHashesOfConsistentSettings(), nullValue());
        assertThat(manifest.getClusterStateCustomMap().size(), is(0));
    }

    public void testWriteFullMetadataSuccessPublicationEnabled() throws IOException {
        // TODO Make the publication flag parameterized
        publicationEnabled = true;
        Settings nodeSettings = Settings.builder().put(REMOTE_PUBLICATION_EXPERIMENTAL, publicationEnabled).build();
        FeatureFlags.initializeFeatureFlags(nodeSettings);
        remoteClusterStateService = new RemoteClusterStateService(
            "test-node-id",
            repositoriesServiceSupplier,
            settings,
            clusterService,
            () -> 0L,
            threadPool,
            List.of(new RemoteIndexPathUploader(threadPool, settings, repositoriesServiceSupplier, clusterSettings)),
            writableRegistry()
        );
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager())
            .customs(
                Map.of(
                    RepositoryCleanupInProgress.TYPE,
                    new RepositoryCleanupInProgress(List.of(new RepositoryCleanupInProgress.Entry("test-repo", 10L)))
                )
            )
            .build();
        mockBlobStoreObjects();
        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeFullMetadata(clusterState, "prev-cluster-uuid")
            .getClusterMetadataManifest();
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
        assertThat(manifest.getPreviousClusterUUID(), is(expectedManifest.getPreviousClusterUUID()));
        assertThat(manifest.getGlobalMetadataFileName(), nullValue());
        assertThat(manifest.getCoordinationMetadata(), notNullValue());
        assertThat(manifest.getSettingsMetadata(), notNullValue());
        assertThat(manifest.getTemplatesMetadata(), notNullValue());
        assertFalse(manifest.getCustomMetadataMap().isEmpty());
        assertThat(manifest.getCustomMetadataMap().containsKey(CustomMetadata1.TYPE), is(true));
        assertThat(manifest.getClusterStateCustomMap().size(), is(1));
        assertThat(manifest.getClusterStateCustomMap().containsKey(RepositoryCleanupInProgress.TYPE), is(true));
    }

    public void testWriteFullMetadataInParallelSuccess() throws IOException {
        // TODO Add test with publication flag enabled
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        AsyncMultiStreamBlobContainer container = (AsyncMultiStreamBlobContainer) mockBlobStoreObjects(AsyncMultiStreamBlobContainer.class);

        ArgumentCaptor<ActionListener<Void>> actionListenerArgumentCaptor = ArgumentCaptor.forClass(ActionListener.class);
        ArgumentCaptor<WriteContext> writeContextArgumentCaptor = ArgumentCaptor.forClass(WriteContext.class);
        ConcurrentHashMap<String, WriteContext> capturedWriteContext = new ConcurrentHashMap<>();
        doAnswer((i) -> {
            actionListenerArgumentCaptor.getValue().onResponse(null);
            WriteContext writeContext = writeContextArgumentCaptor.getValue();
            capturedWriteContext.put(writeContext.getFileName().split(DELIMITER)[0], writeContextArgumentCaptor.getValue());
            return null;
        }).when(container).asyncBlobUpload(writeContextArgumentCaptor.capture(), actionListenerArgumentCaptor.capture());

        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeFullMetadata(clusterState, "prev-cluster-uuid")
            .getClusterMetadataManifest();

        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getGlobalMetadataFileName(), nullValue());
        assertThat(manifest.getCoordinationMetadata(), notNullValue());
        assertThat(manifest.getSettingsMetadata(), notNullValue());
        assertThat(manifest.getTemplatesMetadata(), notNullValue());
        assertThat(manifest.getCustomMetadataMap().size(), not(0));
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
        assertThat(manifest.getPreviousClusterUUID(), is(expectedManifest.getPreviousClusterUUID()));

        assertEquals(7, actionListenerArgumentCaptor.getAllValues().size());
        assertEquals(7, writeContextArgumentCaptor.getAllValues().size());

        byte[] writtenBytes = capturedWriteContext.get("metadata")
            .getStreamProvider(Integer.MAX_VALUE)
            .provideStream(0)
            .getInputStream()
            .readAllBytes();
        IndexMetadata writtenIndexMetadata = INDEX_METADATA_FORMAT.deserialize(
            capturedWriteContext.get("metadata").getFileName(),
            blobStoreRepository.getNamedXContentRegistry(),
            new BytesArray(writtenBytes)
        );

        assertEquals(capturedWriteContext.get("metadata").getWritePriority(), WritePriority.URGENT);
        assertEquals(writtenIndexMetadata.getNumberOfShards(), 1);
        assertEquals(writtenIndexMetadata.getNumberOfReplicas(), 0);
        assertEquals(writtenIndexMetadata.getIndex().getName(), "test-index");
        assertEquals(writtenIndexMetadata.getIndex().getUUID(), "index-uuid");
        long expectedChecksum = RemoteTransferContainer.checksumOfChecksum(new ByteArrayIndexInput("metadata-filename", writtenBytes), 8);
        if (capturedWriteContext.get("metadata").doRemoteDataIntegrityCheck()) {
            assertEquals(capturedWriteContext.get("metadata").getExpectedChecksum().longValue(), expectedChecksum);
        } else {
            assertEquals(capturedWriteContext.get("metadata").getExpectedChecksum(), null);
        }

    }

    public void testWriteFullMetadataFailureForGlobalMetadata() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        AsyncMultiStreamBlobContainer container = (AsyncMultiStreamBlobContainer) mockBlobStoreObjects(AsyncMultiStreamBlobContainer.class);

        ArgumentCaptor<ActionListener<Void>> actionListenerArgumentCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer((i) -> {
            // For async write action listener will be called from different thread, replicating same behaviour here.
            new Thread(new Runnable() {
                @Override
                public void run() {
                    actionListenerArgumentCaptor.getValue().onFailure(new RuntimeException("Cannot upload to remote"));
                }
            }).start();
            return null;
        }).when(container).asyncBlobUpload(any(WriteContext.class), actionListenerArgumentCaptor.capture());

        remoteClusterStateService.start();
        assertThrows(
            RemoteStateTransferException.class,
            () -> remoteClusterStateService.writeFullMetadata(clusterState, randomAlphaOfLength(10))
        );
    }

    public void testTimeoutWhileWritingManifestFile() throws IOException {
        // verify update metadata manifest upload timeout
        int metadataManifestUploadTimeout = 2;
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.metadata_manifest.upload_timeout", metadataManifestUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);

        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        AsyncMultiStreamBlobContainer container = (AsyncMultiStreamBlobContainer) mockBlobStoreObjects(AsyncMultiStreamBlobContainer.class);

        ArgumentCaptor<ActionListener<Void>> actionListenerArgumentCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer((i) -> {
            // For Manifest file perform No Op, so latch in code will timeout
            return null;
        }).when(container).asyncBlobUpload(any(WriteContext.class), actionListenerArgumentCaptor.capture());

        remoteClusterStateService.start();
        RemoteClusterStateService spiedService = spy(remoteClusterStateService);
        when(
            spiedService.writeMetadataInParallel(
                any(),
                anyList(),
                anyMap(),
                anyMap(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyMap(),
                anyBoolean(),
                anyList(),
                anyMap()
            )
        ).thenReturn(new RemoteClusterStateUtils.UploadedMetadataResults());
        RemoteStateTransferException ex = expectThrows(
            RemoteStateTransferException.class,
            () -> spiedService.writeFullMetadata(clusterState, randomAlphaOfLength(10))
        );
        assertTrue(ex.getMessage().contains("Timed out waiting for transfer of following metadata to complete"));
    }

    public void testWriteFullMetadataInParallelFailureForIndexMetadata() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        AsyncMultiStreamBlobContainer container = (AsyncMultiStreamBlobContainer) mockBlobStoreObjects(AsyncMultiStreamBlobContainer.class);

        ArgumentCaptor<ActionListener<Void>> actionListenerArgumentCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer((i) -> {
            actionListenerArgumentCaptor.getValue().onResponse(null);
            return null;
        }).doAnswer((i) -> {
            actionListenerArgumentCaptor.getValue().onFailure(new RuntimeException("Cannot upload to remote"));
            return null;
        }).when(container).asyncBlobUpload(any(WriteContext.class), actionListenerArgumentCaptor.capture());

        remoteClusterStateService.start();
        assertThrows(
            RemoteStateTransferException.class,
            () -> remoteClusterStateService.writeFullMetadata(clusterState, randomAlphaOfLength(10))
        );
        assertEquals(0, remoteClusterStateService.getStats().getSuccessCount());
    }

    public void testFailWriteIncrementalMetadataNonClusterManagerNode() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().build();
        remoteClusterStateService.start();
        final RemoteClusterStateManifestInfo manifestDetails = remoteClusterStateService.writeIncrementalMetadata(
            clusterState,
            clusterState,
            null
        );
        Assert.assertThat(manifestDetails, nullValue());
        assertEquals(0, remoteClusterStateService.getStats().getSuccessCount());
    }

    public void testFailWriteIncrementalMetadataWhenTermChanged() {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(2L).build();
        final ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .build();
        assertThrows(
            AssertionError.class,
            () -> remoteClusterStateService.writeIncrementalMetadata(previousClusterState, clusterState, null)
        );
    }

    public void testWriteIncrementalMetadataSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .build();

        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder().indices(Collections.emptyList()).build();

        remoteClusterStateService.start();
        final RemoteClusterStateService rcssSpy = Mockito.spy(remoteClusterStateService);
        final RemoteClusterStateManifestInfo manifestInfo = rcssSpy.writeIncrementalMetadata(
            previousClusterState,
            clusterState,
            previousManifest
        );
        final ClusterMetadataManifest manifest = manifestInfo.getClusterMetadataManifest();
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename__2");
        final List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        Mockito.verify(rcssSpy)
            .writeMetadataInParallel(
                eq(clusterState),
                eq(new ArrayList<IndexMetadata>(clusterState.metadata().indices().values())),
                eq(Collections.singletonMap(indices.get(0).getIndexName(), null)),
                eq(clusterState.metadata().customs()),
                eq(true),
                eq(true),
                eq(true),
                eq(false),
                eq(false),
                eq(false),
                eq(Collections.emptyMap()),
                eq(false),
                eq(Collections.emptyList()),
                eq(Collections.emptyMap())
            );

        assertThat(manifestInfo.getManifestFileName(), notNullValue());
        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
        assertThat(manifest.getHashesOfConsistentSettings(), nullValue());
        assertThat(manifest.getDiscoveryNodesMetadata(), nullValue());
        assertThat(manifest.getClusterBlocksMetadata(), nullValue());
        assertThat(manifest.getClusterStateCustomMap(), anEmptyMap());
        assertThat(manifest.getTransientSettingsMetadata(), nullValue());
        assertThat(manifest.getTemplatesMetadata(), notNullValue());
        assertThat(manifest.getCoordinationMetadata(), notNullValue());
        assertThat(manifest.getCustomMetadataMap().size(), is(2));
        assertThat(manifest.getIndicesRouting().size(), is(0));
    }

    public void testWriteIncrementalMetadataSuccessWhenPublicationEnabled() throws IOException {
        publicationEnabled = true;
        Settings nodeSettings = Settings.builder().put(REMOTE_PUBLICATION_EXPERIMENTAL, publicationEnabled).build();
        FeatureFlags.initializeFeatureFlags(nodeSettings);
        remoteClusterStateService = new RemoteClusterStateService(
            "test-node-id",
            repositoriesServiceSupplier,
            settings,
            clusterService,
            () -> 0L,
            threadPool,
            List.of(new RemoteIndexPathUploader(threadPool, settings, repositoriesServiceSupplier, clusterSettings)),
            writableRegistry()
        );
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .build();

        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder().indices(Collections.emptyList()).build();

        remoteClusterStateService.start();
        final RemoteClusterStateService rcssSpy = Mockito.spy(remoteClusterStateService);
        final RemoteClusterStateManifestInfo manifestInfo = rcssSpy.writeIncrementalMetadata(
            previousClusterState,
            clusterState,
            previousManifest
        );
        final ClusterMetadataManifest manifest = manifestInfo.getClusterMetadataManifest();
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename__2");
        final List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        Mockito.verify(rcssSpy)
            .writeMetadataInParallel(
                eq(clusterState),
                eq(new ArrayList<IndexMetadata>(clusterState.metadata().indices().values())),
                eq(Collections.singletonMap(indices.get(0).getIndexName(), null)),
                eq(clusterState.metadata().customs()),
                eq(true),
                eq(true),
                eq(true),
                eq(true),
                eq(false),
                eq(false),
                eq(Collections.emptyMap()),
                eq(true),
                anyList(),
                eq(Collections.emptyMap())
            );

        assertThat(manifestInfo.getManifestFileName(), notNullValue());
        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
        assertThat(manifest.getHashesOfConsistentSettings(), notNullValue());
        assertThat(manifest.getDiscoveryNodesMetadata(), notNullValue());
        assertThat(manifest.getClusterBlocksMetadata(), nullValue());
        assertThat(manifest.getClusterStateCustomMap(), anEmptyMap());
        assertThat(manifest.getTransientSettingsMetadata(), nullValue());
        assertThat(manifest.getTemplatesMetadata(), notNullValue());
        assertThat(manifest.getCoordinationMetadata(), notNullValue());
        assertThat(manifest.getCustomMetadataMap().size(), is(2));
        assertThat(manifest.getIndicesRouting().size(), is(1));
    }

    public void testTimeoutWhileWritingMetadata() throws IOException {
        AsyncMultiStreamBlobContainer container = (AsyncMultiStreamBlobContainer) mockBlobStoreObjects(AsyncMultiStreamBlobContainer.class);
        doNothing().when(container).asyncBlobUpload(any(), any());
        int writeTimeout = 2;
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.global_metadata.upload_timeout", writeTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        remoteClusterStateService.start();
        RemoteStateTransferException exception = assertThrows(
            RemoteStateTransferException.class,
            () -> remoteClusterStateService.writeMetadataInParallel(
                ClusterState.EMPTY_STATE,
                emptyList(),
                emptyMap(),
                emptyMap(),
                true,
                true,
                true,
                true,
                true,
                true,
                emptyMap(),
                true,
                emptyList(),
                null
            )
        );
        assertTrue(exception.getMessage().startsWith("Timed out waiting for transfer of following metadata to complete"));
    }

    public void testGetClusterStateForManifest_IncludeEphemeral() throws IOException {
        ClusterMetadataManifest manifest = generateClusterMetadataManifestWithAllAttributes().build();
        mockBlobStoreObjects();
        remoteClusterStateService.start();
        RemoteReadResult mockedResult = mock(RemoteReadResult.class);
        RemoteIndexMetadataManager mockedIndexManager = mock(RemoteIndexMetadataManager.class);
        RemoteGlobalMetadataManager mockedGlobalMetadataManager = mock(RemoteGlobalMetadataManager.class);
        RemoteClusterStateAttributesManager mockedClusterStateAttributeManager = mock(RemoteClusterStateAttributesManager.class);
        remoteClusterStateService.setRemoteIndexMetadataManager(mockedIndexManager);
        remoteClusterStateService.setRemoteGlobalMetadataManager(mockedGlobalMetadataManager);
        remoteClusterStateService.setRemoteClusterStateAttributesManager(mockedClusterStateAttributeManager);
        ArgumentCaptor<LatchedActionListener<RemoteReadResult>> listenerArgumentCaptor = ArgumentCaptor.forClass(
            LatchedActionListener.class
        );
        when(mockedIndexManager.getAsyncIndexMetadataReadAction(any(), anyString(), listenerArgumentCaptor.capture())).thenReturn(
            () -> listenerArgumentCaptor.getValue().onResponse(mockedResult)
        );
        when(mockedGlobalMetadataManager.getAsyncMetadataReadAction(any(), anyString(), listenerArgumentCaptor.capture())).thenReturn(
            () -> listenerArgumentCaptor.getValue().onResponse(mockedResult)
        );
        when(mockedClusterStateAttributeManager.getAsyncMetadataReadAction(anyString(), any(), listenerArgumentCaptor.capture()))
            .thenReturn(() -> listenerArgumentCaptor.getValue().onResponse(mockedResult));
        when(mockedResult.getComponent()).thenReturn(COORDINATION_METADATA);
        RemoteClusterStateService mockService = spy(remoteClusterStateService);
        mockService.getClusterStateForManifest(ClusterName.DEFAULT.value(), manifest, NODE_ID, true);
        verify(mockService, times(1)).readClusterStateInParallel(
            any(),
            eq(manifest),
            eq(manifest.getClusterUUID()),
            eq(NODE_ID),
            eq(manifest.getIndices()),
            eq(manifest.getCustomMetadataMap()),
            eq(true),
            eq(true),
            eq(true),
            eq(true),
            eq(true),
            eq(true),
            eq(manifest.getIndicesRouting()),
            eq(true),
            eq(manifest.getClusterStateCustomMap()),
            eq(false),
            eq(true)
        );
    }

    public void testGetClusterStateForManifest_ExcludeEphemeral() throws IOException {
        ClusterMetadataManifest manifest = generateClusterMetadataManifestWithAllAttributes().build();
        mockBlobStoreObjects();
        remoteClusterStateService.start();
        RemoteReadResult mockedResult = mock(RemoteReadResult.class);
        RemoteIndexMetadataManager mockedIndexManager = mock(RemoteIndexMetadataManager.class);
        RemoteGlobalMetadataManager mockedGlobalMetadataManager = mock(RemoteGlobalMetadataManager.class);
        RemoteClusterStateAttributesManager mockedClusterStateAttributeManager = mock(RemoteClusterStateAttributesManager.class);
        ArgumentCaptor<LatchedActionListener<RemoteReadResult>> listenerArgumentCaptor = ArgumentCaptor.forClass(
            LatchedActionListener.class
        );
        when(mockedIndexManager.getAsyncIndexMetadataReadAction(any(), anyString(), listenerArgumentCaptor.capture())).thenReturn(
            () -> listenerArgumentCaptor.getValue().onResponse(mockedResult)
        );
        when(mockedGlobalMetadataManager.getAsyncMetadataReadAction(any(), anyString(), listenerArgumentCaptor.capture())).thenReturn(
            () -> listenerArgumentCaptor.getValue().onResponse(mockedResult)
        );
        when(mockedClusterStateAttributeManager.getAsyncMetadataReadAction(anyString(), any(), listenerArgumentCaptor.capture()))
            .thenReturn(() -> listenerArgumentCaptor.getValue().onResponse(mockedResult));
        when(mockedResult.getComponent()).thenReturn(COORDINATION_METADATA);
        remoteClusterStateService.setRemoteIndexMetadataManager(mockedIndexManager);
        remoteClusterStateService.setRemoteGlobalMetadataManager(mockedGlobalMetadataManager);
        remoteClusterStateService.setRemoteClusterStateAttributesManager(mockedClusterStateAttributeManager);
        RemoteClusterStateService spiedService = spy(remoteClusterStateService);
        spiedService.getClusterStateForManifest(ClusterName.DEFAULT.value(), manifest, NODE_ID, false);
        verify(spiedService, times(1)).readClusterStateInParallel(
            any(),
            eq(manifest),
            eq(manifest.getClusterUUID()),
            eq(NODE_ID),
            eq(manifest.getIndices()),
            eq(manifest.getCustomMetadataMap()),
            eq(true),
            eq(true),
            eq(false),
            eq(true),
            eq(false),
            eq(false),
            eq(emptyList()),
            eq(false),
            eq(emptyMap()),
            eq(false),
            eq(false)

        );
    }

    public void testGetClusterStateFromManifest_CodecV1() throws IOException {
        ClusterMetadataManifest manifest = generateClusterMetadataManifestWithAllAttributes().codecVersion(CODEC_V1).build();
        mockBlobStoreObjects();
        remoteClusterStateService.start();
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        RemoteIndexMetadataManager mockedIndexManager = mock(RemoteIndexMetadataManager.class);
        RemoteGlobalMetadataManager mockedGlobalMetadataManager = mock(RemoteGlobalMetadataManager.class);
        remoteClusterStateService.setRemoteIndexMetadataManager(mockedIndexManager);
        remoteClusterStateService.setRemoteGlobalMetadataManager(mockedGlobalMetadataManager);
        ArgumentCaptor<LatchedActionListener<RemoteReadResult>> listenerArgumentCaptor = ArgumentCaptor.forClass(
            LatchedActionListener.class
        );
        when(mockedIndexManager.getAsyncIndexMetadataReadAction(any(), anyString(), listenerArgumentCaptor.capture())).thenReturn(
            () -> listenerArgumentCaptor.getValue().onResponse(new RemoteReadResult(indexMetadata, INDEX, INDEX))
        );
        when(mockedGlobalMetadataManager.getGlobalMetadata(anyString(), eq(manifest))).thenReturn(Metadata.EMPTY_METADATA);
        RemoteClusterStateService spiedService = spy(remoteClusterStateService);
        spiedService.getClusterStateForManifest(ClusterName.DEFAULT.value(), manifest, NODE_ID, true);
        verify(spiedService, times(1)).readClusterStateInParallel(
            any(),
            eq(manifest),
            eq(manifest.getClusterUUID()),
            eq(NODE_ID),
            eq(manifest.getIndices()),
            eq(emptyMap()),
            eq(false),
            eq(false),
            eq(false),
            eq(false),
            eq(false),
            eq(false),
            eq(emptyList()),
            eq(false),
            eq(emptyMap()),
            eq(false),
            eq(false)
        );
        verify(mockedGlobalMetadataManager, times(1)).getGlobalMetadata(eq(manifest.getClusterUUID()), eq(manifest));
    }

    public void testGetClusterStateUsingDiffFailWhenDiffManifestAbsent() {
        ClusterMetadataManifest manifest = ClusterMetadataManifest.builder().build();
        ClusterState previousState = ClusterState.EMPTY_STATE;
        AssertionError error = assertThrows(
            AssertionError.class,
            () -> remoteClusterStateService.getClusterStateUsingDiff(manifest, previousState, "test-node")
        );
        assertEquals("Diff manifest null which is required for downloading cluster state", error.getMessage());
    }

    public void testGetClusterStateUsingDiff_NoDiff() throws IOException {
        ClusterStateDiffManifest diffManifest = ClusterStateDiffManifest.builder().build();
        ClusterState clusterState = generateClusterStateWithAllAttributes().build();
        ClusterMetadataManifest manifest = ClusterMetadataManifest.builder()
            .diffManifest(diffManifest)
            .stateUUID(clusterState.stateUUID())
            .stateVersion(clusterState.version())
            .metadataVersion(clusterState.metadata().version())
            .clusterUUID(clusterState.getMetadata().clusterUUID())
            .routingTableVersion(clusterState.routingTable().version())
            .build();
        ClusterState updatedClusterState = remoteClusterStateService.getClusterStateUsingDiff(manifest, clusterState, "test-node");
        assertEquals(clusterState.getClusterName(), updatedClusterState.getClusterName());
        assertEquals(clusterState.metadata().clusterUUID(), updatedClusterState.metadata().clusterUUID());
        assertEquals(clusterState.metadata().version(), updatedClusterState.metadata().version());
        assertEquals(clusterState.metadata().coordinationMetadata(), updatedClusterState.metadata().coordinationMetadata());
        assertEquals(clusterState.metadata().getIndices(), updatedClusterState.metadata().getIndices());
        assertEquals(clusterState.metadata().templates(), updatedClusterState.metadata().templates());
        assertEquals(clusterState.metadata().persistentSettings(), updatedClusterState.metadata().persistentSettings());
        assertEquals(clusterState.metadata().transientSettings(), updatedClusterState.metadata().transientSettings());
        assertEquals(clusterState.metadata().getCustoms(), updatedClusterState.metadata().getCustoms());
        assertEquals(clusterState.metadata().hashesOfConsistentSettings(), updatedClusterState.metadata().hashesOfConsistentSettings());
        assertEquals(clusterState.getCustoms(), updatedClusterState.getCustoms());
        assertEquals(clusterState.stateUUID(), updatedClusterState.stateUUID());
        assertEquals(clusterState.version(), updatedClusterState.version());
        assertEquals(clusterState.getRoutingTable().version(), updatedClusterState.getRoutingTable().version());
        assertEquals(clusterState.getRoutingTable().getIndicesRouting(), updatedClusterState.getRoutingTable().getIndicesRouting());
        assertEquals(clusterState.getNodes(), updatedClusterState.getNodes());
        assertEquals(clusterState.getBlocks(), updatedClusterState.getBlocks());
    }

    public void testGetClusterStateUsingDiff() throws IOException {
        ClusterState clusterState = generateClusterStateWithAllAttributes().build();
        ClusterState.Builder expectedClusterStateBuilder = ClusterState.builder(clusterState);
        Metadata.Builder mb = Metadata.builder(clusterState.metadata());
        ClusterStateDiffManifest.Builder diffManifestBuilder = ClusterStateDiffManifest.builder();
        ClusterMetadataManifest.Builder manifestBuilder = ClusterMetadataManifest.builder();
        BlobContainer blobContainer = mockBlobStoreObjects();
        if (randomBoolean()) {
            // updated coordination metadata
            CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder()
                .term(clusterState.metadata().coordinationMetadata().term() + 1)
                .build();
            mb.coordinationMetadata(coordinationMetadata);
            diffManifestBuilder.coordinationMetadataUpdated(true);
            manifestBuilder.coordinationMetadata(new UploadedMetadataAttribute(COORDINATION_METADATA, COORDINATION_METADATA_FILENAME));
            when(blobContainer.readBlob(COORDINATION_METADATA_FILENAME)).thenAnswer(i -> {
                BytesReference bytes = COORDINATION_METADATA_FORMAT.serialize(
                    coordinationMetadata,
                    COORDINATION_METADATA_FILENAME,
                    compressor,
                    FORMAT_PARAMS
                );
                return new ByteArrayInputStream(bytes.streamInput().readAllBytes());
            });
        }
        if (randomBoolean()) {
            // updated templates
            TemplatesMetadata templatesMetadata = TemplatesMetadata.builder()
                .put(
                    IndexTemplateMetadata.builder("template" + randomAlphaOfLength(3))
                        .patterns(Arrays.asList("bar-*", "foo-*"))
                        .settings(Settings.builder().put("random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5)).build())
                        .build()
                )
                .build();
            mb.templates(templatesMetadata);
            diffManifestBuilder.templatesMetadataUpdated(true);
            manifestBuilder.templatesMetadata(new UploadedMetadataAttribute(TEMPLATES_METADATA, TEMPLATES_METADATA_FILENAME));
            when(blobContainer.readBlob(TEMPLATES_METADATA_FILENAME)).thenAnswer(i -> {
                BytesReference bytes = TEMPLATES_METADATA_FORMAT.serialize(
                    templatesMetadata,
                    TEMPLATES_METADATA_FILENAME,
                    compressor,
                    FORMAT_PARAMS
                );
                return new ByteArrayInputStream(bytes.streamInput().readAllBytes());
            });
        }
        if (randomBoolean()) {
            // updated persistent settings
            Settings persistentSettings = Settings.builder()
                .put("random_persistent_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5))
                .build();
            mb.persistentSettings(persistentSettings);
            diffManifestBuilder.settingsMetadataUpdated(true);
            manifestBuilder.settingMetadata(new UploadedMetadataAttribute(SETTING_METADATA, PERSISTENT_SETTINGS_FILENAME));
            when(blobContainer.readBlob(PERSISTENT_SETTINGS_FILENAME)).thenAnswer(i -> {
                BytesReference bytes = RemotePersistentSettingsMetadata.SETTINGS_METADATA_FORMAT.serialize(
                    persistentSettings,
                    PERSISTENT_SETTINGS_FILENAME,
                    compressor,
                    FORMAT_PARAMS
                );
                return new ByteArrayInputStream(bytes.streamInput().readAllBytes());
            });
        }
        if (randomBoolean()) {
            // updated transient settings
            Settings transientSettings = Settings.builder()
                .put("random_transient_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5))
                .build();
            mb.transientSettings(transientSettings);
            diffManifestBuilder.transientSettingsMetadataUpdate(true);
            manifestBuilder.transientSettingsMetadata(
                new UploadedMetadataAttribute(TRANSIENT_SETTING_METADATA, TRANSIENT_SETTINGS_FILENAME)
            );
            when(blobContainer.readBlob(TRANSIENT_SETTINGS_FILENAME)).thenAnswer(i -> {
                BytesReference bytes = RemoteTransientSettingsMetadata.SETTINGS_METADATA_FORMAT.serialize(
                    transientSettings,
                    TRANSIENT_SETTINGS_FILENAME,
                    compressor,
                    FORMAT_PARAMS
                );
                return new ByteArrayInputStream(bytes.streamInput().readAllBytes());
            });
        }
        if (randomBoolean()) {
            // updated customs
            CustomMetadata2 addedCustom = new CustomMetadata2(randomAlphaOfLength(10));
            mb.putCustom(addedCustom.getWriteableName(), addedCustom);
            diffManifestBuilder.customMetadataUpdated(Collections.singletonList(addedCustom.getWriteableName()));
            manifestBuilder.customMetadataMap(
                Map.of(addedCustom.getWriteableName(), new UploadedMetadataAttribute(addedCustom.getWriteableName(), "custom-md2-file__1"))
            );
            when(blobContainer.readBlob("custom-md2-file__1")).thenAnswer(i -> {
                ChecksumWritableBlobStoreFormat<Metadata.Custom> customMetadataFormat = new ChecksumWritableBlobStoreFormat<>(
                    "custom",
                    is -> readFrom(is, namedWriteableRegistry, addedCustom.getWriteableName())
                );
                BytesReference bytes = customMetadataFormat.serialize(addedCustom, "custom-md2-file__1", compressor);
                return new ByteArrayInputStream(bytes.streamInput().readAllBytes());
            });
        }
        if (randomBoolean()) {
            Set<String> customsToRemove = clusterState.metadata().customs().keySet();
            customsToRemove.forEach(mb::removeCustom);
            diffManifestBuilder.customMetadataDeleted(new ArrayList<>(customsToRemove));
        }
        if (randomBoolean()) {
            // updated hashes of consistent settings
            DiffableStringMap hashesOfConsistentSettings = new DiffableStringMap(Map.of("secure_setting_key", "secure_setting_value"));
            mb.hashesOfConsistentSettings(hashesOfConsistentSettings);
            diffManifestBuilder.hashesOfConsistentSettingsUpdated(true);
            manifestBuilder.hashesOfConsistentSettings(
                new UploadedMetadataAttribute(HASHES_OF_CONSISTENT_SETTINGS, HASHES_OF_CONSISTENT_SETTINGS_FILENAME)
            );
            when(blobContainer.readBlob(HASHES_OF_CONSISTENT_SETTINGS_FILENAME)).thenAnswer(i -> {
                BytesReference bytes = HASHES_OF_CONSISTENT_SETTINGS_FORMAT.serialize(
                    hashesOfConsistentSettings,
                    HASHES_OF_CONSISTENT_SETTINGS_FILENAME,
                    compressor
                );
                return new ByteArrayInputStream(bytes.streamInput().readAllBytes());
            });
        }
        if (randomBoolean()) {
            // updated index metadata
            IndexMetadata indexMetadata = new IndexMetadata.Builder("add-test-index").settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "add-test-index-uuid")
                    .build()
            ).numberOfShards(1).numberOfReplicas(0).build();
            mb.put(indexMetadata, true);
            diffManifestBuilder.indicesUpdated(Collections.singletonList(indexMetadata.getIndex().getName()));
            manifestBuilder.indices(
                List.of(
                    new UploadedIndexMetadata(indexMetadata.getIndex().getName(), indexMetadata.getIndexUUID(), "add-test-index-file__2")
                )
            );
            when(blobContainer.readBlob("add-test-index-file__2")).thenAnswer(i -> {
                BytesReference bytes = INDEX_METADATA_FORMAT.serialize(indexMetadata, "add-test-index-file__2", compressor, FORMAT_PARAMS);
                return new ByteArrayInputStream(bytes.streamInput().readAllBytes());
            });
        }
        if (randomBoolean()) {
            // remove index metadata
            Set<String> indicesToDelete = clusterState.metadata().getIndices().keySet();
            indicesToDelete.forEach(mb::remove);
            diffManifestBuilder.indicesDeleted(new ArrayList<>(indicesToDelete));
        }
        if (randomBoolean()) {
            // update nodes
            DiscoveryNode node = new DiscoveryNode("node_id", buildNewFakeTransportAddress(), Version.CURRENT);
            DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterState.nodes()).add(node);
            expectedClusterStateBuilder.nodes(nodesBuilder.build());
            diffManifestBuilder.discoveryNodesUpdated(true);
            manifestBuilder.discoveryNodesMetadata(new UploadedMetadataAttribute(DISCOVERY_NODES, DISCOVERY_NODES_FILENAME));
            when(blobContainer.readBlob(DISCOVERY_NODES_FILENAME)).thenAnswer(invocationOnMock -> {
                BytesReference bytes = DISCOVERY_NODES_FORMAT.serialize(nodesBuilder.build(), DISCOVERY_NODES_FILENAME, compressor);
                return new ByteArrayInputStream(bytes.streamInput().readAllBytes());
            });
        }
        if (randomBoolean()) {
            // update blocks
            ClusterBlocks newClusterBlock = randomClusterBlocks();
            expectedClusterStateBuilder.blocks(newClusterBlock);
            diffManifestBuilder.clusterBlocksUpdated(true);
            manifestBuilder.clusterBlocksMetadata(new UploadedMetadataAttribute(CLUSTER_BLOCKS, CLUSTER_BLOCKS_FILENAME));
            when(blobContainer.readBlob(CLUSTER_BLOCKS_FILENAME)).thenAnswer(invocationOnMock -> {
                BytesReference bytes = CLUSTER_BLOCKS_FORMAT.serialize(newClusterBlock, CLUSTER_BLOCKS_FILENAME, compressor);
                return new ByteArrayInputStream(bytes.streamInput().readAllBytes());
            });

        }
        ClusterState expectedClusterState = expectedClusterStateBuilder.metadata(mb).build();
        ClusterStateDiffManifest diffManifest = diffManifestBuilder.build();
        manifestBuilder.diffManifest(diffManifest)
            .stateUUID(clusterState.stateUUID())
            .stateVersion(clusterState.version())
            .metadataVersion(clusterState.metadata().version())
            .clusterUUID(clusterState.getMetadata().clusterUUID())
            .routingTableVersion(clusterState.getRoutingTable().version());

        remoteClusterStateService.start();
        ClusterState updatedClusterState = remoteClusterStateService.getClusterStateUsingDiff(
            manifestBuilder.build(),
            clusterState,
            NODE_ID
        );

        assertEquals(expectedClusterState.getClusterName(), updatedClusterState.getClusterName());
        assertEquals(expectedClusterState.stateUUID(), updatedClusterState.stateUUID());
        assertEquals(expectedClusterState.version(), updatedClusterState.version());
        assertEquals(expectedClusterState.metadata().clusterUUID(), updatedClusterState.metadata().clusterUUID());
        assertEquals(expectedClusterState.getRoutingTable().version(), updatedClusterState.getRoutingTable().version());
        assertNotEquals(diffManifest.isClusterBlocksUpdated(), updatedClusterState.getBlocks().equals(clusterState.getBlocks()));
        assertNotEquals(diffManifest.isDiscoveryNodesUpdated(), updatedClusterState.getNodes().equals(clusterState.getNodes()));
        assertNotEquals(
            diffManifest.isCoordinationMetadataUpdated(),
            updatedClusterState.getMetadata().coordinationMetadata().equals(clusterState.getMetadata().coordinationMetadata())
        );
        assertNotEquals(
            diffManifest.isTemplatesMetadataUpdated(),
            updatedClusterState.getMetadata().templates().equals(clusterState.getMetadata().getTemplates())
        );
        assertNotEquals(
            diffManifest.isSettingsMetadataUpdated(),
            updatedClusterState.getMetadata().persistentSettings().equals(clusterState.getMetadata().persistentSettings())
        );
        assertNotEquals(
            diffManifest.isTransientSettingsMetadataUpdated(),
            updatedClusterState.getMetadata().transientSettings().equals(clusterState.getMetadata().transientSettings())
        );
        diffManifest.getIndicesUpdated().forEach(indexName -> {
            IndexMetadata updatedIndexMetadata = updatedClusterState.metadata().index(indexName);
            IndexMetadata originalIndexMetadata = clusterState.metadata().index(indexName);
            assertNotEquals(originalIndexMetadata, updatedIndexMetadata);
        });
        diffManifest.getCustomMetadataUpdated().forEach(customMetadataName -> {
            Metadata.Custom updatedCustomMetadata = updatedClusterState.metadata().custom(customMetadataName);
            Metadata.Custom originalCustomMetadata = clusterState.metadata().custom(customMetadataName);
            assertNotEquals(originalCustomMetadata, updatedCustomMetadata);
        });
        diffManifest.getClusterStateCustomUpdated().forEach(clusterStateCustomName -> {
            ClusterState.Custom updateClusterStateCustom = updatedClusterState.customs().get(clusterStateCustomName);
            ClusterState.Custom originalClusterStateCustom = clusterState.customs().get(clusterStateCustomName);
            assertNotEquals(originalClusterStateCustom, updateClusterStateCustom);
        });
        diffManifest.getIndicesRoutingUpdated().forEach(indexName -> {
            IndexRoutingTable updatedIndexRoutingTable = updatedClusterState.getRoutingTable().getIndicesRouting().get(indexName);
            IndexRoutingTable originalIndexingRoutingTable = clusterState.getRoutingTable().getIndicesRouting().get(indexName);
            assertNotEquals(originalIndexingRoutingTable, updatedIndexRoutingTable);
        });
        diffManifest.getIndicesDeleted()
            .forEach(indexName -> { assertFalse(updatedClusterState.metadata().getIndices().containsKey(indexName)); });
        diffManifest.getCustomMetadataDeleted().forEach(customMetadataName -> {
            assertFalse(updatedClusterState.metadata().customs().containsKey(customMetadataName));
        });
        diffManifest.getClusterStateCustomDeleted().forEach(clusterStateCustomName -> {
            assertFalse(updatedClusterState.customs().containsKey(clusterStateCustomName));
        });
        diffManifest.getIndicesRoutingDeleted().forEach(indexName -> {
            assertFalse(updatedClusterState.getRoutingTable().getIndicesRouting().containsKey(indexName));
        });
    }

    public void testReadClusterStateInParallel_TimedOut() throws IOException {
        ClusterState previousClusterState = generateClusterStateWithAllAttributes().build();
        ClusterMetadataManifest manifest = generateClusterMetadataManifestWithAllAttributes().build();
        BlobContainer container = mockBlobStoreObjects();
        int readTimeOut = 2;
        Settings newSettings = Settings.builder().put("cluster.remote_store.state.read_timeout", readTimeOut + "s").build();
        clusterSettings.applySettings(newSettings);
        when(container.readBlob(anyString())).thenAnswer(invocationOnMock -> {
            Thread.sleep(readTimeOut * 1000 + 100);
            return null;
        });
        remoteClusterStateService.start();
        RemoteStateTransferException exception = expectThrows(
            RemoteStateTransferException.class,
            () -> remoteClusterStateService.readClusterStateInParallel(
                previousClusterState,
                manifest,
                manifest.getClusterUUID(),
                NODE_ID,
                emptyList(),
                emptyMap(),
                true,
                true,
                true,
                true,
                true,
                true,
                emptyList(),
                true,
                emptyMap(),
                false,
                true
            )
        );
        assertEquals("Timed out waiting to read cluster state from remote within timeout " + readTimeOut + "s", exception.getMessage());
    }

    public void testReadClusterStateInParallel_ExceptionDuringRead() throws IOException {
        ClusterState previousClusterState = generateClusterStateWithAllAttributes().build();
        ClusterMetadataManifest manifest = generateClusterMetadataManifestWithAllAttributes().build();
        BlobContainer container = mockBlobStoreObjects();
        Exception mockException = new IOException("mock exception");
        when(container.readBlob(anyString())).thenThrow(mockException);
        remoteClusterStateService.start();
        RemoteStateTransferException exception = expectThrows(
            RemoteStateTransferException.class,
            () -> remoteClusterStateService.readClusterStateInParallel(
                previousClusterState,
                manifest,
                manifest.getClusterUUID(),
                NODE_ID,
                emptyList(),
                emptyMap(),
                true,
                true,
                true,
                true,
                true,
                true,
                emptyList(),
                true,
                emptyMap(),
                false,
                true
            )
        );
        assertEquals("Exception during reading cluster state from remote", exception.getMessage());
        assertTrue(exception.getSuppressed().length > 0);
        assertEquals(mockException, exception.getSuppressed()[0]);
    }

    public void testReadClusterStateInParallel_UnexpectedResult() throws IOException {
        ClusterState previousClusterState = generateClusterStateWithAllAttributes().build();
        // index already present in previous state
        List<UploadedIndexMetadata> uploadedIndexMetadataList = new ArrayList<>(
            List.of(new UploadedIndexMetadata("test-index", "test-index-uuid", "test-index-file__2"))
        );
        // new index to be added
        List<UploadedIndexMetadata> newIndicesToRead = List.of(
            new UploadedIndexMetadata("test-index-1", "test-index-1-uuid", "test-index-1-file__2")
        );
        uploadedIndexMetadataList.addAll(newIndicesToRead);
        // existing custom metadata
        Map<String, UploadedMetadataAttribute> uploadedCustomMetadataMap = new HashMap<>(
            Map.of(
                "custom_md_1",
                new UploadedMetadataAttribute("custom_md_1", "test-custom1-file__1"),
                "custom_md_2",
                new UploadedMetadataAttribute("custom_md_2", "test-custom2-file__1")
            )
        );
        // new custom metadata to be added
        Map<String, UploadedMetadataAttribute> newCustomMetadataMap = Map.of(
            "custom_md_3",
            new UploadedMetadataAttribute("custom_md_3", "test-custom3-file__1")
        );
        uploadedCustomMetadataMap.putAll(newCustomMetadataMap);
        // already existing cluster state customs
        Map<String, UploadedMetadataAttribute> uploadedClusterStateCustomMap = new HashMap<>(
            Map.of(
                "custom_1",
                new UploadedMetadataAttribute("custom_1", "test-cluster-state-custom1-file__1"),
                "custom_2",
                new UploadedMetadataAttribute("custom_2", "test-cluster-state-custom2-file__1")
            )
        );
        // new customs uploaded
        Map<String, UploadedMetadataAttribute> newClusterStateCustoms = Map.of(
            "custom_3",
            new UploadedMetadataAttribute("custom_3", "test-cluster-state-custom3-file__1")
        );
        uploadedClusterStateCustomMap.putAll(newClusterStateCustoms);
        ClusterMetadataManifest manifest = ClusterMetadataManifest.builder()
            .clusterUUID(previousClusterState.getMetadata().clusterUUID())
            .indices(uploadedIndexMetadataList)
            .coordinationMetadata(new UploadedMetadataAttribute(COORDINATION_METADATA, COORDINATION_METADATA_FILENAME))
            .settingMetadata(new UploadedMetadataAttribute(SETTING_METADATA, PERSISTENT_SETTINGS_FILENAME))
            .transientSettingsMetadata(new UploadedMetadataAttribute(TRANSIENT_SETTING_METADATA, TRANSIENT_SETTINGS_FILENAME))
            .templatesMetadata(new UploadedMetadataAttribute(TEMPLATES_METADATA, TEMPLATES_METADATA_FILENAME))
            .hashesOfConsistentSettings(
                new UploadedMetadataAttribute(HASHES_OF_CONSISTENT_SETTINGS, HASHES_OF_CONSISTENT_SETTINGS_FILENAME)
            )
            .customMetadataMap(uploadedCustomMetadataMap)
            .discoveryNodesMetadata(new UploadedMetadataAttribute(DISCOVERY_NODES, DISCOVERY_NODES_FILENAME))
            .clusterBlocksMetadata(new UploadedMetadataAttribute(CLUSTER_BLOCKS, CLUSTER_BLOCKS_FILENAME))
            .clusterStateCustomMetadataMap(uploadedClusterStateCustomMap)
            .build();

        RemoteReadResult mockResult = mock(RemoteReadResult.class);
        RemoteIndexMetadataManager mockIndexMetadataManager = mock(RemoteIndexMetadataManager.class);
        CheckedRunnable<IOException> mockRunnable = mock(CheckedRunnable.class);
        ArgumentCaptor<LatchedActionListener<RemoteReadResult>> latchCapture = ArgumentCaptor.forClass(LatchedActionListener.class);
        when(mockIndexMetadataManager.getAsyncIndexMetadataReadAction(anyString(), anyString(), latchCapture.capture())).thenReturn(
            mockRunnable
        );
        RemoteGlobalMetadataManager mockGlobalMetadataManager = mock(RemoteGlobalMetadataManager.class);
        when(mockGlobalMetadataManager.getAsyncMetadataReadAction(any(), anyString(), latchCapture.capture())).thenReturn(mockRunnable);
        RemoteClusterStateAttributesManager mockClusterStateAttributeManager = mock(RemoteClusterStateAttributesManager.class);
        when(mockClusterStateAttributeManager.getAsyncMetadataReadAction(anyString(), any(), latchCapture.capture())).thenReturn(
            mockRunnable
        );
        doAnswer(invocationOnMock -> {
            latchCapture.getValue().onResponse(mockResult);
            return null;
        }).when(mockRunnable).run();
        when(mockResult.getComponent()).thenReturn("mock-result");
        remoteClusterStateService.start();
        remoteClusterStateService.setRemoteIndexMetadataManager(mockIndexMetadataManager);
        remoteClusterStateService.setRemoteGlobalMetadataManager(mockGlobalMetadataManager);
        remoteClusterStateService.setRemoteClusterStateAttributesManager(mockClusterStateAttributeManager);
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> remoteClusterStateService.readClusterStateInParallel(
                previousClusterState,
                manifest,
                manifest.getClusterUUID(),
                NODE_ID,
                newIndicesToRead,
                newCustomMetadataMap,
                true,
                true,
                true,
                true,
                true,
                true,
                emptyList(),
                true,
                newClusterStateCustoms,
                false,
                true
            )
        );
        assertEquals("Unknown component: mock-result", exception.getMessage());
        newIndicesToRead.forEach(
            uploadedIndexMetadata -> verify(mockIndexMetadataManager, times(1)).getAsyncIndexMetadataReadAction(
                eq(previousClusterState.getMetadata().clusterUUID()),
                eq(uploadedIndexMetadata.getUploadedFilename()),
                any()
            )
        );
        verify(mockGlobalMetadataManager, times(1)).getAsyncMetadataReadAction(
            argThat(new BlobNameMatcher(COORDINATION_METADATA_FILENAME)),
            eq(COORDINATION_METADATA),
            any()
        );
        verify(mockGlobalMetadataManager, times(1)).getAsyncMetadataReadAction(
            argThat(new BlobNameMatcher(PERSISTENT_SETTINGS_FILENAME)),
            eq(SETTING_METADATA),
            any()
        );
        verify(mockGlobalMetadataManager, times(1)).getAsyncMetadataReadAction(
            argThat(new BlobNameMatcher(TRANSIENT_SETTINGS_FILENAME)),
            eq(TRANSIENT_SETTING_METADATA),
            any()
        );
        verify(mockGlobalMetadataManager, times(1)).getAsyncMetadataReadAction(
            argThat(new BlobNameMatcher(TEMPLATES_METADATA_FILENAME)),
            eq(TEMPLATES_METADATA),
            any()
        );
        verify(mockGlobalMetadataManager, times(1)).getAsyncMetadataReadAction(
            argThat(new BlobNameMatcher(HASHES_OF_CONSISTENT_SETTINGS_FILENAME)),
            eq(HASHES_OF_CONSISTENT_SETTINGS),
            any()
        );
        newCustomMetadataMap.keySet().forEach(uploadedCustomMetadataKey -> {
            verify(mockGlobalMetadataManager, times(1)).getAsyncMetadataReadAction(
                argThat(new BlobNameMatcher(newCustomMetadataMap.get(uploadedCustomMetadataKey).getUploadedFilename())),
                eq(uploadedCustomMetadataKey),
                any()
            );
        });
        verify(mockClusterStateAttributeManager, times(1)).getAsyncMetadataReadAction(
            eq(DISCOVERY_NODES),
            argThat(new BlobNameMatcher(DISCOVERY_NODES_FILENAME)),
            any()
        );
        verify(mockClusterStateAttributeManager, times(1)).getAsyncMetadataReadAction(
            eq(CLUSTER_BLOCKS),
            argThat(new BlobNameMatcher(CLUSTER_BLOCKS_FILENAME)),
            any()
        );
        newClusterStateCustoms.keySet().forEach(uploadedClusterStateCustomMetadataKey -> {
            verify(mockClusterStateAttributeManager, times(1)).getAsyncMetadataReadAction(
                eq(String.join(CUSTOM_DELIMITER, CLUSTER_STATE_CUSTOM, uploadedClusterStateCustomMetadataKey)),
                argThat(new BlobNameMatcher(newClusterStateCustoms.get(uploadedClusterStateCustomMetadataKey).getUploadedFilename())),
                any()
            );
        });
    }

    public void testReadClusterStateInParallel_Success() throws IOException {
        ClusterState previousClusterState = generateClusterStateWithAllAttributes().build();
        String indexFilename = "test-index-1-file__2";
        String customMetadataFilename = "test-custom3-file__1";
        String clusterStateCustomFilename = "test-cluster-state-custom3-file__1";
        // index already present in previous state
        List<UploadedIndexMetadata> uploadedIndexMetadataList = new ArrayList<>(
            List.of(new UploadedIndexMetadata("test-index", "test-index-uuid", "test-index-file__2"))
        );
        // new index to be added
        List<UploadedIndexMetadata> newIndicesToRead = List.of(
            new UploadedIndexMetadata("test-index-1", "test-index-1-uuid", indexFilename)
        );
        uploadedIndexMetadataList.addAll(newIndicesToRead);
        // existing custom metadata
        Map<String, UploadedMetadataAttribute> uploadedCustomMetadataMap = new HashMap<>(
            Map.of(
                "custom_md_1",
                new UploadedMetadataAttribute("custom_md_1", "test-custom1-file__1"),
                "custom_md_2",
                new UploadedMetadataAttribute("custom_md_2", "test-custom2-file__1")
            )
        );
        // new custom metadata to be added
        Map<String, UploadedMetadataAttribute> newCustomMetadataMap = Map.of(
            "custom_md_3",
            new UploadedMetadataAttribute("custom_md_3", customMetadataFilename)
        );
        uploadedCustomMetadataMap.putAll(newCustomMetadataMap);
        // already existing cluster state customs
        Map<String, UploadedMetadataAttribute> uploadedClusterStateCustomMap = new HashMap<>(
            Map.of(
                "custom_1",
                new UploadedMetadataAttribute("custom_1", "test-cluster-state-custom1-file__1"),
                "custom_2",
                new UploadedMetadataAttribute("custom_2", "test-cluster-state-custom2-file__1")
            )
        );
        // new customs uploaded
        Map<String, UploadedMetadataAttribute> newClusterStateCustoms = Map.of(
            "custom_3",
            new UploadedMetadataAttribute("custom_3", clusterStateCustomFilename)
        );
        uploadedClusterStateCustomMap.putAll(newClusterStateCustoms);

        ClusterMetadataManifest manifest = generateClusterMetadataManifestWithAllAttributes().indices(uploadedIndexMetadataList)
            .customMetadataMap(uploadedCustomMetadataMap)
            .clusterStateCustomMetadataMap(uploadedClusterStateCustomMap)
            .build();

        IndexMetadata newIndexMetadata = new IndexMetadata.Builder("test-index-1").state(IndexMetadata.State.OPEN)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        CustomMetadata3 customMetadata3 = new CustomMetadata3("custom_md_3");
        CoordinationMetadata updatedCoordinationMetadata = CoordinationMetadata.builder()
            .term(previousClusterState.metadata().coordinationMetadata().term() + 1)
            .build();
        Settings updatedPersistentSettings = Settings.builder()
            .put("random_persistent_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5))
            .build();
        Settings updatedTransientSettings = Settings.builder()
            .put("random_transient_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5))
            .build();
        TemplatesMetadata updatedTemplateMetadata = getTemplatesMetadata();
        DiffableStringMap updatedHashesOfConsistentSettings = getHashesOfConsistentSettings();
        DiscoveryNodes updatedDiscoveryNodes = getDiscoveryNodes();
        ClusterBlocks updatedClusterBlocks = randomClusterBlocks();
        TestClusterStateCustom3 updatedClusterStateCustom3 = new TestClusterStateCustom3("custom_3");

        RemoteIndexMetadataManager mockedIndexManager = mock(RemoteIndexMetadataManager.class);
        RemoteGlobalMetadataManager mockedGlobalMetadataManager = mock(RemoteGlobalMetadataManager.class);
        RemoteClusterStateAttributesManager mockedClusterStateAttributeManager = mock(RemoteClusterStateAttributesManager.class);

        when(
            mockedIndexManager.getAsyncIndexMetadataReadAction(
                eq(manifest.getClusterUUID()),
                eq(indexFilename),
                any(LatchedActionListener.class)
            )
        ).thenAnswer(invocationOnMock -> {
            LatchedActionListener<RemoteReadResult> latchedActionListener = invocationOnMock.getArgument(2, LatchedActionListener.class);
            return (CheckedRunnable<IOException>) () -> latchedActionListener.onResponse(
                new RemoteReadResult(newIndexMetadata, INDEX, "test-index-1")
            );
        });
        when(
            mockedGlobalMetadataManager.getAsyncMetadataReadAction(
                argThat(new BlobNameMatcher(customMetadataFilename)),
                eq("custom_md_3"),
                any()
            )
        ).thenAnswer(invocationOnMock -> {
            LatchedActionListener<RemoteReadResult> latchedActionListener = invocationOnMock.getArgument(2, LatchedActionListener.class);
            return (CheckedRunnable<IOException>) () -> latchedActionListener.onResponse(
                new RemoteReadResult(customMetadata3, CUSTOM_METADATA, "custom_md_3")
            );
        });
        when(
            mockedGlobalMetadataManager.getAsyncMetadataReadAction(
                argThat(new BlobNameMatcher(COORDINATION_METADATA_FILENAME)),
                eq(COORDINATION_METADATA),
                any()
            )
        ).thenAnswer(invocationOnMock -> {
            LatchedActionListener<RemoteReadResult> latchedActionListener = invocationOnMock.getArgument(2, LatchedActionListener.class);
            return (CheckedRunnable<IOException>) () -> latchedActionListener.onResponse(
                new RemoteReadResult(updatedCoordinationMetadata, COORDINATION_METADATA, COORDINATION_METADATA)
            );
        });
        when(
            mockedGlobalMetadataManager.getAsyncMetadataReadAction(
                argThat(new BlobNameMatcher(PERSISTENT_SETTINGS_FILENAME)),
                eq(SETTING_METADATA),
                any()
            )
        ).thenAnswer(invocationOnMock -> {
            LatchedActionListener<RemoteReadResult> latchedActionListener = invocationOnMock.getArgument(2, LatchedActionListener.class);

            return (CheckedRunnable<IOException>) () -> latchedActionListener.onResponse(
                new RemoteReadResult(updatedPersistentSettings, SETTING_METADATA, SETTING_METADATA)
            );
        });
        when(
            mockedGlobalMetadataManager.getAsyncMetadataReadAction(
                argThat(new BlobNameMatcher(TRANSIENT_SETTINGS_FILENAME)),
                eq(TRANSIENT_SETTING_METADATA),
                any()
            )
        ).thenAnswer(invocationOnMock -> {
            LatchedActionListener<RemoteReadResult> latchedActionListener = invocationOnMock.getArgument(2, LatchedActionListener.class);
            return (CheckedRunnable<IOException>) () -> latchedActionListener.onResponse(
                new RemoteReadResult(updatedTransientSettings, TRANSIENT_SETTING_METADATA, TRANSIENT_SETTING_METADATA)
            );
        });
        when(
            mockedGlobalMetadataManager.getAsyncMetadataReadAction(
                argThat(new BlobNameMatcher(TEMPLATES_METADATA_FILENAME)),
                eq(TEMPLATES_METADATA),
                any()
            )
        ).thenAnswer(invocationOnMock -> {
            LatchedActionListener<RemoteReadResult> latchedActionListener = invocationOnMock.getArgument(2, LatchedActionListener.class);
            return (CheckedRunnable<IOException>) () -> latchedActionListener.onResponse(
                new RemoteReadResult(updatedTemplateMetadata, TEMPLATES_METADATA, TEMPLATES_METADATA)
            );
        });
        when(
            mockedGlobalMetadataManager.getAsyncMetadataReadAction(
                argThat(new BlobNameMatcher(HASHES_OF_CONSISTENT_SETTINGS_FILENAME)),
                eq(HASHES_OF_CONSISTENT_SETTINGS),
                any()
            )
        ).thenAnswer(invocationOnMock -> {
            LatchedActionListener<RemoteReadResult> latchedActionListener = invocationOnMock.getArgument(2, LatchedActionListener.class);
            return (CheckedRunnable<IOException>) () -> latchedActionListener.onResponse(
                new RemoteReadResult(updatedHashesOfConsistentSettings, HASHES_OF_CONSISTENT_SETTINGS, HASHES_OF_CONSISTENT_SETTINGS)
            );
        });
        when(
            mockedClusterStateAttributeManager.getAsyncMetadataReadAction(
                eq(DISCOVERY_NODES),
                argThat(new BlobNameMatcher(DISCOVERY_NODES_FILENAME)),
                any()
            )
        ).thenAnswer(invocationOnMock -> {
            LatchedActionListener<RemoteReadResult> latchedActionListener = invocationOnMock.getArgument(2, LatchedActionListener.class);
            return (CheckedRunnable<IOException>) () -> latchedActionListener.onResponse(
                new RemoteReadResult(updatedDiscoveryNodes, CLUSTER_STATE_ATTRIBUTE, DISCOVERY_NODES)
            );
        });
        when(
            mockedClusterStateAttributeManager.getAsyncMetadataReadAction(
                eq(CLUSTER_BLOCKS),
                argThat(new BlobNameMatcher(CLUSTER_BLOCKS_FILENAME)),
                any()
            )
        ).thenAnswer(invocationOnMock -> {
            LatchedActionListener<RemoteReadResult> latchedActionListener = invocationOnMock.getArgument(2, LatchedActionListener.class);
            return (CheckedRunnable<IOException>) () -> latchedActionListener.onResponse(
                new RemoteReadResult(updatedClusterBlocks, CLUSTER_STATE_ATTRIBUTE, CLUSTER_BLOCKS)
            );
        });
        when(
            mockedClusterStateAttributeManager.getAsyncMetadataReadAction(
                eq(String.join(CUSTOM_DELIMITER, CLUSTER_STATE_CUSTOM, updatedClusterStateCustom3.getWriteableName())),
                argThat(new BlobNameMatcher(clusterStateCustomFilename)),
                any()
            )
        ).thenAnswer(invocationOnMock -> {
            LatchedActionListener<RemoteReadResult> latchedActionListener = invocationOnMock.getArgument(2, LatchedActionListener.class);
            return (CheckedRunnable<IOException>) () -> latchedActionListener.onResponse(
                new RemoteReadResult(
                    updatedClusterStateCustom3,
                    CLUSTER_STATE_ATTRIBUTE,
                    String.join(CUSTOM_DELIMITER, CLUSTER_STATE_CUSTOM, updatedClusterStateCustom3.getWriteableName())
                )
            );
        });

        remoteClusterStateService.start();
        remoteClusterStateService.setRemoteIndexMetadataManager(mockedIndexManager);
        remoteClusterStateService.setRemoteGlobalMetadataManager(mockedGlobalMetadataManager);
        remoteClusterStateService.setRemoteClusterStateAttributesManager(mockedClusterStateAttributeManager);

        ClusterState updatedClusterState = remoteClusterStateService.readClusterStateInParallel(
            previousClusterState,
            manifest,
            manifest.getClusterUUID(),
            NODE_ID,
            newIndicesToRead,
            newCustomMetadataMap,
            true,
            true,
            true,
            true,
            true,
            true,
            emptyList(),
            true,
            newClusterStateCustoms,
            false,
            true
        );

        assertEquals(uploadedIndexMetadataList.size(), updatedClusterState.metadata().indices().size());
        assertTrue(updatedClusterState.metadata().indices().containsKey("test-index-1"));
        assertEquals(newIndexMetadata, updatedClusterState.metadata().index(newIndexMetadata.getIndex()));
        uploadedCustomMetadataMap.keySet().forEach(key -> assertTrue(updatedClusterState.metadata().customs().containsKey(key)));
        assertEquals(customMetadata3, updatedClusterState.metadata().custom(customMetadata3.getWriteableName()));
        assertEquals(
            previousClusterState.metadata().coordinationMetadata().term() + 1,
            updatedClusterState.metadata().coordinationMetadata().term()
        );
        assertEquals(updatedPersistentSettings, updatedClusterState.metadata().persistentSettings());
        assertEquals(updatedTransientSettings, updatedClusterState.metadata().transientSettings());
        assertEquals(updatedTemplateMetadata.getTemplates(), updatedClusterState.metadata().templates());
        assertEquals(updatedHashesOfConsistentSettings, updatedClusterState.metadata().hashesOfConsistentSettings());
        assertEquals(updatedDiscoveryNodes.getSize(), updatedClusterState.getNodes().getSize());
        updatedDiscoveryNodes.getNodes().forEach((nodeId, node) -> assertEquals(updatedClusterState.getNodes().get(nodeId), node));
        assertEquals(updatedDiscoveryNodes.getClusterManagerNodeId(), updatedClusterState.getNodes().getClusterManagerNodeId());
        assertEquals(updatedClusterBlocks, updatedClusterState.blocks());
        uploadedClusterStateCustomMap.keySet().forEach(key -> assertTrue(updatedClusterState.customs().containsKey(key)));
        assertEquals(updatedClusterStateCustom3, updatedClusterState.custom("custom_3"));
        newIndicesToRead.forEach(
            uploadedIndexMetadata -> verify(mockedIndexManager, times(1)).getAsyncIndexMetadataReadAction(
                eq(previousClusterState.getMetadata().clusterUUID()),
                eq(uploadedIndexMetadata.getUploadedFilename()),
                any()
            )
        );
        verify(mockedGlobalMetadataManager, times(1)).getAsyncMetadataReadAction(
            argThat(new BlobNameMatcher(COORDINATION_METADATA_FILENAME)),
            eq(COORDINATION_METADATA),
            any()
        );
        verify(mockedGlobalMetadataManager, times(1)).getAsyncMetadataReadAction(
            argThat(new BlobNameMatcher(PERSISTENT_SETTINGS_FILENAME)),
            eq(SETTING_METADATA),
            any()
        );
        verify(mockedGlobalMetadataManager, times(1)).getAsyncMetadataReadAction(
            argThat(new BlobNameMatcher(TRANSIENT_SETTINGS_FILENAME)),
            eq(TRANSIENT_SETTING_METADATA),
            any()
        );
        verify(mockedGlobalMetadataManager, times(1)).getAsyncMetadataReadAction(
            argThat(new BlobNameMatcher(TEMPLATES_METADATA_FILENAME)),
            eq(TEMPLATES_METADATA),
            any()
        );
        verify(mockedGlobalMetadataManager, times(1)).getAsyncMetadataReadAction(
            argThat(new BlobNameMatcher(HASHES_OF_CONSISTENT_SETTINGS_FILENAME)),
            eq(HASHES_OF_CONSISTENT_SETTINGS),
            any()
        );
        newCustomMetadataMap.keySet().forEach(uploadedCustomMetadataKey -> {
            verify(mockedGlobalMetadataManager, times(1)).getAsyncMetadataReadAction(
                argThat(new BlobNameMatcher(newCustomMetadataMap.get(uploadedCustomMetadataKey).getUploadedFilename())),
                eq(uploadedCustomMetadataKey),
                any()
            );
        });
        verify(mockedClusterStateAttributeManager, times(1)).getAsyncMetadataReadAction(
            eq(DISCOVERY_NODES),
            argThat(new BlobNameMatcher(DISCOVERY_NODES_FILENAME)),
            any()
        );
        verify(mockedClusterStateAttributeManager, times(1)).getAsyncMetadataReadAction(
            eq(CLUSTER_BLOCKS),
            argThat(new BlobNameMatcher(CLUSTER_BLOCKS_FILENAME)),
            any()
        );
        newClusterStateCustoms.keySet().forEach(uploadedClusterStateCustomMetadataKey -> {
            verify(mockedClusterStateAttributeManager, times(1)).getAsyncMetadataReadAction(
                eq(String.join(CUSTOM_DELIMITER, CLUSTER_STATE_CUSTOM, uploadedClusterStateCustomMetadataKey)),
                argThat(new BlobNameMatcher(newClusterStateCustoms.get(uploadedClusterStateCustomMetadataKey).getUploadedFilename())),
                any()
            );
        });
    }

    /*
     * Here we will verify the migration of manifest file from codec V0.
     *
     * Initially codec version is 0 and global metadata is also null, we will perform index metadata update.
     * In final manifest codec version should be 2 and have metadata files updated,
     * even if it was not changed in this cluster state update
     */
    public void testMigrationFromCodecV0ManifestToCodecV2Manifest() throws IOException {
        verifyCodecMigrationManifest(ClusterMetadataManifest.CODEC_V0);
    }

    /*
     * Here we will verify the migration of manifest file from codec V1.
     *
     * Initially codec version is 1 and a global metadata file is there, we will perform index metadata update.
     * In final manifest codec version should be 2 and have metadata files updated,
     * even if it was not changed in this cluster state update
     */
    public void testMigrationFromCodecV1ManifestToCodecV2Manifest() throws IOException {
        verifyCodecMigrationManifest(CODEC_V1);
    }

    private void verifyCodecMigrationManifest(int previousCodec) throws IOException {
        mockBlobStoreObjects();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .nodes(nodesWithLocalNodeClusterManager())
            .build();

        // Update only index metadata
        final IndexMetadata indexMetadata = new IndexMetadata.Builder("test").settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(1).numberOfReplicas(0).build();
        Metadata newMetadata = Metadata.builder(previousClusterState.metadata()).put(indexMetadata, true).build();
        ClusterState newClusterState = ClusterState.builder(previousClusterState).metadata(newMetadata).build();

        // previous manifest with codec 0 and null global metadata
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder()
            .codecVersion(previousCodec)
            .globalMetadataFileName(null)
            .indices(Collections.emptyList())
            .build();

        remoteClusterStateService.start();
        final ClusterMetadataManifest manifestAfterUpdate = remoteClusterStateService.writeIncrementalMetadata(
            previousClusterState,
            newClusterState,
            previousManifest
        ).getClusterMetadataManifest();

        // global metadata is updated
        assertThat(manifestAfterUpdate.hasMetadataAttributesFiles(), is(true));
        // Manifest file with codec version with 1 is updated.
        assertThat(manifestAfterUpdate.getCodecVersion(), is(MANIFEST_CURRENT_CODEC_VERSION));
    }

    public void testWriteIncrementalGlobalMetadataFromCodecV0Success() throws IOException {
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder().indices(Collections.emptyList()).build();

        verifyWriteIncrementalGlobalMetadataFromOlderCodecSuccess(previousManifest);
    }

    public void testWriteIncrementalGlobalMetadataFromCodecV1Success() throws IOException {
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder()
            .codecVersion(CODEC_V1)
            .globalMetadataFileName("global-metadata-file")
            .indices(Collections.emptyList())
            .build();

        verifyWriteIncrementalGlobalMetadataFromOlderCodecSuccess(previousManifest);
    }

    private void verifyWriteIncrementalGlobalMetadataFromOlderCodecSuccess(ClusterMetadataManifest previousManifest) throws IOException {
        final ClusterState clusterState = generateClusterStateWithGlobalMetadata().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .build();

        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeIncrementalMetadata(
            previousClusterState,
            clusterState,
            previousManifest
        ).getClusterMetadataManifest();

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .codecVersion(MANIFEST_CURRENT_CODEC_VERSION)
            .indices(Collections.emptyList())
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        assertNull(manifest.getGlobalMetadataFileName());
        assertNotNull(manifest.getCoordinationMetadata());
        assertNotNull(manifest.getSettingsMetadata());
        assertNotNull(manifest.getTemplatesMetadata());
        assertNotNull(manifest.getCustomMetadataMap());

        assertEquals(expectedManifest.getClusterTerm(), manifest.getClusterTerm());
        assertEquals(expectedManifest.getStateVersion(), manifest.getStateVersion());
        assertEquals(expectedManifest.getClusterUUID(), manifest.getClusterUUID());
        assertEquals(expectedManifest.getStateUUID(), manifest.getStateUUID());
        assertEquals(expectedManifest.getCodecVersion(), manifest.getCodecVersion());
    }

    public void testCoordinationMetadataOnlyUpdated() throws IOException {
        // Updating the voting config, as updating the term will upload the full cluster state and other files will also get updated
        Function<ClusterState, ClusterState> updater = (initialClusterState) -> ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.metadata())
                    .version(initialClusterState.metadata().version() + 1)
                    .coordinationMetadata(
                        CoordinationMetadata.builder(initialClusterState.coordinationMetadata())
                            .addVotingConfigExclusion(new CoordinationMetadata.VotingConfigExclusion("excludedNodeId", "excludedNodeName"))
                            .build()
                    )
                    .build()
            )
            .build();
        verifyMetadataAttributeOnlyUpdated(updater, (initialMetadata, metadataAfterUpdate) -> {
            // Verify that index metadata information is same in manifest files
            assertEquals(metadataAfterUpdate.getIndices().size(), initialMetadata.getIndices().size());
            IntStream.range(0, initialMetadata.getIndices().size()).forEach(i -> {
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexName(), initialMetadata.getIndices().get(i).getIndexName());
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexUUID(), initialMetadata.getIndices().get(i).getIndexUUID());
                // since timestamp is part of file name, if file name is same we can confirm that file is not update in global metadata
                // update
                assertEquals(
                    metadataAfterUpdate.getIndices().get(i).getUploadedFilename(),
                    initialMetadata.getIndices().get(i).getUploadedFilename()
                );
            });

            // coordination metadata file would have changed
            assertFalse(
                metadataAfterUpdate.getCoordinationMetadata()
                    .getUploadedFilename()
                    .equalsIgnoreCase(initialMetadata.getCoordinationMetadata().getUploadedFilename())
            );
            // Other files will be equal
            assertEquals(
                metadataAfterUpdate.getSettingsMetadata().getUploadedFilename(),
                initialMetadata.getSettingsMetadata().getUploadedFilename()
            );
            assertEquals(metadataAfterUpdate.getTemplatesMetadata(), initialMetadata.getTemplatesMetadata());
            assertEquals(metadataAfterUpdate.getCustomMetadataMap(), initialMetadata.getCustomMetadataMap());
        });
    }

    public void testSettingsMetadataOnlyUpdated() throws IOException {
        Function<ClusterState, ClusterState> updater = (initialClusterState) -> ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.metadata()).persistentSettings(Settings.builder().put("foo", "bar").build()).build()
            )
            .build();

        verifyMetadataAttributeOnlyUpdated(updater, (initialMetadata, metadataAfterUpdate) -> {
            // Verify that index metadata information is same in manifest files
            assertEquals(metadataAfterUpdate.getIndices().size(), initialMetadata.getIndices().size());
            IntStream.range(0, initialMetadata.getIndices().size()).forEach(i -> {
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexName(), initialMetadata.getIndices().get(i).getIndexName());
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexUUID(), initialMetadata.getIndices().get(i).getIndexUUID());
                // since timestamp is part of file name, if file name is same we can confirm that file is not update in global metadata
                // update
                assertEquals(
                    metadataAfterUpdate.getIndices().get(i).getUploadedFilename(),
                    initialMetadata.getIndices().get(i).getUploadedFilename()
                );
            });

            // setting metadata file would have changed
            assertFalse(
                metadataAfterUpdate.getSettingsMetadata()
                    .getUploadedFilename()
                    .equalsIgnoreCase(initialMetadata.getSettingsMetadata().getUploadedFilename())
            );
            assertEquals(metadataAfterUpdate.getCoordinationMetadata(), initialMetadata.getCoordinationMetadata());
            assertEquals(metadataAfterUpdate.getTemplatesMetadata(), initialMetadata.getTemplatesMetadata());
            assertEquals(metadataAfterUpdate.getCustomMetadataMap(), initialMetadata.getCustomMetadataMap());
        });
    }

    public void testTemplatesMetadataOnlyUpdated() throws IOException {
        Function<ClusterState, ClusterState> updater = (initialClusterState) -> ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.metadata())
                    .templates(
                        TemplatesMetadata.builder()
                            .put(
                                IndexTemplateMetadata.builder("template" + randomAlphaOfLength(3))
                                    .patterns(Arrays.asList("bar-*", "foo-*"))
                                    .settings(
                                        Settings.builder()
                                            .put("random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5))
                                            .build()
                                    )
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        verifyMetadataAttributeOnlyUpdated(updater, (initialMetadata, metadataAfterUpdate) -> {
            // Verify that index metadata information is same in manifest files
            assertEquals(metadataAfterUpdate.getIndices().size(), initialMetadata.getIndices().size());
            IntStream.range(0, initialMetadata.getIndices().size()).forEach(i -> {
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexName(), initialMetadata.getIndices().get(i).getIndexName());
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexUUID(), initialMetadata.getIndices().get(i).getIndexUUID());
                // since timestamp is part of file name, if file name is same we can confirm that file is not update in global metadata
                // update
                assertEquals(
                    metadataAfterUpdate.getIndices().get(i).getUploadedFilename(),
                    initialMetadata.getIndices().get(i).getUploadedFilename()
                );
            });

            // template metadata file would have changed
            assertFalse(
                metadataAfterUpdate.getTemplatesMetadata()
                    .getUploadedFilename()
                    .equalsIgnoreCase(initialMetadata.getTemplatesMetadata().getUploadedFilename())
            );
            assertEquals(metadataAfterUpdate.getCoordinationMetadata(), initialMetadata.getCoordinationMetadata());
            assertEquals(metadataAfterUpdate.getSettingsMetadata(), initialMetadata.getSettingsMetadata());
            assertEquals(metadataAfterUpdate.getCustomMetadataMap(), initialMetadata.getCustomMetadataMap());
        });
    }

    public void testCustomMetadataOnlyUpdated() throws IOException {
        Function<ClusterState, ClusterState> updater = (initialClusterState) -> ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.metadata())
                    .putCustom("custom_metadata_type", new CustomMetadata1("mock_custom_metadata"))
                    .build()
            )
            .build();

        verifyMetadataAttributeOnlyUpdated(updater, (initialMetadata, metadataAfterUpdate) -> {
            // Verify that index metadata information is same in manifest files
            assertEquals(metadataAfterUpdate.getIndices().size(), initialMetadata.getIndices().size());
            IntStream.range(0, initialMetadata.getIndices().size()).forEach(i -> {
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexName(), initialMetadata.getIndices().get(i).getIndexName());
                assertEquals(metadataAfterUpdate.getIndices().get(i).getIndexUUID(), initialMetadata.getIndices().get(i).getIndexUUID());
                // since timestamp is part of file name, if file name is same we can confirm that file is not update in global metadata
                // update
                assertEquals(
                    metadataAfterUpdate.getIndices().get(i).getUploadedFilename(),
                    initialMetadata.getIndices().get(i).getUploadedFilename()
                );
                // custom metadata map would have changed
                assertNotEquals(metadataAfterUpdate.getCustomMetadataMap(), initialMetadata.getCustomMetadataMap());
                assertEquals(initialMetadata.getCustomMetadataMap().size() + 1, metadataAfterUpdate.getCustomMetadataMap().size());
                initialMetadata.getCustomMetadataMap().forEach((k, v) -> {
                    assertTrue(metadataAfterUpdate.getCustomMetadataMap().containsKey(k));
                    assertEquals(v, metadataAfterUpdate.getCustomMetadataMap().get(k));
                });
                assertEquals(metadataAfterUpdate.getCoordinationMetadata(), initialMetadata.getCoordinationMetadata());
                assertEquals(metadataAfterUpdate.getSettingsMetadata(), initialMetadata.getSettingsMetadata());
                assertEquals(metadataAfterUpdate.getTemplatesMetadata(), initialMetadata.getTemplatesMetadata());
            });
        });
    }

    public void testCustomMetadataDeletedUpdatedAndAdded() throws IOException {
        // setup
        mockBlobStoreObjects();

        // Initial cluster state with index.
        final ClusterState initialClusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.start();
        final ClusterMetadataManifest initialManifest = remoteClusterStateService.writeFullMetadata(initialClusterState, "_na_")
            .getClusterMetadataManifest();

        ClusterState clusterState1 = ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.metadata())
                    .putCustom("custom1", new CustomMetadata1("mock_custom_metadata1"))
                    .putCustom("custom2", new CustomMetadata1("mock_custom_metadata2"))
                    .putCustom("custom3", new CustomMetadata1("mock_custom_metadata3"))
                    .version(initialClusterState.metadata().version() + 1)
            )
            .build();

        ClusterMetadataManifest manifest1 = remoteClusterStateService.writeIncrementalMetadata(
            initialClusterState,
            clusterState1,
            initialManifest
        ).getClusterMetadataManifest();
        // remove custom1 from the cluster state, update custom2, custom3 is at it is, added custom4
        ClusterState clusterState2 = ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.metadata())
                    .putCustom("custom2", new CustomMetadata1("mock_updated_custom_metadata"))
                    .putCustom("custom3", new CustomMetadata1("mock_custom_metadata3"))
                    .putCustom("custom4", new CustomMetadata1("mock_custom_metadata4"))
                    .version(clusterState1.metadata().version() + 1)
            )
            .build();
        ClusterMetadataManifest manifest2 = remoteClusterStateService.writeIncrementalMetadata(clusterState1, clusterState2, manifest1)
            .getClusterMetadataManifest();
        // custom1 is removed
        assertFalse(manifest2.getCustomMetadataMap().containsKey("custom1"));
        // custom2 is updated
        assertNotEquals(manifest1.getCustomMetadataMap().get("custom2"), manifest2.getCustomMetadataMap().get("custom2"));
        // custom3 is unchanged
        assertEquals(manifest1.getCustomMetadataMap().get("custom3"), manifest2.getCustomMetadataMap().get("custom3"));
        // custom4 is added
        assertTrue(manifest2.getCustomMetadataMap().containsKey("custom4"));
        assertFalse(manifest1.getCustomMetadataMap().containsKey("custom4"));
    }

    /*
     * Here we will verify global metadata is not uploaded again if change is only in index metadata
     */
    public void testIndexMetadataOnlyUpdated() throws IOException {
        Function<ClusterState, ClusterState> updater = (initialState) -> ClusterState.builder(initialState)
            .metadata(
                Metadata.builder(initialState.metadata())
                    .put(
                        IndexMetadata.builder("test" + randomAlphaOfLength(3))
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                                    .build()
                            )
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
                    .build()
            )
            .build();

        verifyMetadataAttributeOnlyUpdated(updater, (initialMetadata, metadataAfterUpdate) -> {
            assertEquals(metadataAfterUpdate.getCoordinationMetadata(), initialMetadata.getCoordinationMetadata());
            assertEquals(metadataAfterUpdate.getSettingsMetadata(), initialMetadata.getSettingsMetadata());
            assertEquals(metadataAfterUpdate.getTemplatesMetadata(), initialMetadata.getTemplatesMetadata());
            assertEquals(metadataAfterUpdate.getCustomMetadataMap(), initialMetadata.getCustomMetadataMap());
            assertEquals(initialMetadata.getIndices().size() + 1, metadataAfterUpdate.getIndices().size());
        });
    }

    public void testIndexMetadataDeletedUpdatedAndAdded() throws IOException {
        // setup
        mockBlobStoreObjects();

        // Initial cluster state with index.
        final ClusterState initialClusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.start();
        final ClusterMetadataManifest initialManifest = remoteClusterStateService.writeFullMetadata(initialClusterState, "_na_")
            .getClusterMetadataManifest();
        String initialIndex = "test-index";
        Index index1 = new Index("test-index-1", "index-uuid-1");
        Index index2 = new Index("test-index-2", "index-uuid-2");
        Settings idxSettings1 = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index1.getUUID())
            .build();
        IndexMetadata indexMetadata1 = new IndexMetadata.Builder(index1.getName()).settings(idxSettings1)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Settings idxSettings2 = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index2.getUUID())
            .build();
        IndexMetadata indexMetadata2 = new IndexMetadata.Builder(index2.getName()).settings(idxSettings2)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState clusterState1 = ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.getMetadata())
                    .put(indexMetadata1, true)
                    .put(indexMetadata2, true)
                    .remove(initialIndex)
                    .build()
            )
            .build();
        ClusterMetadataManifest manifest1 = remoteClusterStateService.writeIncrementalMetadata(
            initialClusterState,
            clusterState1,
            initialManifest
        ).getClusterMetadataManifest();
        // verify that initial index is removed, and new index are added
        assertEquals(1, initialManifest.getIndices().size());
        assertEquals(2, manifest1.getIndices().size());
        assertTrue(initialManifest.getIndices().stream().anyMatch(indexMetadata -> indexMetadata.getIndexName().equals(initialIndex)));
        assertFalse(manifest1.getIndices().stream().anyMatch(indexMetadata -> indexMetadata.getIndexName().equals(initialIndex)));
        // update index1, index2 is unchanged
        indexMetadata1 = new IndexMetadata.Builder(indexMetadata1).version(indexMetadata1.getVersion() + 1).build();
        ClusterState clusterState2 = ClusterState.builder(clusterState1)
            .metadata(Metadata.builder(clusterState1.getMetadata()).put(indexMetadata1, true).build())
            .build();
        ClusterMetadataManifest manifest2 = remoteClusterStateService.writeIncrementalMetadata(clusterState1, clusterState2, manifest1)
            .getClusterMetadataManifest();
        // index1 is updated
        assertEquals(2, manifest2.getIndices().size());
        assertEquals(
            1,
            manifest2.getIndices().stream().filter(uploadedIndex -> uploadedIndex.getIndexName().equals(index1.getName())).count()
        );
        assertNotEquals(
            manifest2.getIndices()
                .stream()
                .filter(uploadedIndex -> uploadedIndex.getIndexName().equals(index1.getName()))
                .findFirst()
                .get()
                .getUploadedFilename(),
            manifest1.getIndices()
                .stream()
                .filter(uploadedIndex -> uploadedIndex.getIndexName().equals(index1.getName()))
                .findFirst()
                .get()
                .getUploadedFilename()
        );
    }

    private void verifyMetadataAttributeOnlyUpdated(
        Function<ClusterState, ClusterState> clusterStateUpdater,
        BiConsumer<ClusterMetadataManifest, ClusterMetadataManifest> assertions
    ) throws IOException {
        // setup
        mockBlobStoreObjects();

        // Initial cluster state with index.
        final ClusterState initialClusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.start();
        final ClusterMetadataManifest initialManifest = remoteClusterStateService.writeFullMetadata(initialClusterState, "_na_")
            .getClusterMetadataManifest();

        ClusterState newClusterState = clusterStateUpdater.apply(initialClusterState);

        // updating remote cluster state with global metadata
        final ClusterMetadataManifest manifestAfterMetadataUpdate;
        if (initialClusterState.term() == newClusterState.term()) {
            manifestAfterMetadataUpdate = remoteClusterStateService.writeIncrementalMetadata(
                initialClusterState,
                newClusterState,
                initialManifest
            ).getClusterMetadataManifest();
        } else {
            manifestAfterMetadataUpdate = remoteClusterStateService.writeFullMetadata(newClusterState, initialClusterState.stateUUID())
                .getClusterMetadataManifest();
        }

        assertions.accept(initialManifest, manifestAfterMetadataUpdate);
    }

    public void testReadLatestMetadataManifestFailedNoManifestFileInRemote() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        when(blobContainer.listBlobsByPrefixInSortedOrder("manifest" + DELIMITER, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenReturn(List.of());

        remoteClusterStateService.start();
        Optional<ClusterMetadataManifest> manifest = remoteClusterStateService.getLatestClusterMetadataManifest(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID()
        );
        assertEquals(manifest, Optional.empty());
    }

    public void testReadLatestMetadataManifestFailedManifestFileRemoveAfterFetchInRemote() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        BlobMetadata blobMetadata = new PlainBlobMetadata("manifestFileName", 1);
        when(blobContainer.listBlobsByPrefixInSortedOrder("manifest" + DELIMITER, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenReturn(Arrays.asList(blobMetadata));
        when(blobContainer.readBlob("manifestFileName")).thenThrow(FileNotFoundException.class);

        remoteClusterStateService.start();
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> remoteClusterStateService.getLatestClusterMetadataManifest(
                clusterState.getClusterName().value(),
                clusterState.metadata().clusterUUID()
            )
        );
        assertEquals(e.getMessage(), "Error while downloading cluster metadata - manifestFileName");
    }

    public void testReadLatestMetadataManifestSuccessButNoIndexMetadata() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(List.of())
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID("prev-cluster-uuid")
            .codecVersion(ClusterMetadataManifest.CODEC_V0)
            .build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        mockBlobContainer(blobContainer, expectedManifest, Map.of());

        remoteClusterStateService.start();
        assertEquals(
            remoteClusterStateService.getLatestClusterState(
                clusterState.getClusterName().value(),
                clusterState.metadata().clusterUUID(),
                false
            ).getMetadata().getIndices().size(),
            0
        );
    }

    public void testReadLatestMetadataManifestSuccessButIndexMetadataFetchIOException() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename__2");
        final List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);
        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .codecVersion(CODEC_V2)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        mockBlobContainer(blobContainer, expectedManifest, Map.of(), CODEC_V2);
        when(blobContainer.readBlob(uploadedIndexMetadata.getUploadedFilename())).thenThrow(FileNotFoundException.class);

        remoteClusterStateService.start();
        Exception e = assertThrows(
            RemoteStateTransferException.class,
            () -> remoteClusterStateService.getLatestClusterState(
                clusterState.getClusterName().value(),
                clusterState.metadata().clusterUUID(),
                false
            ).getMetadata().getIndices()
        );
        assertEquals("Exception during reading cluster state from remote", e.getMessage());
    }

    public void testReadLatestMetadataManifestSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        final List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .codecVersion(CODEC_V2)
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        mockBlobContainer(mockBlobStoreObjects(), expectedManifest, new HashMap<>(), CODEC_V2);
        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.getLatestClusterMetadataManifest(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID()
        ).get();

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
    }

    public void testReadGlobalMetadata() throws IOException {
        when(blobStoreRepository.getNamedXContentRegistry()).thenReturn(new NamedXContentRegistry(
            List.of(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(IndexGraveyard.TYPE), IndexGraveyard::fromXContent))));
        final ClusterState clusterState = generateClusterStateWithGlobalMetadata().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.start();

        long prevClusterStateVersion = 13L;
        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(List.of())
            .clusterTerm(1L)
            .stateVersion(prevClusterStateVersion)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .codecVersion(MANIFEST_CURRENT_CODEC_VERSION)
            .coordinationMetadata(new ClusterMetadataManifest.UploadedMetadataAttribute(COORDINATION_METADATA, "mock-coordination-file"))
            .settingMetadata(new ClusterMetadataManifest.UploadedMetadataAttribute(SETTING_METADATA, "mock-setting-file"))
            .templatesMetadata(new ClusterMetadataManifest.UploadedMetadataAttribute(TEMPLATES_METADATA, "mock-templates-file"))
            .put(IndexGraveyard.TYPE, new ClusterMetadataManifest.UploadedMetadataAttribute(IndexGraveyard.TYPE, "mock-custom-" +IndexGraveyard.TYPE+ "-file"))
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID("prev-cluster-uuid")
            .routingTableVersion(1)
            .indicesRouting(List.of())
            .build();

        Metadata expectedMetadata = Metadata.builder().clusterUUID("cluster-uuid").persistentSettings(Settings.builder().put("readonly", true).build()).build();
        mockBlobContainerForGlobalMetadata(mockBlobStoreObjects(), expectedManifest, expectedMetadata);

        ClusterState newClusterState = remoteClusterStateService.getLatestClusterState(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID(),
            false
        );

        assertTrue(Metadata.isGlobalStateEquals(newClusterState.getMetadata(), expectedMetadata));

        long newClusterStateVersion = newClusterState.getVersion();
        assert prevClusterStateVersion == newClusterStateVersion : String.format(
            Locale.ROOT,
            "ClusterState version is not restored. previousClusterVersion: [%s] is not equal to current [%s]",
            prevClusterStateVersion,
            newClusterStateVersion
        );
    }

    public void testReadGlobalMetadataIOException() throws IOException {
        final ClusterState clusterState = generateClusterStateWithGlobalMetadata().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.start();
        String globalIndexMetadataName = "global-metadata-file";
        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(List.of())
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .codecVersion(CODEC_V1)
            .globalMetadataFileName(globalIndexMetadataName)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        Metadata expactedMetadata = Metadata.builder().persistentSettings(Settings.builder().put("readonly", true).build()).build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        mockBlobContainerForGlobalMetadata(blobContainer, expectedManifest, expactedMetadata);

        when(blobContainer.readBlob(GLOBAL_METADATA_FORMAT.blobName(globalIndexMetadataName))).thenThrow(FileNotFoundException.class);

        remoteClusterStateService.start();
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> remoteClusterStateService.getLatestClusterState(
                clusterState.getClusterName().value(),
                clusterState.metadata().clusterUUID(),
                false
            )
        );
        assertEquals(e.getMessage(), "Error while downloading Global Metadata - " + globalIndexMetadataName);
    }

    public void testReadLatestIndexMetadataSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.start();

        final Index index = new Index("test-index", "index-uuid");
        String fileName = "metadata-" + index.getUUID() + "__1";
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata(index.getName(), index.getUUID(), fileName);
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(11)
            .numberOfReplicas(10)
            .build();

        final List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);
        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID("prev-cluster-uuid")
            .codecVersion(CODEC_V2)
            .build();

        mockBlobContainer(mockBlobStoreObjects(), expectedManifest, Map.of(index.getUUID(), indexMetadata), CODEC_V2);

        Map<String, IndexMetadata> indexMetadataMap = remoteClusterStateService.getLatestClusterState(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID(),
            false
        ).getMetadata().getIndices();

        assertEquals(indexMetadataMap.size(), 1);
        assertEquals(indexMetadataMap.get(index.getName()).getIndex().getName(), index.getName());
        assertEquals(indexMetadataMap.get(index.getName()).getNumberOfShards(), indexMetadata.getNumberOfShards());
        assertEquals(indexMetadataMap.get(index.getName()).getNumberOfReplicas(), indexMetadata.getNumberOfReplicas());
    }

    public void testMarkLastStateAsCommittedSuccess() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        remoteClusterStateService.start();
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        List<UploadedIndexMetadata> indices = List.of(uploadedIndexMetadata);
        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder().indices(indices).build();

        final ClusterMetadataManifest manifest = remoteClusterStateService.markLastStateAsCommitted(clusterState, previousManifest)
            .getClusterMetadataManifest();

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(indices)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .nodeId("nodeA")
            .previousClusterUUID("prev-cluster-uuid")
            .build();

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
    }

    public void testGetValidPreviousClusterUUID() throws IOException {
        Map<String, String> clusterUUIDsPointers = Map.of(
            "cluster-uuid1",
            ClusterState.UNKNOWN_UUID,
            "cluster-uuid2",
            "cluster-uuid1",
            "cluster-uuid3",
            "cluster-uuid2"
        );
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers);

        remoteClusterStateService.start();
        String previousClusterUUID = remoteClusterStateService.getLastKnownUUIDFromRemote("test-cluster");
        assertThat(previousClusterUUID, equalTo("cluster-uuid3"));
    }

    public void testGetValidPreviousClusterUUIDForInvalidChain() throws IOException {
        Map<String, String> clusterUUIDsPointers = Map.of(
            "cluster-uuid2",
            "cluster-uuid1",
            "cluster-uuid3",
            "cluster-uuid2",
            "cluster-uuid5",
            "cluster-uuid4"
        );
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers);

        remoteClusterStateService.start();
        assertThrows(IllegalStateException.class, () -> remoteClusterStateService.getLastKnownUUIDFromRemote("test-cluster"));
    }

    public void testGetValidPreviousClusterUUIDWithMultipleChains() throws IOException {
        Map<String, String> clusterUUIDsPointers = Map.of(
            "cluster-uuid2",
            "cluster-uuid1",
            "cluster-uuid1",
            ClusterState.UNKNOWN_UUID,
            "cluster-uuid3",
            "cluster-uuid1"
        );
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers, randomBoolean(), Collections.emptyMap());

        remoteClusterStateService.start();
        String previousClusterUUID = remoteClusterStateService.getLastKnownUUIDFromRemote("test-cluster");
        assertThat(previousClusterUUID, equalTo("cluster-uuid3"));
    }

    public void testGetValidPreviousClusterUUIDWithInvalidMultipleChains() throws IOException {
        Map<String, String> clusterUUIDsPointers = Map.of(
            "cluster-uuid1",
            ClusterState.UNKNOWN_UUID,
            "cluster-uuid2",
            "cluster-uuid1",
            "cluster-uuid3",
            ClusterState.UNKNOWN_UUID
        );
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers);

        remoteClusterStateService.start();
        assertThrows(IllegalStateException.class, () -> remoteClusterStateService.getLastKnownUUIDFromRemote("test-cluster"));
    }

    public void testGetValidPreviousClusterUUIDWhenLastUUIDUncommitted() throws IOException {
        Map<String, String> clusterUUIDsPointers = Map.of(
            "cluster-uuid1",
            ClusterState.UNKNOWN_UUID,
            "cluster-uuid2",
            "cluster-uuid1",
            "cluster-uuid3",
            "cluster-uuid2"
        );
        Map<String, Boolean> clusterUUIDCommitted = Map.of("cluster-uuid1", true, "cluster-uuid2", true, "cluster-uuid3", false);
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers, clusterUUIDCommitted);

        remoteClusterStateService.start();
        String previousClusterUUID = remoteClusterStateService.getLastKnownUUIDFromRemote("test-cluster");
        assertThat(previousClusterUUID, equalTo("cluster-uuid2"));
    }

    public void testRemoteStateStats() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeFullMetadata(clusterState, "prev-cluster-uuid")
            .getClusterMetadataManifest();

        assertTrue(remoteClusterStateService.getStats() != null);
        assertEquals(1, remoteClusterStateService.getStats().getSuccessCount());
        assertEquals(0, remoteClusterStateService.getStats().getCleanupAttemptFailedCount());
        assertEquals(0, remoteClusterStateService.getStats().getFailedCount());
    }

    public void testRemoteRoutingTableNotInitializedWhenDisabled() {
        if (publicationEnabled) {
            assertTrue(remoteClusterStateService.getRemoteRoutingTableService() instanceof InternalRemoteRoutingTableService);
        } else {
            assertTrue(remoteClusterStateService.getRemoteRoutingTableService() instanceof NoopRemoteRoutingTableService);
        }
    }

    public void testRemoteRoutingTableInitializedWhenEnabled() {
        Settings newSettings = Settings.builder()
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, "routing_repository")
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, "remote_store_repository")
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .build();
        clusterSettings.applySettings(newSettings);

        Settings nodeSettings = Settings.builder().put(REMOTE_PUBLICATION_EXPERIMENTAL, "true").build();
        FeatureFlags.initializeFeatureFlags(nodeSettings);

        remoteClusterStateService = new RemoteClusterStateService(
            "test-node-id",
            repositoriesServiceSupplier,
            newSettings,
            clusterService,
            () -> 0L,
            threadPool,
            List.of(new RemoteIndexPathUploader(threadPool, newSettings, repositoriesServiceSupplier, clusterSettings)),
            writableRegistry()
        );
        assertTrue(remoteClusterStateService.getRemoteRoutingTableService() instanceof InternalRemoteRoutingTableService);
    }

    public void testWriteFullMetadataSuccessWithRoutingTable() throws IOException {
        initializeRoutingTable();
        mockBlobStoreObjects();
        when((blobStoreRepository.basePath())).thenReturn(BlobPath.cleanPath().add("base-path"));

        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeFullMetadata(clusterState, "prev-cluster-uuid")
            .getClusterMetadataManifest();
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        final UploadedIndexMetadata uploadedIndiceRoutingMetadata = new UploadedIndexMetadata(
            "test-index",
            "index-uuid",
            "routing-filename",
            INDEX_ROUTING_METADATA_PREFIX
        );
        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(List.of(uploadedIndexMetadata))
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .routingTableVersion(1L)
            .indicesRouting(List.of(uploadedIndiceRoutingMetadata))
            .build();

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
        assertThat(manifest.getPreviousClusterUUID(), is(expectedManifest.getPreviousClusterUUID()));
        assertThat(manifest.getRoutingTableVersion(), is(expectedManifest.getRoutingTableVersion()));
        assertThat(manifest.getIndicesRouting().get(0).getIndexName(), is(uploadedIndiceRoutingMetadata.getIndexName()));
        assertThat(manifest.getIndicesRouting().get(0).getIndexUUID(), is(uploadedIndiceRoutingMetadata.getIndexUUID()));
        assertThat(manifest.getIndicesRouting().get(0).getUploadedFilename(), notNullValue());
    }

    public void testWriteFullMetadataInParallelSuccessWithRoutingTable() throws IOException {
        initializeRoutingTable();
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        AsyncMultiStreamBlobContainer container = (AsyncMultiStreamBlobContainer) mockBlobStoreObjects(AsyncMultiStreamBlobContainer.class);

        ArgumentCaptor<ActionListener<Void>> actionListenerArgumentCaptor = ArgumentCaptor.forClass(ActionListener.class);
        ArgumentCaptor<WriteContext> writeContextArgumentCaptor = ArgumentCaptor.forClass(WriteContext.class);
        ConcurrentHashMap<String, WriteContext> capturedWriteContext = new ConcurrentHashMap<>();
        doAnswer((i) -> {
            actionListenerArgumentCaptor.getValue().onResponse(null);
            WriteContext writeContext = writeContextArgumentCaptor.getValue();
            capturedWriteContext.put(writeContext.getFileName().split(DELIMITER)[0], writeContextArgumentCaptor.getValue());
            return null;
        }).when(container).asyncBlobUpload(writeContextArgumentCaptor.capture(), actionListenerArgumentCaptor.capture());

        when((blobStoreRepository.basePath())).thenReturn(BlobPath.cleanPath().add("base-path"));

        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeFullMetadata(clusterState, "prev-cluster-uuid")
            .getClusterMetadataManifest();

        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        final UploadedIndexMetadata uploadedIndiceRoutingMetadata = new UploadedIndexMetadata(
            "test-index",
            "index-uuid",
            "routing-filename",
            INDEX_ROUTING_METADATA_PREFIX
        );

        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(List.of(uploadedIndexMetadata))
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .routingTableVersion(1)
            .codecVersion(CODEC_V2)
            .indicesRouting(List.of(uploadedIndiceRoutingMetadata))
            .build();

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getIndices().get(0).getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(manifest.getIndices().get(0).getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(manifest.getIndices().get(0).getUploadedFilename(), notNullValue());
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
        assertThat(manifest.getPreviousClusterUUID(), is(expectedManifest.getPreviousClusterUUID()));
        assertThat(manifest.getRoutingTableVersion(), is(expectedManifest.getRoutingTableVersion()));
        assertThat(manifest.getIndicesRouting().get(0).getIndexName(), is(uploadedIndiceRoutingMetadata.getIndexName()));
        assertThat(manifest.getIndicesRouting().get(0).getIndexUUID(), is(uploadedIndiceRoutingMetadata.getIndexUUID()));
        assertThat(manifest.getIndicesRouting().get(0).getUploadedFilename(), notNullValue());

        assertEquals(12, actionListenerArgumentCaptor.getAllValues().size());
        assertEquals(12, writeContextArgumentCaptor.getAllValues().size());
    }

    public void testWriteIncrementalMetadataSuccessWithRoutingTable() throws IOException {
        initializeRoutingTable();
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();
        mockBlobStoreObjects();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .build();

        final ClusterMetadataManifest previousManifest = ClusterMetadataManifest.builder().indices(Collections.emptyList()).build();
        when((blobStoreRepository.basePath())).thenReturn(BlobPath.cleanPath().add("base-path"));

        remoteClusterStateService.start();
        final ClusterMetadataManifest manifest = remoteClusterStateService.writeIncrementalMetadata(
            previousClusterState,
            clusterState,
            previousManifest
        ).getClusterMetadataManifest();
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        final UploadedIndexMetadata uploadedIndiceRoutingMetadata = new UploadedIndexMetadata(
            "test-index",
            "index-uuid",
            "routing-filename",
            INDEX_ROUTING_METADATA_PREFIX
        );
        final ClusterMetadataManifest expectedManifest = ClusterMetadataManifest.builder()
            .indices(List.of(uploadedIndexMetadata))
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .previousClusterUUID("prev-cluster-uuid")
            .routingTableVersion(1)
            .indicesRouting(List.of(uploadedIndiceRoutingMetadata))
            .build();

        assertThat(manifest.getIndices().size(), is(1));
        assertThat(manifest.getClusterTerm(), is(expectedManifest.getClusterTerm()));
        assertThat(manifest.getStateVersion(), is(expectedManifest.getStateVersion()));
        assertThat(manifest.getClusterUUID(), is(expectedManifest.getClusterUUID()));
        assertThat(manifest.getStateUUID(), is(expectedManifest.getStateUUID()));
        assertThat(manifest.getRoutingTableVersion(), is(expectedManifest.getRoutingTableVersion()));
        assertThat(manifest.getIndicesRouting().get(0).getIndexName(), is(uploadedIndiceRoutingMetadata.getIndexName()));
        assertThat(manifest.getIndicesRouting().get(0).getIndexUUID(), is(uploadedIndiceRoutingMetadata.getIndexUUID()));
        assertThat(manifest.getIndicesRouting().get(0).getUploadedFilename(), notNullValue());
    }

    private void initializeRoutingTable() {
        Settings newSettings = Settings.builder()
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, "routing_repository")
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, "remote_store_repository")
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .build();
        clusterSettings.applySettings(newSettings);

        Settings nodeSettings = Settings.builder().put(REMOTE_PUBLICATION_EXPERIMENTAL, "true").build();
        FeatureFlags.initializeFeatureFlags(nodeSettings);
        remoteClusterStateService = new RemoteClusterStateService(
            "test-node-id",
            repositoriesServiceSupplier,
            newSettings,
            clusterService,
            () -> 0L,
            threadPool,
            List.of(new RemoteIndexPathUploader(threadPool, newSettings, repositoriesServiceSupplier, clusterSettings)),
            writableRegistry()
        );
    }

    private void mockObjectsForGettingPreviousClusterUUID(Map<String, String> clusterUUIDsPointers) throws IOException {
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers, false, Collections.emptyMap());
    }

    private void mockObjectsForGettingPreviousClusterUUID(
        Map<String, String> clusterUUIDsPointers,
        Map<String, Boolean> clusterUUIDCommitted
    ) throws IOException {
        mockObjectsForGettingPreviousClusterUUID(clusterUUIDsPointers, false, clusterUUIDCommitted);
    }

    private void mockObjectsForGettingPreviousClusterUUID(
        Map<String, String> clusterUUIDsPointers,
        boolean differGlobalMetadata,
        Map<String, Boolean> clusterUUIDCommitted
    ) throws IOException {
        final BlobPath blobPath = mock(BlobPath.class);
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        when(blobPath.add(anyString())).thenReturn(blobPath);
        when(blobPath.buildAsString()).thenReturn("/blob/path/");
        BlobContainer blobContainer1 = mock(BlobContainer.class);
        BlobContainer blobContainer2 = mock(BlobContainer.class);
        BlobContainer blobContainer3 = mock(BlobContainer.class);
        BlobContainer uuidBlobContainer = mock(BlobContainer.class);
        when(blobContainer1.path()).thenReturn(blobPath);
        when(blobContainer2.path()).thenReturn(blobPath);
        when(blobContainer3.path()).thenReturn(blobPath);

        mockBlobContainerForClusterUUIDs(uuidBlobContainer, clusterUUIDsPointers.keySet());
        List<UploadedIndexMetadata> uploadedIndexMetadataList1 = List.of(
            new UploadedIndexMetadata("index1", "index-uuid1", "key1__2"),
            new UploadedIndexMetadata("index2", "index-uuid2", "key2__2")
        );
        Map<String, UploadedMetadataAttribute> customMetadataMap = new HashMap<>();
        final ClusterMetadataManifest clusterManifest1 = generateClusterMetadataManifest(
            "cluster-uuid1",
            clusterUUIDsPointers.get("cluster-uuid1"),
            randomAlphaOfLength(10),
            uploadedIndexMetadataList1,
            customMetadataMap,
            new UploadedMetadataAttribute(COORDINATION_METADATA, "key3"),
            new UploadedMetadataAttribute(SETTING_METADATA, "key4"),
            new UploadedMetadataAttribute(TEMPLATES_METADATA, "key5"),
            clusterUUIDCommitted.getOrDefault("cluster-uuid1", true)
        );
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexMetadata indexMetadata1 = IndexMetadata.builder("index1")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        IndexMetadata indexMetadata2 = IndexMetadata.builder("index2")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        Metadata metadata1 = Metadata.builder()
            .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build())
            .build();
        Map<String, IndexMetadata> indexMetadataMap1 = Map.of("index-uuid1", indexMetadata1, "index-uuid2", indexMetadata2);
        mockBlobContainerForGlobalMetadata(blobContainer1, clusterManifest1, metadata1);
        mockBlobContainer(blobContainer1, clusterManifest1, indexMetadataMap1, MANIFEST_CURRENT_CODEC_VERSION);

        List<UploadedIndexMetadata> uploadedIndexMetadataList2 = List.of(
            new UploadedIndexMetadata("index1", "index-uuid1", "key1__2"),
            new UploadedIndexMetadata("index2", "index-uuid2", "key2__2")
        );
        final ClusterMetadataManifest clusterManifest2 = generateClusterMetadataManifest(
            "cluster-uuid2",
            clusterUUIDsPointers.get("cluster-uuid2"),
            randomAlphaOfLength(10),
            uploadedIndexMetadataList2,
            customMetadataMap,
            new UploadedMetadataAttribute(COORDINATION_METADATA, "key3"),
            new UploadedMetadataAttribute(SETTING_METADATA, "key4"),
            new UploadedMetadataAttribute(TEMPLATES_METADATA, "key5"),
            clusterUUIDCommitted.getOrDefault("cluster-uuid2", true)
        );
        IndexMetadata indexMetadata3 = IndexMetadata.builder("index1")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        IndexMetadata indexMetadata4 = IndexMetadata.builder("index2")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        Metadata metadata2 = Metadata.builder()
            .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build())
            .build();
        Map<String, IndexMetadata> indexMetadataMap2 = Map.of("index-uuid1", indexMetadata3, "index-uuid2", indexMetadata4);
        mockBlobContainerForGlobalMetadata(blobContainer2, clusterManifest2, metadata2);
        mockBlobContainer(blobContainer2, clusterManifest2, indexMetadataMap2, MANIFEST_CURRENT_CODEC_VERSION);

        // differGlobalMetadata controls which one of IndexMetadata or Metadata object would be different
        // when comparing cluster-uuid3 and cluster-uuid1 state.
        // if set true, only Metadata will differ b/w cluster uuid1 and cluster uuid3.
        // If set to false, only IndexMetadata would be different
        // Adding difference in EXACTLY on of these randomly will help us test if our uuid trimming logic compares both
        // IndexMetadata and Metadata when deciding if the remote state b/w two different cluster uuids is same.
        List<UploadedIndexMetadata> uploadedIndexMetadataList3 = differGlobalMetadata
            ? new ArrayList<>(uploadedIndexMetadataList1)
            : List.of(new UploadedIndexMetadata("index1", "index-uuid1", "key1__2"));
        IndexMetadata indexMetadata5 = IndexMetadata.builder("index1")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        Map<String, IndexMetadata> indexMetadataMap3 = differGlobalMetadata
            ? new HashMap<>(indexMetadataMap1)
            : Map.of("index-uuid1", indexMetadata5);
        Metadata metadata3 = Metadata.builder()
            .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), !differGlobalMetadata).build())
            .build();
        final ClusterMetadataManifest clusterManifest3 = generateClusterMetadataManifest(
            "cluster-uuid3",
            clusterUUIDsPointers.get("cluster-uuid3"),
            randomAlphaOfLength(10),
            uploadedIndexMetadataList3,
            customMetadataMap,
            new UploadedMetadataAttribute(COORDINATION_METADATA, "key3"),
            new UploadedMetadataAttribute(SETTING_METADATA, "key4"),
            new UploadedMetadataAttribute(TEMPLATES_METADATA, "key5"),
            clusterUUIDCommitted.getOrDefault("cluster-uuid3", true)
        );
        mockBlobContainerForGlobalMetadata(blobContainer3, clusterManifest3, metadata3);
        mockBlobContainer(blobContainer3, clusterManifest3, indexMetadataMap3, MANIFEST_CURRENT_CODEC_VERSION);

        ArrayList<BlobContainer> mockBlobContainerOrderedList = new ArrayList<>(
            List.of(blobContainer1, blobContainer1, blobContainer3, blobContainer3, blobContainer2, blobContainer2)
        );

        if (differGlobalMetadata) {
            mockBlobContainerOrderedList.addAll(
                List.of(
                    blobContainer3,
                    blobContainer1,
                    blobContainer3,
                    blobContainer1,
                    blobContainer1,
                    blobContainer1,
                    blobContainer1,
                    blobContainer3,
                    blobContainer3,
                    blobContainer3
                )
            );
        }
        mockBlobContainerOrderedList.addAll(
            List.of(
                blobContainer2,
                blobContainer1,
                blobContainer2,
                blobContainer1,
                blobContainer1,
                blobContainer1,
                blobContainer1,
                blobContainer2,
                blobContainer2,
                blobContainer2
            )
        );
        BlobContainer[] mockBlobContainerOrderedArray = new BlobContainer[mockBlobContainerOrderedList.size()];
        mockBlobContainerOrderedList.toArray(mockBlobContainerOrderedArray);
        when(blobStore.blobContainer(ArgumentMatchers.any())).thenReturn(uuidBlobContainer, mockBlobContainerOrderedArray);
        when(blobStoreRepository.getCompressor()).thenReturn(compressor);
    }

    private ClusterMetadataManifest generateV1ClusterMetadataManifest(
        String clusterUUID,
        String previousClusterUUID,
        String stateUUID,
        List<UploadedIndexMetadata> uploadedIndexMetadata,
        String globalMetadataFileName,
        Boolean isUUIDCommitted
    ) {
        return ClusterMetadataManifest.builder()
            .indices(uploadedIndexMetadata)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID(stateUUID)
            .clusterUUID(clusterUUID)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID(previousClusterUUID)
            .committed(true)
            .clusterUUIDCommitted(isUUIDCommitted)
            .globalMetadataFileName(globalMetadataFileName)
            .codecVersion(CODEC_V1)
            .build();
    }

    private ClusterMetadataManifest generateClusterMetadataManifest(
        String clusterUUID,
        String previousClusterUUID,
        String stateUUID,
        List<UploadedIndexMetadata> uploadedIndexMetadata,
        Map<String, UploadedMetadataAttribute> customMetadataMap,
        UploadedMetadataAttribute coordinationMetadata,
        UploadedMetadataAttribute settingsMetadata,
        UploadedMetadataAttribute templatesMetadata,
        Boolean isUUIDCommitted
    ) {
        return ClusterMetadataManifest.builder()
            .indices(uploadedIndexMetadata)
            .clusterTerm(1L)
            .stateVersion(1L)
            .stateUUID(stateUUID)
            .clusterUUID(clusterUUID)
            .nodeId("nodeA")
            .opensearchVersion(VersionUtils.randomOpenSearchVersion(random()))
            .previousClusterUUID(previousClusterUUID)
            .committed(true)
            .clusterUUIDCommitted(isUUIDCommitted)
            .coordinationMetadata(coordinationMetadata)
            .settingMetadata(settingsMetadata)
            .templatesMetadata(templatesMetadata)
            .customMetadataMap(customMetadataMap)
            .codecVersion(MANIFEST_CURRENT_CODEC_VERSION)
            .build();
    }

    private BlobContainer mockBlobStoreObjects() {
        return mockBlobStoreObjects(BlobContainer.class);
    }

    private BlobContainer mockBlobStoreObjects(Class<? extends BlobContainer> blobContainerClazz) {
        final BlobPath blobPath = mock(BlobPath.class);
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        when(blobPath.add(anyString())).thenReturn(blobPath);
        when(blobPath.iterator()).thenReturn(new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public String next() {
                return null;
            }
        });
        when(blobPath.buildAsString()).thenReturn("/blob/path/");
        final BlobContainer blobContainer = mock(blobContainerClazz);
        when(blobContainer.path()).thenReturn(blobPath);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        when(blobStoreRepository.getCompressor()).thenReturn(new DeflateCompressor());
        return blobContainer;
    }

    private void mockBlobContainerForClusterUUIDs(BlobContainer blobContainer, Set<String> clusterUUIDs) throws IOException {
        Map<String, BlobContainer> blobContainerMap = new HashMap<>();
        for (String clusterUUID : clusterUUIDs) {
            blobContainerMap.put(clusterUUID, mockBlobStoreObjects());
        }
        when(blobContainer.children()).thenReturn(blobContainerMap);
    }

    private void mockBlobContainer(
        BlobContainer blobContainer,
        ClusterMetadataManifest clusterMetadataManifest,
        Map<String, IndexMetadata> indexMetadataMap
    ) throws IOException {
        mockBlobContainer(blobContainer, clusterMetadataManifest, indexMetadataMap, ClusterMetadataManifest.CODEC_V0);
    }

    private void mockBlobContainer(
        BlobContainer blobContainer,
        ClusterMetadataManifest clusterMetadataManifest,
        Map<String, IndexMetadata> indexMetadataMap,
        int codecVersion
    ) throws IOException {
        String manifestFileName = codecVersion >= CODEC_V1
            ? "manifest__manifestFileName__abcd__abcd__abcd__" + codecVersion
            : "manifestFileName";
        BlobMetadata blobMetadata = new PlainBlobMetadata(manifestFileName, 1);
        when(blobContainer.listBlobsByPrefixInSortedOrder("manifest" + DELIMITER, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenReturn(Arrays.asList(blobMetadata));

        BytesReference bytes = RemoteClusterMetadataManifest.CLUSTER_METADATA_MANIFEST_FORMAT.serialize(
            clusterMetadataManifest,
            manifestFileName,
            blobStoreRepository.getCompressor(),
            FORMAT_PARAMS
        );
        when(blobContainer.readBlob(manifestFileName)).thenReturn(new ByteArrayInputStream(bytes.streamInput().readAllBytes()));

        clusterMetadataManifest.getIndices().forEach(uploadedIndexMetadata -> {
            try {
                IndexMetadata indexMetadata = indexMetadataMap.get(uploadedIndexMetadata.getIndexUUID());
                if (indexMetadata == null) {
                    return;
                }
                String fileName = uploadedIndexMetadata.getUploadedFilename();
                when(blobContainer.readBlob(getFormattedIndexFileName(fileName))).thenAnswer((invocationOnMock) -> {
                    BytesReference bytesIndexMetadata = INDEX_METADATA_FORMAT.serialize(
                        indexMetadata,
                        fileName,
                        blobStoreRepository.getCompressor(),
                        FORMAT_PARAMS
                    );
                    return new ByteArrayInputStream(bytesIndexMetadata.streamInput().readAllBytes());
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void mockBlobContainerForGlobalMetadata(
        BlobContainer blobContainer,
        ClusterMetadataManifest clusterMetadataManifest,
        Metadata metadata
    ) throws IOException {
        int codecVersion = clusterMetadataManifest.getCodecVersion();
        String mockManifestFileName = "manifest__1__2__C__456__" + codecVersion;
        BlobMetadata blobMetadata = new PlainBlobMetadata(mockManifestFileName, 1);
        when(blobContainer.listBlobsByPrefixInSortedOrder("manifest" + DELIMITER, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenReturn(Arrays.asList(blobMetadata));

        BytesReference bytes = RemoteClusterMetadataManifest.CLUSTER_METADATA_MANIFEST_FORMAT.serialize(
            clusterMetadataManifest,
            mockManifestFileName,
            blobStoreRepository.getCompressor(),
            FORMAT_PARAMS
        );
        when(blobContainer.readBlob(mockManifestFileName)).thenReturn(new ByteArrayInputStream(bytes.streamInput().readAllBytes()));
        if (codecVersion >= CODEC_V2) {
            String coordinationFileName = getFileNameFromPath(clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename());
            when(blobContainer.readBlob(COORDINATION_METADATA_FORMAT.blobName(coordinationFileName))).thenAnswer((invocationOnMock) -> {
                BytesReference bytesReference = COORDINATION_METADATA_FORMAT.serialize(
                    metadata.coordinationMetadata(),
                    coordinationFileName,
                    blobStoreRepository.getCompressor(),
                    FORMAT_PARAMS
                );
                return new ByteArrayInputStream(bytesReference.streamInput().readAllBytes());
            });

            String settingsFileName = getFileNameFromPath(clusterMetadataManifest.getSettingsMetadata().getUploadedFilename());
            when(blobContainer.readBlob(SETTINGS_METADATA_FORMAT.blobName(settingsFileName))).thenAnswer((invocationOnMock) -> {
                BytesReference bytesReference = SETTINGS_METADATA_FORMAT.serialize(
                    metadata.persistentSettings(),
                    settingsFileName,
                    blobStoreRepository.getCompressor(),
                    FORMAT_PARAMS
                );
                return new ByteArrayInputStream(bytesReference.streamInput().readAllBytes());
            });

            String templatesFileName = getFileNameFromPath(clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename());
            when(blobContainer.readBlob(TEMPLATES_METADATA_FORMAT.blobName(templatesFileName))).thenAnswer((invocationOnMock) -> {
                BytesReference bytesReference = TEMPLATES_METADATA_FORMAT.serialize(
                    metadata.templatesMetadata(),
                    templatesFileName,
                    blobStoreRepository.getCompressor(),
                    FORMAT_PARAMS
                );
                return new ByteArrayInputStream(bytesReference.streamInput().readAllBytes());
            });

            Map<String, String> customFileMap = clusterMetadataManifest.getCustomMetadataMap()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> getFileNameFromPath(entry.getValue().getUploadedFilename())));

            ChecksumWritableBlobStoreFormat<Metadata.Custom> customMetadataFormat = new ChecksumWritableBlobStoreFormat<>("custom", null);
            for (Map.Entry<String, String> entry : customFileMap.entrySet()) {
                String custom = entry.getKey();
                String fileName = entry.getValue();
                when(blobContainer.readBlob(fileName)).thenAnswer((invocation) -> {
                    BytesReference bytesReference = customMetadataFormat.serialize(
                        metadata.custom(custom),
                        fileName,
                        blobStoreRepository.getCompressor()
                    );
                    return new ByteArrayInputStream(bytesReference.streamInput().readAllBytes());
                });
            }
        } else if (codecVersion == CODEC_V1) {
            String[] splitPath = clusterMetadataManifest.getGlobalMetadataFileName().split("/");
            when(blobContainer.readBlob(GLOBAL_METADATA_FORMAT.blobName(splitPath[splitPath.length - 1]))).thenAnswer(
                (invocationOnMock) -> {
                    BytesReference bytesGlobalMetadata = GLOBAL_METADATA_FORMAT.serialize(
                        metadata,
                        "global-metadata-file",
                        blobStoreRepository.getCompressor(),
                        FORMAT_PARAMS
                    );
                    return new ByteArrayInputStream(bytesGlobalMetadata.streamInput().readAllBytes());
                }
            );
        }
    }

    private String getFileNameFromPath(String filePath) {
        String[] splitPath = filePath.split("/");
        return splitPath[splitPath.length - 1];
    }

    static ClusterState.Builder generateClusterStateWithGlobalMetadata() {
        final Settings clusterSettings = Settings.builder().put("cluster.blocks.read_only", true).build();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .version(1L)
            .stateUUID("state-uuid")
            .metadata(
                Metadata.builder()
                    .persistentSettings(clusterSettings)
                    .clusterUUID("cluster-uuid")
                    .coordinationMetadata(coordinationMetadata)
                    .build()
            );
    }

    static ClusterState.Builder generateClusterStateWithOneIndex() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
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
        final CustomMetadata1 customMetadata1 = new CustomMetadata1("custom-metadata-1");
        return ClusterState.builder(ClusterName.DEFAULT)
            .version(1L)
            .stateUUID("state-uuid")
            .metadata(
                Metadata.builder()
                    .version(randomNonNegativeLong())
                    .put(indexMetadata, true)
                    .clusterUUID("cluster-uuid")
                    .coordinationMetadata(coordinationMetadata)
                    .persistentSettings(settings)
                    .templates(templatesMetadata)
                    .hashesOfConsistentSettings(Map.of("key1", "value1", "key2", "value2"))
                    .putCustom(customMetadata1.getWriteableName(), customMetadata1)
                    .build()
            )
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).version(1L).build());
    }

    static ClusterState.Builder generateClusterStateWithAllAttributes() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final Settings settings = Settings.builder().put("mock-settings", true).build();
        final Settings transientSettings = Settings.builder().put("mock-transient-settings", true).build();
        final DiffableStringMap hashesOfConsistentSettings = new DiffableStringMap(emptyMap());
        final TemplatesMetadata templatesMetadata = TemplatesMetadata.builder()
            .put(IndexTemplateMetadata.builder("template-1").patterns(List.of("test-index* ")).build())
            .build();
        final CustomMetadata1 customMetadata1 = new CustomMetadata1("custom-metadata-1");
        final CustomMetadata2 customMetadata2 = new CustomMetadata2("custom-metadata-2");
        final DiscoveryNodes nodes = nodesWithLocalNodeClusterManager();
        final ClusterBlocks clusterBlocks = randomClusterBlocks();
        final TestClusterStateCustom1 custom1 = new RemoteClusterStateTestUtils.TestClusterStateCustom1("custom-1");
        final TestClusterStateCustom2 custom2 = new RemoteClusterStateTestUtils.TestClusterStateCustom2("custom-2");
        return ClusterState.builder(ClusterName.DEFAULT)
            .version(1L)
            .stateUUID("state-uuid")
            .metadata(
                Metadata.builder()
                    .version(randomNonNegativeLong())
                    .put(indexMetadata, true)
                    .clusterUUID("cluster-uuid")
                    .coordinationMetadata(coordinationMetadata)
                    .persistentSettings(settings)
                    .transientSettings(transientSettings)
                    .hashesOfConsistentSettings(hashesOfConsistentSettings)
                    .templates(templatesMetadata)
                    .putCustom(customMetadata1.getWriteableName(), customMetadata1)
                    .putCustom(customMetadata2.getWriteableName(), customMetadata2)
                    .build()
            )
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).version(1L).build())
            .nodes(nodes)
            .blocks(clusterBlocks)
            .putCustom(custom1.getWriteableName(), custom1)
            .putCustom(custom2.getWriteableName(), custom2);
    }

    static ClusterMetadataManifest.Builder generateClusterMetadataManifestWithAllAttributes() {
        return ClusterMetadataManifest.builder()
            .codecVersion(CODEC_V2)
            .clusterUUID("cluster-uuid")
            .indices(List.of(new UploadedIndexMetadata("test-index", "test-index-uuid", "test-index-file__2")))
            .customMetadataMap(
                Map.of(
                    "custom_md_1",
                    new UploadedMetadataAttribute("custom_md_1", "test-custom1-file__1"),
                    "custom_md_2",
                    new UploadedMetadataAttribute("custom_md_2", "test-custom2-file__1")
                )
            )
            .coordinationMetadata(new UploadedMetadataAttribute(COORDINATION_METADATA, COORDINATION_METADATA_FILENAME))
            .settingMetadata(new UploadedMetadataAttribute(SETTING_METADATA, PERSISTENT_SETTINGS_FILENAME))
            .transientSettingsMetadata(new UploadedMetadataAttribute(TRANSIENT_SETTING_METADATA, TRANSIENT_SETTINGS_FILENAME))
            .templatesMetadata(new UploadedMetadataAttribute(TEMPLATES_METADATA, TEMPLATES_METADATA_FILENAME))
            .hashesOfConsistentSettings(
                new UploadedMetadataAttribute(HASHES_OF_CONSISTENT_SETTINGS, HASHES_OF_CONSISTENT_SETTINGS_FILENAME)
            )
            .discoveryNodesMetadata(new UploadedMetadataAttribute(DISCOVERY_NODES, DISCOVERY_NODES_FILENAME))
            .clusterBlocksMetadata(new UploadedMetadataAttribute(CLUSTER_BLOCKS, CLUSTER_BLOCKS_FILENAME))
            .clusterStateCustomMetadataMap(
                Map.of(
                    "custom_1",
                    new UploadedMetadataAttribute("custom_1", "test-cluster-state-custom1-file__1"),
                    "custom_2",
                    new UploadedMetadataAttribute("custom_2", "test-cluster-state-custom2-file__1")
                )
            );
    }

    static DiscoveryNodes nodesWithLocalNodeClusterManager() {
        final DiscoveryNode localNode = new DiscoveryNode("cluster-manager-id", buildNewFakeTransportAddress(), Version.CURRENT);
        return DiscoveryNodes.builder().clusterManagerNodeId("cluster-manager-id").localNodeId("cluster-manager-id").add(localNode).build();
    }

    private class BlobNameMatcher implements ArgumentMatcher<AbstractRemoteWritableBlobEntity> {
        private final String expectedBlobName;

        BlobNameMatcher(String expectedBlobName) {
            this.expectedBlobName = expectedBlobName;
        }

        @Override
        public boolean matches(AbstractRemoteWritableBlobEntity argument) {
            return argument != null && expectedBlobName.equals(argument.getFullBlobName());
        }

        @Override
        public String toString() {
            return "BlobNameMatcher[Expected blobName: " + expectedBlobName + "]";
        }
    }
}
