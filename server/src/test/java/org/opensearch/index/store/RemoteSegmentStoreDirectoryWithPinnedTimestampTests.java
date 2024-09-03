/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.model.RemotePinnedTimestamps;
import org.opensearch.gateway.remote.model.RemoteStorePinnedTimestampsBlobStore;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.mockito.Mockito;

import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteSegmentStoreDirectoryWithPinnedTimestampTests extends RemoteSegmentStoreDirectoryTests {

    Runnable updatePinnedTimstampTask;
    BlobStoreTransferService blobStoreTransferService;
    RemoteStorePinnedTimestampsBlobStore remoteStorePinnedTimestampsBlobStore;
    RemoteStorePinnedTimestampService remoteStorePinnedTimestampServiceSpy;

    @Before
    public void setupPinnedTimestamp() throws IOException {
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(
            Settings.builder().put(CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), true).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        Supplier<RepositoriesService> repositoriesServiceSupplier = mock(Supplier.class);
        Settings settings = Settings.builder()
            .put(Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, "remote-repo")
            .build();
        RepositoriesService repositoriesService = mock(RepositoriesService.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        BlobStoreRepository blobStoreRepository = mock(BlobStoreRepository.class);
        when(repositoriesService.repository("remote-repo")).thenReturn(blobStoreRepository);

        when(threadPool.schedule(any(), any(), any())).then(invocationOnMock -> {
            updatePinnedTimstampTask = invocationOnMock.getArgument(0);
            updatePinnedTimstampTask.run();
            return null;
        }).then(subsequentInvocationsOnMock -> null);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = new RemoteStorePinnedTimestampService(
            repositoriesServiceSupplier,
            settings,
            threadPool,
            clusterService
        );
        remoteStorePinnedTimestampServiceSpy = Mockito.spy(remoteStorePinnedTimestampService);

        remoteStorePinnedTimestampsBlobStore = mock(RemoteStorePinnedTimestampsBlobStore.class);
        blobStoreTransferService = mock(BlobStoreTransferService.class);
        when(remoteStorePinnedTimestampServiceSpy.pinnedTimestampsBlobStore()).thenReturn(remoteStorePinnedTimestampsBlobStore);
        when(remoteStorePinnedTimestampServiceSpy.blobStoreTransferService()).thenReturn(blobStoreTransferService);

        doAnswer(invocationOnMock -> {
            ActionListener<List<BlobMetadata>> actionListener = invocationOnMock.getArgument(3);
            actionListener.onResponse(new ArrayList<>());
            return null;
        }).when(blobStoreTransferService).listAllInSortedOrder(any(), any(), eq(1), any());

        remoteStorePinnedTimestampServiceSpy.start();

        metadataWithOlderTimestamp();
    }

    private void metadataWithOlderTimestamp() {
        metadataFilename = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
            12,
            23,
            34,
            1,
            1,
            "node-1",
            System.currentTimeMillis() - 300000
        );
        metadataFilename2 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
            12,
            13,
            34,
            1,
            1,
            "node-1",
            System.currentTimeMillis() - 400000
        );
        metadataFilename3 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
            10,
            38,
            34,
            1,
            1,
            "node-1",
            System.currentTimeMillis() - 500000
        );
        metadataFilename4 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
            10,
            36,
            34,
            1,
            1,
            "node-1",
            System.currentTimeMillis() - 600000
        );
    }

    public void testDeleteStaleCommitsNoPinnedTimestampMdFilesLatest() throws Exception {
        metadataFilename = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
            12,
            23,
            34,
            1,
            1,
            "node-1",
            System.currentTimeMillis()
        );
        metadataFilename2 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
            12,
            13,
            34,
            1,
            1,
            "node-1",
            System.currentTimeMillis()
        );
        metadataFilename3 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
            10,
            38,
            34,
            1,
            1,
            "node-1",
            System.currentTimeMillis()
        );

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                eq(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX),
                anyInt()
            )
        ).thenReturn(List.of(metadataFilename, metadataFilename2, metadataFilename3));

        populateMetadata();
        remoteSegmentStoreDirectory.init();

        // populateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here so that oldest 1 metadata file will be deleted
        // But as the oldest metadata file's timestamp is within time threshold since last successful fetch,
        // GC will skip deleting any data or metadata files.
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(2);

        assertBusy(() -> assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(true)));
        verify(remoteDataDirectory, times(0)).deleteFile(any());
        verify(remoteMetadataDirectory, times(0)).deleteFile(any());
    }

    public void testDeleteStaleCommitsPinnedTimestampMdFile() throws Exception {
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                eq(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX),
                anyInt()
            )
        ).thenReturn(List.of(metadataFilename, metadataFilename2, metadataFilename3));

        doAnswer(invocationOnMock -> {
            ActionListener<List<BlobMetadata>> actionListener = invocationOnMock.getArgument(3);
            actionListener.onResponse(List.of(new PlainBlobMetadata("pinned_timestamp_123", 1000)));
            return null;
        }).when(blobStoreTransferService).listAllInSortedOrder(any(), any(), eq(1), any());

        long pinnedTimestampMatchingMetadataFilename2 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getTimestamp(metadataFilename2) + 10;
        when(remoteStorePinnedTimestampsBlobStore.read(any())).thenReturn(new RemotePinnedTimestamps.PinnedTimestamps(Map.of(pinnedTimestampMatchingMetadataFilename2, List.of("xyz"))));
        when(remoteStorePinnedTimestampsBlobStore.getBlobPathForUpload(any())).thenReturn(new BlobPath());

        final Map<String, Map<String, String>> metadataFilenameContentMapping = populateMetadata();
        final List<String> filesToBeDeleted = metadataFilenameContentMapping.get(metadataFilename3)
            .values()
            .stream()
            .map(metadata -> metadata.split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1])
            .collect(Collectors.toList());

        updatePinnedTimstampTask.run();

        remoteSegmentStoreDirectory.init();

        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here so that oldest 1 metadata file will be deleted
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(1);

        for (final String file : filesToBeDeleted) {
            verify(remoteDataDirectory).deleteFile(file);
        }
        assertBusy(() -> assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(true)));
        verify(remoteMetadataDirectory).deleteFile(metadataFilename3);
        verify(remoteMetadataDirectory, times(0)).deleteFile(metadataFilename2);
    }
}
