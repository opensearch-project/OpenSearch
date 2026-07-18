/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.mockito.Mockito;

import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteSegmentStoreDirectoryWithPinnedTimestampTests extends RemoteSegmentStoreDirectoryTests {

    Runnable updatePinnedTimstampTask;
    BlobContainer blobContainer;
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

        BlobStore blobStore = mock(BlobStore.class);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(blobStoreRepository.basePath()).thenReturn(new BlobPath());
        blobContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);

        when(blobContainer.listBlobs()).thenReturn(new HashMap<>());

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

        long pinnedTimestampMatchingMetadataFilename2 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getTimestamp(metadataFilename2) + 10;
        String blobName = "snapshot1__" + pinnedTimestampMatchingMetadataFilename2;
        when(blobContainer.listBlobs()).thenReturn(Map.of(blobName, new PlainBlobMetadata(blobName, 100)));

        final Map<String, Map<String, String>> metadataFilenameContentMapping = populateMetadata();
        final Set<String> filesToBeDeleted = metadataFilenameContentMapping.get(metadataFilename3)
            .values()
            .stream()
            .map(metadata -> metadata.split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1])
            .collect(Collectors.toSet());

        updatePinnedTimstampTask.run();

        remoteSegmentStoreDirectory.init();

        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here so that oldest 1 metadata file will be deleted
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(1);

        // Verify batch deletion was called with the list of files (order-independent)
        assertBusy(() -> {
            verify(remoteDataDirectory).deleteFiles(
                org.mockito.ArgumentMatchers.argThat(files -> files != null && new HashSet<>(files).equals(filesToBeDeleted))
            );
        });

        assertBusy(() -> assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(true)));
        verify(remoteMetadataDirectory).deleteFile(metadataFilename3);
        verify(remoteMetadataDirectory, times(0)).deleteFile(metadataFilename2);
    }

    public void testDeleteStaleSegmentsBatchesDeletions_cache() throws Exception {
        Map<String, Map<String, String>> metadataFilenameContentMapping = populateMetadata();
        remoteSegmentStoreDirectory.init();

        // Collect expected files to be deleted
        Set<String> expectedFilesToDelete = metadataFilenameContentMapping.get(metadataFilename2)
            .values()
            .stream()
            .map(metadata -> metadata.split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1])
            .collect(Collectors.toSet());

        // Execute deletion
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(0);

        // Verify that deleteFiles was called with the batch of files (order-independent)
        assertBusy(() -> {
            verify(remoteDataDirectory).deleteFiles(argThat(files -> files != null && new HashSet<>(files).equals(expectedFilesToDelete)));
            assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(true));
        });

        // Verify metadata file was deleted
        verify(remoteMetadataDirectory).deleteFile(metadataFilename2);
    }
}
