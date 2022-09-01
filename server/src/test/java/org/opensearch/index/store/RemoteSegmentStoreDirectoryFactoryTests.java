/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class RemoteSegmentStoreDirectoryFactoryTests extends OpenSearchTestCase {

    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private RepositoriesService repositoriesService;
    private RemoteSegmentStoreDirectoryFactory remoteSegmentStoreDirectoryFactory;

    @Before
    public void setup() {
        repositoriesServiceSupplier = mock(Supplier.class);
        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        remoteSegmentStoreDirectoryFactory = new RemoteSegmentStoreDirectoryFactory(repositoriesServiceSupplier);
    }

    public void testNewDirectory() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_INDEX_UUID, "uuid_1").build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        Path tempDir = createTempDir().resolve(indexSettings.getUUID()).resolve("0");
        ShardPath shardPath = new ShardPath(false, tempDir, tempDir, new ShardId(indexSettings.getIndex(), 0));
        BlobStoreRepository repository = mock(BlobStoreRepository.class);
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(repository.blobStore()).thenReturn(blobStore);
        when(repository.basePath()).thenReturn(new BlobPath().add("base_path"));
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        when(blobContainer.listBlobs()).thenReturn(Collections.emptyMap());

        when(repositoriesService.repository("remote_store_repository")).thenReturn(repository);

        try (Directory directory = remoteSegmentStoreDirectoryFactory.newDirectory("remote_store_repository", indexSettings, shardPath)) {
            assertTrue(directory instanceof RemoteSegmentStoreDirectory);
            ArgumentCaptor<BlobPath> blobPathCaptor = ArgumentCaptor.forClass(BlobPath.class);
            verify(blobStore, times(2)).blobContainer(blobPathCaptor.capture());
            List<BlobPath> blobPaths = blobPathCaptor.getAllValues();
            assertEquals("base_path/uuid_1/0/segments/data/", blobPaths.get(0).buildAsString());
            assertEquals("base_path/uuid_1/0/segments/metadata/", blobPaths.get(1).buildAsString());

            verify(blobContainer).listBlobsByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX);
            verify(repositoriesService).repository("remote_store_repository");
        }
    }

    public void testNewDirectoryRepositoryDoesNotExist() {
        Settings settings = Settings.builder().build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        Path tempDir = createTempDir().resolve(indexSettings.getUUID()).resolve("0");
        ShardPath shardPath = new ShardPath(false, tempDir, tempDir, new ShardId(indexSettings.getIndex(), 0));

        when(repositoriesService.repository("remote_store_repository")).thenThrow(new RepositoryMissingException("Missing"));

        assertThrows(
            IllegalArgumentException.class,
            () -> remoteSegmentStoreDirectoryFactory.newDirectory("remote_store_repository", indexSettings, shardPath)
        );
    }

}
