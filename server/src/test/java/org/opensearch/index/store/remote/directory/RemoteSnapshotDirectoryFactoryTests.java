/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.directory;

import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteSnapshotDirectoryFactoryTests extends OpenSearchTestCase {

    private static final String FILE_NAME = "_0.cfs";

    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    /**
     * The cluster applier thread calls newDirectory for every shard it starts. It must not read the repository, otherwise a node
     * with many searchable snapshot shards starts them one after the other.
     */
    public void testNewDirectoryDoesNotReadRepository() throws Exception {
        FileInfo fileInfo = new FileInfo(FILE_NAME, new StoreFileMetadata(FILE_NAME, 10, "", org.apache.lucene.util.Version.LATEST), null);
        BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot("snap", 1, Collections.singletonList(fileInfo), 0, 0, 0, 0);

        BlobStoreRepository repository = mock(BlobStoreRepository.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(repository.shardContainer(any(IndexId.class), anyInt())).thenReturn(blobContainer);
        when(repository.loadShardSnapshot(any(BlobContainer.class), any(SnapshotId.class))).thenReturn(snapshot);
        RepositoriesService repositoriesService = mock(RepositoriesService.class);
        when(repositoriesService.repository("repo")).thenReturn(repository);

        RemoteSnapshotDirectoryFactory factory = new RemoteSnapshotDirectoryFactory(
            () -> repositoriesService,
            threadPool,
            mock(FileCache.class)
        );
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.SEARCHABLE_SNAPSHOT_REPOSITORY.getKey(), "repo")
            .put(IndexSettings.SEARCHABLE_SNAPSHOT_ID_NAME.getKey(), "snap")
            .put(IndexSettings.SEARCHABLE_SNAPSHOT_ID_UUID.getKey(), "snap-uuid")
            .put(IndexSettings.SEARCHABLE_SNAPSHOT_INDEX_ID.getKey(), "index-uuid")
            .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        ShardId shardId = new ShardId(indexSettings.getIndex(), 0);
        Path shardDir = createTempDir().resolve(shardId.getIndex().getUUID()).resolve("0");
        ShardPath shardPath = new ShardPath(false, shardDir, shardDir, shardId);

        Directory directory = factory.newDirectory(indexSettings, shardPath);
        assertTrue(directory instanceof RemoteSnapshotDirectory);
        verify(repository, never()).shardContainer(any(IndexId.class), anyInt());
        verify(repository, never()).loadShardSnapshot(any(BlobContainer.class), any(SnapshotId.class));

        assertArrayEquals(new String[] { FILE_NAME }, directory.listAll());
        assertArrayEquals(new String[] { FILE_NAME }, directory.listAll());
        // the snapshot file is read once, not on every call
        verify(repository, times(1)).loadShardSnapshot(any(BlobContainer.class), any(SnapshotId.class));
        directory.close();
    }
}
