/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.directory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.threadpool.ThreadPool;

/**
 * Factory for a Directory implementation that can read directly from index
 * data stored remotely in a blob store repository.
 *
 * @opensearch.internal
 */
public final class RemoteSnapshotDirectoryFactory implements IndexStorePlugin.DirectoryFactory {
    public static final String LOCAL_STORE_LOCATION = "RemoteLocalStore";

    private final Supplier<RepositoriesService> repositoriesService;
    private final ThreadPool threadPool;

    private final FileCache remoteStoreFileCache;

    public RemoteSnapshotDirectoryFactory(
        Supplier<RepositoriesService> repositoriesService,
        ThreadPool threadPool,
        FileCache remoteStoreFileCache
    ) {
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
        this.remoteStoreFileCache = remoteStoreFileCache;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath localShardPath) throws IOException {
        final String repositoryName = IndexSettings.SEARCHABLE_SNAPSHOT_REPOSITORY.get(indexSettings.getSettings());
        final Repository repository = repositoriesService.get().repository(repositoryName);
        assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        try {
            return createRemoteSnapshotDirectoryFromSnapshot(indexSettings, localShardPath, blobStoreRepository).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private Future<RemoteSnapshotDirectory> createRemoteSnapshotDirectoryFromSnapshot(
        IndexSettings indexSettings,
        ShardPath localShardPath,
        BlobStoreRepository blobStoreRepository
    ) throws IOException {
        final BlobPath blobPath = blobStoreRepository.basePath()
            .add("indices")
            .add(IndexSettings.SEARCHABLE_SNAPSHOT_INDEX_ID.get(indexSettings.getSettings()))
            .add(Integer.toString(localShardPath.getShardId().getId()));
        final SnapshotId snapshotId = new SnapshotId(
            IndexSettings.SEARCHABLE_SNAPSHOT_ID_NAME.get(indexSettings.getSettings()),
            IndexSettings.SEARCHABLE_SNAPSHOT_ID_UUID.get(indexSettings.getSettings())
        );
        Path localStorePath = localShardPath.getDataPath().resolve(LOCAL_STORE_LOCATION);
        FSDirectory localStoreDir = FSDirectory.open(Files.createDirectories(localStorePath));
        // make sure directory is flushed to persistent storage
        localStoreDir.syncMetaData();
        // this trick is needed to bypass assertions in BlobStoreRepository::assertAllowableThreadPools in case of node restart and a remote
        // index restore is invoked
        return threadPool.executor(ThreadPool.Names.SNAPSHOT).submit(() -> {
            final BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(blobPath);
            final BlobStoreIndexShardSnapshot snapshot = blobStoreRepository.loadShardSnapshot(blobContainer, snapshotId);
            TransferManager transferManager = new TransferManager(blobContainer, remoteStoreFileCache);
            return new RemoteSnapshotDirectory(snapshot, localStoreDir, transferManager);
        });
    }
}
