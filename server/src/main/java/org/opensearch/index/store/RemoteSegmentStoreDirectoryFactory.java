/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManagerFactory;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Factory for a remote store directory
 *
 * @opensearch.internal
 */
public class RemoteSegmentStoreDirectoryFactory implements IndexStorePlugin.DirectoryFactory {
    private static final String SEGMENTS = "segments";

    private final Supplier<RepositoriesService> repositoriesService;

    private final ThreadPool threadPool;

    public RemoteSegmentStoreDirectoryFactory(Supplier<RepositoriesService> repositoriesService, ThreadPool threadPool) {
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        String repositoryName = indexSettings.getRemoteStoreRepository();
        String indexUUID = indexSettings.getIndex().getUUID();
        String shardId = String.valueOf(path.getShardId().getId());

        return newDirectory(repositoryName, indexUUID, shardId);
    }

    public Directory newDirectory(String repositoryName, String indexUUID, String shardId) throws IOException {
        try (Repository repository = repositoriesService.get().repository(repositoryName)) {
            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobPath commonBlobPath = ((BlobStoreRepository) repository).basePath();
            commonBlobPath = commonBlobPath.add(indexUUID).add(shardId).add(SEGMENTS);

            RemoteDirectory dataDirectory = createRemoteDirectory(repository, commonBlobPath, "data");
            RemoteDirectory metadataDirectory = createRemoteDirectory(repository, commonBlobPath, "metadata");
            RemoteStoreMetadataLockManager mdLockManager = RemoteStoreLockManagerFactory.newLockManager(
                repositoriesService.get(),
                repositoryName,
                indexUUID,
                shardId
            );

            return new RemoteSegmentStoreDirectory(dataDirectory, metadataDirectory, mdLockManager, threadPool);
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", e);
        }
    }

    private RemoteDirectory createRemoteDirectory(Repository repository, BlobPath commonBlobPath, String extention) {
        BlobPath extendedPath = commonBlobPath.add(extention);
        BlobContainer dataBlobContainer = ((BlobStoreRepository) repository).blobStore().blobContainer(extendedPath);
        return new RemoteDirectory(dataBlobContainer);
    }
}
