/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Factory for a remote store directory
 *
 * @opensearch.internal
 */
public class RemoteSegmentStoreDirectoryFactory implements IndexStorePlugin.RemoteDirectoryFactory {

    private final Supplier<RepositoriesService> repositoriesService;

    public RemoteSegmentStoreDirectoryFactory(Supplier<RepositoriesService> repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    @Override
    public Directory newDirectory(String repositoryName, IndexSettings indexSettings, ShardPath path) throws IOException {
        String indexUUID = indexSettings.getIndex().getUUID();
        String shardId = String.valueOf(path.getShardId().getId());
        return newDirectory(repositoryName, indexUUID, shardId);
    }

    public Directory newDirectory(String repositoryName, String indexUUID, String shardId) throws IOException {
        try (Repository repository = repositoriesService.get().repository(repositoryName)) {
            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobPath commonBlobPath = ((BlobStoreRepository) repository).basePath();
            BlobPath indexLevelBlobPath = ((BlobStoreRepository) repository).basePath().add(indexUUID);
            BlobPath shardLevelBlobPath = indexLevelBlobPath.add(shardId)
                .add("segments");

            RemoteDirectory dataDirectory = createRemoteDirectory(repository, shardLevelBlobPath, "data");
            RemoteDirectory metadataDirectory = createRemoteDirectory(repository, shardLevelBlobPath,
                "metadata");
            RemoteDirectory remoteLockFilesDirectory = createRemoteDirectory(repository, shardLevelBlobPath,
                "lock_files");
            RemoteDirectory lockAcquiringResourceDirectory = createRemoteDirectory(repository, indexLevelBlobPath,
                "lock_acquiring_resources");

            return new RemoteSegmentStoreDirectory(dataDirectory, metadataDirectory, remoteLockFilesDirectory,
                lockAcquiringResourceDirectory);
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
