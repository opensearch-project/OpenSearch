/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteStorePathType;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManagerFactory;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

import static org.opensearch.index.remote.RemoteStoreDataEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreDataEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreDataEnums.DataType.METADATA;

/**
 * Factory for a remote store directory
 *
 * @opensearch.api
 */
@PublicApi(since = "2.3.0")
public class RemoteSegmentStoreDirectoryFactory implements IndexStorePlugin.DirectoryFactory {
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
        return newDirectory(repositoryName, indexUUID, path.getShardId(), indexSettings.getRemoteStorePathType());
    }

    public Directory newDirectory(String repositoryName, String indexUUID, ShardId shardId, RemoteStorePathType pathType)
        throws IOException {
        assert Objects.nonNull(pathType);
        try (Repository repository = repositoriesService.get().repository(repositoryName)) {

            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobStoreRepository blobStoreRepository = ((BlobStoreRepository) repository);
            BlobPath repositoryBasePath = blobStoreRepository.basePath();
            String shardIdStr = String.valueOf(shardId.id());

            // Derive the path for data directory of SEGMENTS
            BlobPath dataPath = pathType.path(repositoryBasePath, indexUUID, shardIdStr, SEGMENTS, DATA);
            RemoteDirectory dataDirectory = new RemoteDirectory(
                blobStoreRepository.blobStore().blobContainer(dataPath),
                blobStoreRepository::maybeRateLimitRemoteUploadTransfers,
                blobStoreRepository::maybeRateLimitRemoteDownloadTransfers
            );

            // Derive the path for metadata directory of SEGMENTS
            BlobPath mdPath = pathType.path(repositoryBasePath, indexUUID, shardIdStr, SEGMENTS, METADATA);
            RemoteDirectory metadataDirectory = new RemoteDirectory(blobStoreRepository.blobStore().blobContainer(mdPath));

            // The path for lock is derived within the RemoteStoreLockManagerFactory
            RemoteStoreLockManager mdLockManager = RemoteStoreLockManagerFactory.newLockManager(
                repositoriesService.get(),
                repositoryName,
                indexUUID,
                shardIdStr,
                pathType
            );

            return new RemoteSegmentStoreDirectory(dataDirectory, metadataDirectory, mdLockManager, threadPool, shardId);
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", e);
        }
    }

}
