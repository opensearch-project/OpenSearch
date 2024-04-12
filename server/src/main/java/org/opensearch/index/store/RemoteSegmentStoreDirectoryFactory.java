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
import org.opensearch.index.remote.RemoteStorePathStrategy;
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

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;

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
        String dataRepositoryName = indexSettings.getRemoteSegmentStoreDataRepository();
        String metadataRepositoryName = indexSettings.getRemoteSegmentStoreMetadataRepository();
        String indexUUID = indexSettings.getIndex().getUUID();
        return newDirectory(
            dataRepositoryName,
            metadataRepositoryName,
            indexUUID,
            path.getShardId(),
            indexSettings.getRemoteStorePathStrategy()
        );
    }

    public Directory newDirectory(
        String dataRepositoryName,
        String metadataRepositoryName,
        String indexUUID,
        ShardId shardId,
        RemoteStorePathStrategy pathStrategy
    ) throws IOException {
        assert Objects.nonNull(pathStrategy);
        if (metadataRepositoryName == null) {
            metadataRepositoryName = dataRepositoryName;
        }
        try (
            Repository dataRepository = repositoriesService.get().repository(dataRepositoryName);
            Repository metadataRepository = repositoriesService.get().repository(metadataRepositoryName)
        ) {
            assert dataRepository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobStoreRepository blobStoreDataRepository = ((BlobStoreRepository) dataRepository);
            BlobPath dataRepositoryBasePath = blobStoreDataRepository.basePath();
            String shardIdStr = String.valueOf(shardId.id());

            RemoteStorePathStrategy.PathInput dataPathInput = RemoteStorePathStrategy.PathInput.builder()
                .basePath(dataRepositoryBasePath)
                .indexUUID(indexUUID)
                .shardId(shardIdStr)
                .dataCategory(SEGMENTS)
                .dataType(DATA)
                .build();
            // Derive the path for data directory of SEGMENTS
            BlobPath dataPath = pathStrategy.generatePath(dataPathInput);
            RemoteDirectory dataDirectory = new RemoteDirectory(
                blobStoreDataRepository.blobStore().blobContainer(dataPath),
                blobStoreDataRepository::maybeRateLimitRemoteUploadTransfers,
                blobStoreDataRepository::maybeRateLimitRemoteDownloadTransfers
            );

            BlobStoreRepository blobStoreMetadataRepository = ((BlobStoreRepository) metadataRepository);
            BlobPath metadataRepositoryBasePath = blobStoreMetadataRepository.basePath();
            RemoteStorePathStrategy.PathInput mdPathInput = RemoteStorePathStrategy.PathInput.builder()
                .basePath(metadataRepositoryBasePath)
                .indexUUID(indexUUID)
                .shardId(shardIdStr)
                .dataCategory(SEGMENTS)
                .dataType(METADATA)
                .build();
            // Derive the path for metadata directory of SEGMENTS
            BlobPath mdPath = pathStrategy.generatePath(mdPathInput);
            RemoteDirectory metadataDirectory = new RemoteDirectory(blobStoreMetadataRepository.blobStore().blobContainer(mdPath));

            // The path for lock is derived within the RemoteStoreLockManagerFactory
            RemoteStoreLockManager mdLockManager = RemoteStoreLockManagerFactory.newLockManager(
                repositoriesService.get(),
                metadataRepositoryName,
                indexUUID,
                shardIdStr,
                pathStrategy
            );

            return new RemoteSegmentStoreDirectory(dataDirectory, metadataDirectory, mdLockManager, threadPool, shardId);
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", e);
        }
    }

}
