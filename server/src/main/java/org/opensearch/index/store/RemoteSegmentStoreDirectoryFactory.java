/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteStoreUtils;
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
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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
    private final String segmentsPathFixedPrefix;

    private final ThreadPool threadPool;

    public RemoteSegmentStoreDirectoryFactory(
        Supplier<RepositoriesService> repositoriesService,
        ThreadPool threadPool,
        String segmentsPathFixedPrefix
    ) {
        this.repositoriesService = repositoriesService;
        this.segmentsPathFixedPrefix = segmentsPathFixedPrefix;
        this.threadPool = threadPool;
    }

    @Override
    public Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
        return null;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        String repositoryName = indexSettings.getRemoteStoreRepository();
        String indexUUID = indexSettings.getIndex().getUUID();
        return newDirectory(
            repositoryName,
            indexUUID,
            path.getShardId(),
            indexSettings.getRemoteStorePathStrategy(),
            null,
            RemoteStoreUtils.isServerSideEncryptionEnabledIndex(indexSettings.getIndexMetadata()),
            indexSettings.isWarmIndex()
        );
    }

    public Directory newDirectory(String repositoryName, String indexUUID, ShardId shardId, RemoteStorePathStrategy pathStrategy)
        throws IOException {
        return newDirectory(repositoryName, indexUUID, shardId, pathStrategy, null, false);
    }

    public Directory newDirectory(
        String repositoryName,
        String indexUUID,
        ShardId shardId,
        RemoteStorePathStrategy pathStrategy,
        String indexFixedPrefix
    ) throws IOException {
        return newDirectory(repositoryName, indexUUID, shardId, pathStrategy, indexFixedPrefix, false);
    }

    public Directory newDirectory(
        String repositoryName,
        String indexUUID,
        ShardId shardId,
        RemoteStorePathStrategy pathStrategy,
        String indexFixedPrefix,
        boolean isServerSideEncryptionEnabled
    ) throws IOException {
        return newDirectory(repositoryName, indexUUID, shardId, pathStrategy, indexFixedPrefix, isServerSideEncryptionEnabled, false);
    }

    public Directory newDirectory(
        String repositoryName,
        String indexUUID,
        ShardId shardId,
        RemoteStorePathStrategy pathStrategy,
        String indexFixedPrefix,
        boolean isServerSideEncryptionEnabled,
        boolean isWarmIndex
    ) throws IOException {
        assert Objects.nonNull(pathStrategy);
        // We should be not calling close for repository.
        Repository repository = repositoriesService.get().repository(repositoryName);
        try {
            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobStoreRepository blobStoreRepository = ((BlobStoreRepository) repository);
            BlobPath repositoryBasePath = blobStoreRepository.basePath();
            String shardIdStr = String.valueOf(shardId.id());
            Map<String, String> pendingDownloadMergedSegments = new ConcurrentHashMap<>();

            RemoteStorePathStrategy.ShardDataPathInput dataPathInput = RemoteStorePathStrategy.ShardDataPathInput.builder()
                .basePath(repositoryBasePath)
                .indexUUID(indexUUID)
                .shardId(shardIdStr)
                .dataCategory(SEGMENTS)
                .dataType(DATA)
                .fixedPrefix(segmentsPathFixedPrefix)
                .indexFixedPrefix(indexFixedPrefix)
                .build();

            // Derive the path for data directory of SEGMENTS
            BlobPath dataPath = pathStrategy.generatePath(dataPathInput);
            RemoteDirectory dataDirectory = new RemoteDirectory(
                blobStoreRepository.blobStore(isServerSideEncryptionEnabled).blobContainer(dataPath),
                blobStoreRepository::maybeRateLimitRemoteUploadTransfers,
                blobStoreRepository::maybeRateLimitLowPriorityRemoteUploadTransfers,
                isWarmIndex
                    ? blobStoreRepository::maybeRateLimitRemoteDownloadTransfersForWarm
                    : blobStoreRepository::maybeRateLimitRemoteDownloadTransfers,
                blobStoreRepository::maybeRateLimitLowPriorityDownloadTransfers,
                pendingDownloadMergedSegments
            );

            RemoteStorePathStrategy.ShardDataPathInput mdPathInput = RemoteStorePathStrategy.ShardDataPathInput.builder()
                .basePath(repositoryBasePath)
                .indexUUID(indexUUID)
                .shardId(shardIdStr)
                .dataCategory(SEGMENTS)
                .dataType(METADATA)
                .fixedPrefix(segmentsPathFixedPrefix)
                .indexFixedPrefix(indexFixedPrefix)
                .build();
            // Derive the path for metadata directory of SEGMENTS
            BlobPath mdPath = pathStrategy.generatePath(mdPathInput);
            RemoteDirectory metadataDirectory = new RemoteDirectory(
                blobStoreRepository.blobStore(isServerSideEncryptionEnabled).blobContainer(mdPath)
            );

            // The path for lock is derived within the RemoteStoreLockManagerFactory
            RemoteStoreLockManager mdLockManager = RemoteStoreLockManagerFactory.newLockManager(
                repositoriesService.get(),
                repositoryName,
                indexUUID,
                shardIdStr,
                pathStrategy,
                segmentsPathFixedPrefix,
                indexFixedPrefix
            );

            return new RemoteSegmentStoreDirectory(
                dataDirectory,
                metadataDirectory,
                mdLockManager,
                threadPool,
                shardId,
                pendingDownloadMergedSegments
            );
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", e);
        }
    }

    public Supplier<RepositoriesService> getRepositoriesService() {
        return this.repositoriesService;
    }

}
