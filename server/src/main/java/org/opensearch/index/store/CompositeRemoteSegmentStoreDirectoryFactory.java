/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManagerFactory;
import org.opensearch.index.store.remote.CompositeRemoteDirectory;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;

/**
 * Factory for composite remote segment store directory.
 *
 * @opensearch.internal
 */
public class CompositeRemoteSegmentStoreDirectoryFactory implements IndexStorePlugin.DirectoryFactory {
    private final Supplier<RepositoriesService> repositoriesService;
    private final String segmentsPathFixedPrefix;
    private final ThreadPool threadPool;
    private final PluginsService pluginsService;

    public CompositeRemoteSegmentStoreDirectoryFactory(
        Supplier<RepositoriesService> repositoriesService,
        ThreadPool threadPool,
        String segmentsPathFixedPrefix
    ) {
        this(repositoriesService, threadPool, segmentsPathFixedPrefix, null);
    }

    public CompositeRemoteSegmentStoreDirectoryFactory(
        Supplier<RepositoriesService> repositoriesService,
        ThreadPool threadPool,
        String segmentsPathFixedPrefix,
        PluginsService pluginsService
    ) {
        this.repositoriesService = repositoriesService;
        this.segmentsPathFixedPrefix = segmentsPathFixedPrefix;
        this.threadPool = threadPool;
        this.pluginsService = pluginsService;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        String repositoryName = indexSettings.getRemoteStoreRepository();
        String indexUUID = indexSettings.getIndex().getUUID();
        return newDirectory(repositoryName, indexUUID, path.getShardId(), indexSettings.getRemoteStorePathStrategy());
    }

    public Directory newDirectory(String repositoryName, String indexUUID, ShardId shardId, RemoteStorePathStrategy pathStrategy)
        throws IOException {
        return newDirectory(repositoryName, indexUUID, shardId, pathStrategy, null);
    }

    public Directory newDirectory(
        String repositoryName,
        String indexUUID,
        ShardId shardId,
        RemoteStorePathStrategy pathStrategy,
        String indexFixedPrefix
    ) throws IOException {
        assert Objects.nonNull(pathStrategy);
        try (Repository repository = repositoriesService.get().repository(repositoryName)) {

            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobStoreRepository blobStoreRepository = ((BlobStoreRepository) repository);
            BlobPath repositoryBasePath = blobStoreRepository.basePath();
            String shardIdStr = String.valueOf(shardId.id());
            Map<org.opensearch.index.engine.exec.FileMetadata, String> pendingDownloadMergedSegments = new ConcurrentHashMap<>();

            RemoteStorePathStrategy.ShardDataPathInput dataPathInput = RemoteStorePathStrategy.ShardDataPathInput.builder()
                .basePath(repositoryBasePath)
                .indexUUID(indexUUID)
                .shardId(shardIdStr)
                .dataCategory(SEGMENTS)
                .dataType(DATA)
                .fixedPrefix(segmentsPathFixedPrefix)
                .indexFixedPrefix(indexFixedPrefix)
                .build();

            BlobPath dataPath = pathStrategy.generatePath(dataPathInput);

            CompositeRemoteDirectory compositeDataDirectory = new CompositeRemoteDirectory(
                blobStoreRepository.blobStore(),
                dataPath,
                blobStoreRepository::maybeRateLimitRemoteUploadTransfers,
                blobStoreRepository::maybeRateLimitLowPriorityRemoteUploadTransfers,
                blobStoreRepository::maybeRateLimitRemoteDownloadTransfers,
                blobStoreRepository::maybeRateLimitLowPriorityDownloadTransfers,
                pendingDownloadMergedSegments,
                LogManager.getLogger("index.store.remote.composite." + shardId),
                pluginsService
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

            BlobPath mdPath = pathStrategy.generatePath(mdPathInput);
            RemoteDirectory metadataDirectory = new RemoteDirectory(blobStoreRepository.blobStore().blobContainer(mdPath));

            RemoteStoreLockManager mdLockManager = RemoteStoreLockManagerFactory.newLockManager(
                repositoriesService.get(),
                repositoryName,
                indexUUID,
                shardIdStr,
                pathStrategy,
                segmentsPathFixedPrefix,
                indexFixedPrefix
            );

            return new CompositeRemoteSegmentStoreDirectory(
                compositeDataDirectory,
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
