/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.store.RemoteBufferedOutputDirectory;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.util.function.Supplier;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.LOCK_FILES;

/**
 * Factory for remote store lock manager
 *
 * @opensearch.api
 */
@PublicApi(since = "2.8.0")
public class RemoteStoreLockManagerFactory {
    private final Supplier<RepositoriesService> repositoriesService;
    private final String segmentsPathFixedPrefix;

    public RemoteStoreLockManagerFactory(Supplier<RepositoriesService> repositoriesService, String segmentsPathFixedPrefix) {
        this.repositoriesService = repositoriesService;
        this.segmentsPathFixedPrefix = segmentsPathFixedPrefix;
    }

    public RemoteStoreLockManager newLockManager(
        String repositoryName,
        String indexUUID,
        String shardId,
        RemoteStorePathStrategy pathStrategy
    ) {
        return newLockManager(repositoriesService.get(), repositoryName, indexUUID, shardId, pathStrategy, segmentsPathFixedPrefix);
    }

    public static RemoteStoreMetadataLockManager newLockManager(
        RepositoriesService repositoriesService,
        String repositoryName,
        String indexUUID,
        String shardId,
        RemoteStorePathStrategy pathStrategy,
        String segmentsPathFixedPrefix
    ) {
        try (Repository repository = repositoriesService.repository(repositoryName)) {
            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobPath repositoryBasePath = ((BlobStoreRepository) repository).basePath();

            RemoteStorePathStrategy.ShardDataPathInput lockFilesPathInput = RemoteStorePathStrategy.ShardDataPathInput.builder()
                .basePath(repositoryBasePath)
                .indexUUID(indexUUID)
                .shardId(shardId)
                .dataCategory(SEGMENTS)
                .dataType(LOCK_FILES)
                .fixedPrefix(segmentsPathFixedPrefix)
                .build();
            BlobPath lockDirectoryPath = pathStrategy.generatePath(lockFilesPathInput);
            BlobContainer lockDirectoryBlobContainer = ((BlobStoreRepository) repository).blobStore().blobContainer(lockDirectoryPath);
            return new RemoteStoreMetadataLockManager(new RemoteBufferedOutputDirectory(lockDirectoryBlobContainer));
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be present to acquire/release lock", e);
        }
    }

    // TODO: remove this once we add poller in place to trigger remote store cleanup
    // see: https://github.com/opensearch-project/OpenSearch/issues/8469
    public Supplier<RepositoriesService> getRepositoriesService() {
        return repositoriesService;
    }
}
