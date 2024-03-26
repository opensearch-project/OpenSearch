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
import org.opensearch.index.remote.RemoteStorePathType;
import org.opensearch.index.store.RemoteBufferedOutputDirectory;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.util.function.Supplier;

import static org.opensearch.index.remote.RemoteStoreDataEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreDataEnums.DataType.LOCK_FILES;

/**
 * Factory for remote store lock manager
 *
 * @opensearch.api
 */
@PublicApi(since = "2.8.0")
public class RemoteStoreLockManagerFactory {
    private final Supplier<RepositoriesService> repositoriesService;

    public RemoteStoreLockManagerFactory(Supplier<RepositoriesService> repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    public RemoteStoreLockManager newLockManager(String repositoryName, String indexUUID, String shardId, RemoteStorePathType pathType) {
        return newLockManager(repositoriesService.get(), repositoryName, indexUUID, shardId, pathType);
    }

    public static RemoteStoreMetadataLockManager newLockManager(
        RepositoriesService repositoriesService,
        String repositoryName,
        String indexUUID,
        String shardId,
        RemoteStorePathType pathType
    ) {
        try (Repository repository = repositoriesService.repository(repositoryName)) {
            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobPath repositoryBasePath = ((BlobStoreRepository) repository).basePath();
            BlobPath lockDirectoryPath = pathType.path(repositoryBasePath, indexUUID, shardId, SEGMENTS, LOCK_FILES);
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
