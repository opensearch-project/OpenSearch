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
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;

/**
 * Factory for a remote store directory
 *
 * @opensearch.internal
 */
public class RemoteDirectoryFactory implements IndexStorePlugin.RemoteDirectoryFactory {

    private final RepositoriesService repositoriesService;

    public RemoteDirectoryFactory(RepositoriesService repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    @Override
    public Directory newDirectory(String repositoryName, IndexSettings indexSettings, ShardPath path) throws IOException {
        try {
            Repository repository = repositoriesService.repository(repositoryName);
            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobPath blobPath = new BlobPath();
            blobPath = blobPath.add(indexSettings.getIndex().getName()).add(String.valueOf(path.getShardId().getId()));
            BlobContainer blobContainer = ((BlobStoreRepository) repository).blobStore().blobContainer(blobPath);
            return new RemoteDirectory(blobContainer);
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", e);
        }
    }

}
