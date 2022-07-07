/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.SnapshotId;

/**
 * Factory for the {@link InMemoryRemoteSnapshotDirectory}. This is functional
 * but is only temporary to demonstrate functional searchable snapshot
 * functionality. The proper implementation will be implemented per
 * https://github.com/opensearch-project/OpenSearch/issues/3114.
 *
 * @opensearch.internal
 */
public final class InMemoryRemoteSnapshotDirectoryFactory implements IndexStorePlugin.DirectoryFactory {
    private final Supplier<RepositoriesService> repositoriesService;

    public InMemoryRemoteSnapshotDirectoryFactory(Supplier<RepositoriesService> repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        final String repositoryName = IndexSettings.SNAPSHOT_REPOSITORY.get(indexSettings.getSettings());
        final Repository repository = repositoriesService.get().repository(repositoryName);
        assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        final BlobPath blobPath = new BlobPath().add("indices")
            .add(IndexSettings.SNAPSHOT_INDEX_ID.get(indexSettings.getSettings()))
            .add(Integer.toString(shardPath.getShardId().getId()));
        final SnapshotId snapshotId = new SnapshotId(
            IndexSettings.SNAPSHOT_ID_NAME.get(indexSettings.getSettings()),
            IndexSettings.SNAPSHOT_ID_UUID.get(indexSettings.getSettings())
        );
        return new InMemoryRemoteSnapshotDirectory(blobStoreRepository, blobPath, snapshotId);
    }
}
