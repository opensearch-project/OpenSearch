/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class RemoteBlobStoreInternalTranslogFactory implements TranslogFactory {

    private final Supplier<RepositoriesService> repositoriesService;
    private final String repositoryName;


    public RemoteBlobStoreInternalTranslogFactory(Supplier<RepositoriesService> repositoriesService, String repositoryName) {
        this.repositoriesService = repositoriesService;
        this.repositoryName = repositoryName;
    }

    @Override
    public Translog newTranslog(TranslogConfig config, String translogUUID, TranslogDeletionPolicy deletionPolicy, LongSupplier globalCheckpointSupplier, LongSupplier primaryTermSupplier, LongConsumer persistedSequenceNumberConsumer) throws IOException {
        try (Repository repository = repositoriesService.get().repository(repositoryName)) {
            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobPath blobPath = new BlobPath();
            blobPath = blobPath.add(config.getShardId().getIndexName()).add(String.valueOf(config.getShardId().getId()));
            BlobContainer blobContainer = ((BlobStoreRepository) repository).blobStore().blobContainer(blobPath);
            return new RemoteFsTranslog(config, translogUUID, deletionPolicy, globalCheckpointSupplier, primaryTermSupplier, persistedSequenceNumberConsumer, blobContainer);
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", e);
        }
    }
}
