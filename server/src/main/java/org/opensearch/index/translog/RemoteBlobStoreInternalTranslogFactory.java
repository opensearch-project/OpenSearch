/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class RemoteBlobStoreInternalTranslogFactory implements TranslogFactory {

    private final Repository repository;
    private final ThreadPool threadPool;

    public RemoteBlobStoreInternalTranslogFactory(
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        Repository repository;
        try {
            repository = repositoriesServiceSupplier.get().repository("clusterUUID");
        } catch (RepositoryMissingException ex) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", ex);
        }
        this.repository = repository;
        this.threadPool = threadPool;
    }

    @Override
    public Translog newTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer
    ) throws IOException {

        assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
        BlobPath blobPath = new BlobPath();
        blobPath = blobPath.add(config.getShardId().getIndexName())
            .add(String.valueOf(config.getShardId().getId()))
            .add(String.valueOf(primaryTermSupplier.getAsLong()));
        BlobStoreRepository blobStoreRepository = ((BlobStoreRepository) repository);
        return new RemoteFsTranslog(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            blobStoreRepository,
            threadPool
        );
    }
}
