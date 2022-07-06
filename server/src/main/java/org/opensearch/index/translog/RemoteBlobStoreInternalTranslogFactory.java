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
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public class RemoteBlobStoreInternalTranslogFactory implements TranslogFactory {

    private final Repository repository;

    public RemoteBlobStoreInternalTranslogFactory(Repository repository) {
        this.repository = repository;
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
        blobPath = blobPath.add(config.getShardId().getIndexName()).add(String.valueOf(config.getShardId().getId()));
        BlobContainer blobContainer = ((BlobStoreRepository) repository).blobStore().blobContainer(blobPath);
        return new RemoteFsTranslog(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            blobContainer
        );
    }
}
