/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import org.apache.lucene.store.IndexInput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * This acts as entry point to fetch {@link BlobFetchRequest} and return actual {@link IndexInput}. Utilizes the BlobContainer interface to
 * read snapshot files located within a repository. This basically adapts BlobContainer snapshots files into IndexInput
 *
 * @opensearch.internal
 */
public class TransferManager {
    private final BlobContainer blobContainer;
    private final ConcurrentInvocationLinearizer<Path, IndexInput> invocationLinearizer;

    public TransferManager(final BlobContainer blobContainer, final ExecutorService remoteStoreExecutorService) {
        this.blobContainer = blobContainer;
        this.invocationLinearizer = new ConcurrentInvocationLinearizer<>(remoteStoreExecutorService);
    }

    /**
     * Given a blobFetchRequest, return it's corresponding IndexInput.
     * @param blobFetchRequest to fetch
     * @return future of IndexInput augmented with internal caching maintenance tasks
     */
    public CompletableFuture<IndexInput> asyncFetchBlob(BlobFetchRequest blobFetchRequest) {
        return asyncFetchBlob(blobFetchRequest.getFilePath(), () -> {
            try {
                return fetchBlob(blobFetchRequest);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    public CompletableFuture<IndexInput> asyncFetchBlob(Path path, Supplier<IndexInput> indexInputSupplier) {
        return invocationLinearizer.linearize(path, p -> indexInputSupplier.get());
    }

    private IndexInput fetchBlob(BlobFetchRequest blobFetchRequest) throws IOException {
        // for first phase, this is a simple remote repo blob read with no caching at all
        try (
            InputStream snapshotFileInputStream = blobContainer.readBlob(
                blobFetchRequest.getBlobName(),
                blobFetchRequest.getPosition(),
                blobFetchRequest.getLength()
            );
        ) {
            return new ByteArrayIndexInput(blobFetchRequest.getBlobName(), snapshotFileInputStream.readAllBytes());
        }
    }
}
