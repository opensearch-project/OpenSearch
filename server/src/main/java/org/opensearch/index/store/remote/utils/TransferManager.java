/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCachedIndexInput;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This acts as entry point to fetch {@link BlobFetchRequest} and return actual {@link IndexInput}. Utilizes the BlobContainer interface to
 * read snapshot files located within a repository. This basically adapts BlobContainer snapshots files into IndexInput
 *
 * @opensearch.internal
 */
public class TransferManager {
    private static final Logger logger = LogManager.getLogger(TransferManager.class);

    private final BlobContainer blobContainer;
    private final FileCache fileCache;

    public TransferManager(final BlobContainer blobContainer, final FileCache fileCache) {
        this.blobContainer = blobContainer;
        this.fileCache = fileCache;
    }

    /**
     * Given a blobFetchRequest, return it's corresponding IndexInput.
     * @param blobFetchRequest to fetch
     * @return future of IndexInput augmented with internal caching maintenance tasks
     */
    public IndexInput fetchBlob(BlobFetchRequest blobFetchRequest) throws IOException {
        final Path key = blobFetchRequest.getFilePath();

        final IndexInput origin = fileCache.compute(key, (path, cachedIndexInput) -> {
            if (cachedIndexInput == null) {
                try {
                    return new FileCachedIndexInput(fileCache, blobFetchRequest.getFilePath(), downloadBlockLocally(blobFetchRequest));
                } catch (IOException e) {
                    logger.warn("Failed to download " + blobFetchRequest.getFilePath(), e);
                    return null;
                }
            } else {
                if (cachedIndexInput.isClosed()) {
                    // if it's already in the file cache, but closed, open it and replace the original one
                    try {
                        final IndexInput luceneIndexInput = blobFetchRequest.getDirectory()
                            .openInput(blobFetchRequest.getFileName(), IOContext.READ);
                        return new FileCachedIndexInput(fileCache, blobFetchRequest.getFilePath(), luceneIndexInput);
                    } catch (IOException e) {
                        logger.warn("Failed to open existing file for " + blobFetchRequest.getFilePath(), e);
                        return null;
                    }
                }
                // already in the cache and ready to be used (open)
                return cachedIndexInput;
            }
        });

        if (origin == null) {
            throw new IOException("Failed to create IndexInput for " + blobFetchRequest.getFileName());
        }

        // Origin was either retrieved from the cache or newly added, either
        // way the reference count has been incremented by one. We can only
        // decrement this reference _after_ creating the clone to be returned.
        try {
            return origin.clone();
        } finally {
            fileCache.decRef(key);
        }
    }

    private IndexInput downloadBlockLocally(BlobFetchRequest blobFetchRequest) throws IOException {
        try (
            InputStream snapshotFileInputStream = blobContainer.readBlob(
                blobFetchRequest.getBlobName(),
                blobFetchRequest.getPosition(),
                blobFetchRequest.getLength()
            );
            OutputStream fileOutputStream = Files.newOutputStream(blobFetchRequest.getFilePath());
            OutputStream localFileOutputStream = new BufferedOutputStream(fileOutputStream);
        ) {
            snapshotFileInputStream.transferTo(localFileOutputStream);
        }
        return blobFetchRequest.getDirectory().openInput(blobFetchRequest.getFileName(), IOContext.READ);
    }
}
