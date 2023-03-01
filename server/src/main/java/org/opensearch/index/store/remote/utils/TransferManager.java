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
import java.util.Objects;

/**
 * This acts as entry point to fetch {@link BlobFetchRequest} and return actual {@link IndexInput}. Utilizes the BlobContainer interface to
 * read snapshot files located within a repository. This basically adapts BlobContainer snapshots files into IndexInput
 *
 * @opensearch.internal
 */
public class TransferManager {
    private static final Logger logger = LogManager.getLogger(TransferManager.class);

    private final BlobContainer blobContainer;
    private final ConcurrentInvocationLinearizer<Path, IndexInput> invocationLinearizer;
    private final FileCache fileCache;

    public TransferManager(final BlobContainer blobContainer, final FileCache fileCache) {
        this.blobContainer = blobContainer;
        this.invocationLinearizer = new ConcurrentInvocationLinearizer<>();
        this.fileCache = fileCache;
    }

    /**
     * Given a blobFetchRequest, return it's corresponding IndexInput.
     * @param blobFetchRequest to fetch
     * @return future of IndexInput augmented with internal caching maintenance tasks
     */
    public IndexInput fetchBlob(BlobFetchRequest blobFetchRequest) throws InterruptedException, IOException {
        final IndexInput indexInput = invocationLinearizer.linearize(
            blobFetchRequest.getFilePath(),
            p -> fetchOriginBlob(blobFetchRequest)
        );
        return indexInput.clone();
    }

    /**
     * Fetches the "origin" IndexInput from the cache, downloading it first if it is
     * not already cached. This instance must be cloned before using. This method is
     * accessed through the ConcurrentInvocationLinearizer so read-check-write is
     * acceptable here
     */
    private IndexInput fetchOriginBlob(BlobFetchRequest blobFetchRequest) throws IOException {
        // check if the origin is already in block cache
        IndexInput origin = fileCache.computeIfPresent(blobFetchRequest.getFilePath(), (path, cachedIndexInput) -> {
            if (cachedIndexInput.isClosed()) {
                // if it's already in the file cache, but closed, open it and replace the original one
                try {
                    IndexInput luceneIndexInput = blobFetchRequest.getDirectory().openInput(blobFetchRequest.getFileName(), IOContext.READ);
                    return new FileCachedIndexInput(fileCache, blobFetchRequest.getFilePath(), luceneIndexInput);
                } catch (IOException ioe) {
                    logger.warn("Open index input " + blobFetchRequest.getFilePath() + " got error ", ioe);
                    // open failed so return null to download the file again
                    return null;
                }

            }
            // already in the cache and ready to be used (open)
            return cachedIndexInput;
        });

        if (Objects.isNull(origin)) {
            // origin is not in file cache, download origin

            // open new origin
            IndexInput downloaded = downloadBlockLocally(blobFetchRequest);

            // refcount = 0 at the beginning
            FileCachedIndexInput newOrigin = new FileCachedIndexInput(fileCache, blobFetchRequest.getFilePath(), downloaded);

            // put origin into file cache
            fileCache.put(blobFetchRequest.getFilePath(), newOrigin);
            origin = newOrigin;
        }
        return origin;
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
