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
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

/**
 * This acts as entry point to fetch {@link BlobFetchRequest} and return actual {@link IndexInput}. Utilizes the BlobContainer interface to
 * read snapshot files located within a repository. This basically adapts BlobContainer snapshots files into IndexInput
 *
 * @opensearch.internal
 */
public class TransferManager {
    private static final Logger logger = LogManager.getLogger(TransferManager.class);

    private final ConcurrentMap<Path, CountDownLatch> latchMap = new ConcurrentHashMap<>();
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
        // We need to do a privileged action here in order to fetch from remote
        // and write to the local file cache in case this is invoked as a side
        // effect of a plugin (such as a scripted search) that doesn't have the
        // necessary permissions.
        try {
            return AccessController.doPrivileged((PrivilegedAction<IndexInput>) () -> fetchBlobInternal(blobFetchRequest));
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private IndexInput fetchBlobInternal(BlobFetchRequest blobFetchRequest) {
        final Path key = blobFetchRequest.getFilePath();
        // check if the origin is already in block cache
        IndexInput origin = fileCache.compute(key, (path, cachedIndexInput) -> {
            if (cachedIndexInput != null && cachedIndexInput.isClosed()) {
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

        if (origin == null) {
            final CountDownLatch existingLatch = latchMap.putIfAbsent(key, new CountDownLatch(1));
            if (existingLatch != null) {
                // Another thread is downloading the same resource. Wait for it
                // to complete then make a recursive call to fetch it from the
                // cache.
                try {
                    existingLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting on block download at " + key, e);
                }
                return fetchBlobInternal(blobFetchRequest);
            } else {
                // Origin is not in file cache, download origin and put in cache
                // We've effectively taken a lock for this key by inserting a
                // latch into the concurrent map, so we must be sure to remove it
                // and count it down before leaving.
                try {
                    IndexInput downloaded = downloadBlockLocally(blobFetchRequest);
                    FileCachedIndexInput newOrigin =
                        new FileCachedIndexInput(fileCache, blobFetchRequest.getFilePath(), downloaded);
                    fileCache.put(key, newOrigin);
                    origin = newOrigin;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    latchMap.remove(key).countDown();
                }
            }
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
