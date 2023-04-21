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
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

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

        final CachedIndexInput cacheEntry = fileCache.compute(key, (path, cachedIndexInput) -> {
            if (cachedIndexInput == null || cachedIndexInput.isClosed()) {
                // Doesn't exist or is closed, either way create a new one
                return new DelayedCreationCachedIndexInput(fileCache, blobContainer, blobFetchRequest);
            } else {
                // already in the cache and ready to be used (open)
                return cachedIndexInput;
            }
        });

        // Cache entry was either retrieved from the cache or newly added, either
        // way the reference count has been incremented by one. We can only
        // decrement this reference _after_ creating the clone to be returned.
        try {
            return cacheEntry.getIndexInput().clone();
        } finally {
            fileCache.decRef(key);
        }
    }

    private static FileCachedIndexInput createIndexInput(FileCache fileCache, BlobContainer blobContainer, BlobFetchRequest request) {
        // We need to do a privileged action here in order to fetch from remote
        // and write to the local file cache in case this is invoked as a side
        // effect of a plugin (such as a scripted search) that doesn't have the
        // necessary permissions.
        return AccessController.doPrivileged((PrivilegedAction<FileCachedIndexInput>) () -> {
            try {
                if (Files.exists(request.getFilePath()) == false) {
                    try (
                        InputStream snapshotFileInputStream = blobContainer.readBlob(
                            request.getBlobName(),
                            request.getPosition(),
                            request.getLength()
                        );
                        OutputStream fileOutputStream = Files.newOutputStream(request.getFilePath());
                        OutputStream localFileOutputStream = new BufferedOutputStream(fileOutputStream)
                    ) {
                        snapshotFileInputStream.transferTo(localFileOutputStream);
                    }
                }
                final IndexInput luceneIndexInput = request.getDirectory().openInput(request.getFileName(), IOContext.READ);
                return new FileCachedIndexInput(fileCache, request.getFilePath(), luceneIndexInput);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    /**
     * Implementation of CachedIndexInput the defers creation of the underlying
     * IndexInput until the first invocation of {@link #getIndexInput()}. This
     * class is thread safe, and concurrent calls to {@link #getIndexInput()} will
     * result in blocking until the initial thread completes the creation of the
     * IndexInput.
     */
    private static class DelayedCreationCachedIndexInput implements CachedIndexInput {
        private final FileCache fileCache;
        private final BlobContainer blobContainer;
        private final BlobFetchRequest request;
        private final CompletableFuture<IndexInput> result = new CompletableFuture<>();
        private final AtomicBoolean isStarted = new AtomicBoolean(false);
        private final AtomicBoolean isClosed = new AtomicBoolean(false);

        private DelayedCreationCachedIndexInput(FileCache fileCache, BlobContainer blobContainer, BlobFetchRequest request) {
            this.fileCache = fileCache;
            this.blobContainer = blobContainer;
            this.request = request;
        }

        @Override
        public IndexInput getIndexInput() throws IOException {
            if (isClosed.get()) {
                throw new IllegalStateException("Already closed");
            }
            if (isStarted.getAndSet(true) == false) {
                // We're the first one here, need to download the block
                try {
                    result.complete(createIndexInput(fileCache, blobContainer, request));
                } catch (Exception e) {
                    result.completeExceptionally(e);
                    fileCache.remove(request.getFilePath());
                }
            }
            try {
                return result.join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof UncheckedIOException) {
                    throw ((UncheckedIOException) e.getCause()).getCause();
                } else if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                }
                throw e;
            }
        }

        @Override
        public long length() {
            return request.getLength();
        }

        @Override
        public boolean isClosed() {
            return isClosed.get();
        }

        @Override
        public void close() throws Exception {
            if (isClosed.getAndSet(true) == false) {
                result.whenComplete((indexInput, error) -> {
                    if (indexInput != null) {
                        try {
                            indexInput.close();
                        } catch (IOException e) {
                            logger.warn("Error closing IndexInput", e);
                        }
                    }
                });
            }
        }
    }
}
