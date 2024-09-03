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

    /**
     * Functional interface to get an InputStream for a file at a certain offset and size
     */
    @FunctionalInterface
    public interface StreamReader {
        InputStream read(String name, long position, long length) throws IOException;
    }

    private final StreamReader streamReader;
    private final FileCache fileCache;

    public TransferManager(final StreamReader streamReader, final FileCache fileCache) {
        this.streamReader = streamReader;
        this.fileCache = fileCache;
    }

    /**
     * Given a blobFetchRequestList, return it's corresponding IndexInput.
     * @param blobFetchRequest to fetch
     * @return future of IndexInput augmented with internal caching maintenance tasks
     */
    public IndexInput fetchBlob(BlobFetchRequest blobFetchRequest) throws IOException {

        final Path key = blobFetchRequest.getFilePath();
        logger.trace("fetchBlob called for {}", key.toString());

        // We need to do a privileged action here in order to fetch from remote
        // and write/evict from local file cache in case this is invoked as a side
        // effect of a plugin (such as a scripted search) that doesn't have the
        // necessary permissions.
        final CachedIndexInput cacheEntry = AccessController.doPrivileged((PrivilegedAction<CachedIndexInput>) () -> {
            return fileCache.compute(key, (path, cachedIndexInput) -> {
                if (cachedIndexInput == null || cachedIndexInput.isClosed()) {
                    logger.trace("Transfer Manager - IndexInput closed or not in cache");
                    // Doesn't exist or is closed, either way create a new one
                    return new DelayedCreationCachedIndexInput(fileCache, streamReader, blobFetchRequest);
                } else {
                    logger.trace("Transfer Manager - Already in cache");
                    // already in the cache and ready to be used (open)
                    return cachedIndexInput;
                }
            });
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

    @SuppressWarnings("removal")
    private static FileCachedIndexInput createIndexInput(FileCache fileCache, StreamReader streamReader, BlobFetchRequest request) {
        try {
            if (Files.exists(request.getFilePath()) == false) {
                logger.trace("Fetching from Remote in createIndexInput of Transfer Manager");
                try (
                    OutputStream fileOutputStream = Files.newOutputStream(request.getFilePath());
                    OutputStream localFileOutputStream = new BufferedOutputStream(fileOutputStream)
                ) {
                    for (BlobFetchRequest.BlobPart blobPart : request.blobParts()) {
                        try (
                            InputStream snapshotFileInputStream = streamReader.read(
                                blobPart.getBlobName(),
                                blobPart.getPosition(),
                                blobPart.getLength()
                            );
                        ) {
                            snapshotFileInputStream.transferTo(localFileOutputStream);
                        }
                    }
                }
            }
            final IndexInput luceneIndexInput = request.getDirectory().openInput(request.getFileName(), IOContext.READ);
            return new FileCachedIndexInput(fileCache, request.getFilePath(), luceneIndexInput);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
        private final StreamReader streamReader;
        private final BlobFetchRequest request;
        private final CompletableFuture<IndexInput> result = new CompletableFuture<>();
        private final AtomicBoolean isStarted = new AtomicBoolean(false);
        private final AtomicBoolean isClosed = new AtomicBoolean(false);

        private DelayedCreationCachedIndexInput(FileCache fileCache, StreamReader streamReader, BlobFetchRequest request) {
            this.fileCache = fileCache;
            this.streamReader = streamReader;
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
                    result.complete(createIndexInput(fileCache, streamReader, request));
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
            return request.getBlobLength();
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
