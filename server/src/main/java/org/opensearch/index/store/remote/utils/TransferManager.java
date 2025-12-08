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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCachedIndexInput;
import org.opensearch.secure_sm.AccessController;
import org.opensearch.threadpool.ThreadPool;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
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
    private final ThreadPool threadPool;

    public TransferManager(final StreamReader streamReader, final FileCache fileCache, ThreadPool threadPool) {
        this.streamReader = streamReader;
        this.fileCache = fileCache;
        this.threadPool = threadPool;
    }

    /**
     * Given a blobFetchRequestList, return it's corresponding IndexInput.
     *
     * Note: Scripted queries/aggs may trigger a blob fetch within a new security context.
     * As such the following operations require elevated permissions.
     *
     * cacheEntry.getIndexInput() downloads new blobs from the remote store to local fileCache.
     * fileCache.compute() as inserting into the local fileCache may trigger an eviction.
     *
     * @param blobFetchRequest to fetch
     * @return future of IndexInput augmented with internal caching maintenance tasks
     */
    @SuppressWarnings("removal")
    public IndexInput fetchBlob(BlobFetchRequest blobFetchRequest) throws IOException {
        final Path key = blobFetchRequest.getFilePath();
        logger.trace("fetchBlob called for {}", key.toString());

        try {
            return AccessController.doPrivilegedChecked(() -> {
                CachedIndexInput cacheEntry = fileCache.compute(key, (path, cachedIndexInput) -> {
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

                // Cache entry was either retrieved from the cache or newly added, either
                // way the reference count has been incremented by one. We can only
                // decrement this reference _after_ creating the clone to be returned.
                try {
                    return cacheEntry.getIndexInput().clone();
                } finally {
                    fileCache.decRef(key);
                }
            });
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            } else if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new IOException(e);
            }
        }
    }

    @ExperimentalApi
    public CompletableFuture<IndexInput> fetchBlobAsync(BlobFetchRequest blobFetchRequest) throws IOException {
        final Path key = blobFetchRequest.getFilePath();
        logger.trace("Asynchronous fetchBlob called for {}", key.toString());
        try {
            CachedIndexInput cacheEntry = fileCache.compute(key, (path, cachedIndexInput) -> {
                if (cachedIndexInput == null || cachedIndexInput.isClosed()) {
                    logger.trace("Transfer Manager - IndexInput closed or not in cache");
                    // Doesn't exist or is closed, either way create a new one
                    return new DelayedCreationCachedIndexInput(fileCache, streamReader, blobFetchRequest);
                } else {
                    logger.trace("Transfer Manager - Required blob Already in cache: {}", blobFetchRequest.toString());
                    // already in the cache and ready to be used (open)
                    return cachedIndexInput;
                }
            });
            // Cache entry was either retrieved from the cache or newly added, either
            // way the reference count has been incremented by one. We can only
            // decrement this reference _after_ creating the clone to be returned.
            // Making sure remote recovery thread-pool take care of background download
            try {
                return cacheEntry.asyncLoadIndexInput(threadPool.executor(ThreadPool.Names.REMOTE_RECOVERY));
            } catch (Exception exception) {
                fileCache.decRef(key);
                throw exception;
            }
        } catch (Exception cause) {
            logger.error("Exception while asynchronous fetching blob key:{}, Exception {}", key, cause.getMessage());
            throw (RuntimeException) cause;
        }
    }

    private static FileCachedIndexInput createIndexInput(FileCache fileCache, StreamReader streamReader, BlobFetchRequest request) {
        try {
            // This local file cache is ref counted and may not strictly enforce configured capacity.
            // If we find available capacity is exceeded, deny further BlobFetchRequests.
            if (fileCache.capacity() < fileCache.usage()) {
                fileCache.prune();
                throw new IOException(
                    "Local file cache capacity ("
                        + fileCache.capacity()
                        + ") exceeded ("
                        + fileCache.usage()
                        + ") - BlobFetchRequest failed: "
                        + request.getFilePath()
                );
            }
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
            final IndexInput luceneIndexInput = request.getDirectory().openInput(request.getFileName(), IOContext.DEFAULT);
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

        @ExperimentalApi
        public CompletableFuture<IndexInput> asyncLoadIndexInput(Executor executor) {
            if (isClosed.get()) {
                fileCache.decRef(request.getFilePath());
                return CompletableFuture.failedFuture(new IllegalStateException("Already closed"));
            }
            if (isStarted.getAndSet(true) == false) {
                // Create new future and set it as the result
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return createIndexInput(fileCache, streamReader, request);
                    } catch (Exception e) {
                        fileCache.remove(request.getFilePath());
                        throw new CompletionException(e);
                    }
                }, executor).handle((indexInput, throwable) -> {
                    fileCache.decRef(request.getFilePath());
                    if (throwable != null) {
                        result.completeExceptionally(throwable);
                    } else {
                        result.complete(indexInput);
                    }
                    return null;
                });
            } else {
                // Decreasing the extra ref count introduced by compute
                fileCache.decRef(request.getFilePath());
            }
            return result;
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
