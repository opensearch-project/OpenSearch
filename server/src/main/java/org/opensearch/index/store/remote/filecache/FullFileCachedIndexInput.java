/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Extension of {@link FileCachedIndexInput} for full files for handling clones and slices
 * Since Lucene does not guarantee that it will close the clones/slices, we have created a Cleaner which handles closing of the clones/slices when they become phantom reachable
 * https://github.com/apache/lucene/blob/8340b01c3cc229f33584ce2178b07b8984daa6a9/lucene/core/src/java/org/apache/lucene/store/IndexInput.java#L32-L33
 * @opensearch.experimental
 */
@ExperimentalApi
public class FullFileCachedIndexInput extends FileCachedIndexInput {
    private static final Logger logger = LogManager.getLogger(FullFileCachedIndexInput.class);
    private final IndexInputHolder indexInputHolder;
    private static final Cleaner CLEANER = Cleaner.create(OpenSearchExecutors.daemonThreadFactory("index-input-cleaner"));

    public FullFileCachedIndexInput(FileCache cache, Path filePath, IndexInput underlyingIndexInput) {
        this(cache, filePath, underlyingIndexInput, false);
    }

    public FullFileCachedIndexInput(FileCache cache, Path filePath, IndexInput underlyingIndexInput, boolean isClone) {
        super(cache, filePath, underlyingIndexInput, isClone);
        indexInputHolder = new IndexInputHolder(closed, underlyingIndexInput, isClone, cache, filePath);
        CLEANER.register(this, indexInputHolder);
    }

    /**
     * Clones the index input and returns the clone
     * Increase the ref count whenever the index input is cloned and add it to the clone map as well
     */
    @Override
    public FullFileCachedIndexInput clone() {
        FullFileCachedIndexInput clonedIndexInput = new FullFileCachedIndexInput(cache, filePath, luceneIndexInput.clone(), true);
        cache.incRef(filePath);
        return clonedIndexInput;
    }

    /**
     * Clones the index input and returns the slice
     * Increase the ref count whenever the index input is sliced and add it to the clone map as well
     */
    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + this.length()
                    + ": "
                    + this
            );
        }
        IndexInput slicedLuceneIndexInput = luceneIndexInput.slice(sliceDescription, offset, length);
        FullFileCachedIndexInput slicedIndexInput = new FullFileCachedIndexInput(cache, filePath, slicedLuceneIndexInput, true);
        cache.incRef(filePath);
        return slicedIndexInput;
    }

    /**
     * Closes the index input and it's clones as well
     */
    @Override
    public void close() throws IOException {
        if (!closed.get()) {
            if (isClone) {
                cache.decRef(filePath);
            }
            try {
                luceneIndexInput.close();
            } catch (AlreadyClosedException e) {
                logger.trace("FullFileCachedIndexInput already closed");
            }
            luceneIndexInput = null;
            closed.set(true);
        }
    }

    /**
     * Run resource cleaning，To be used only in test
     */
    public void indexInputHolderRun() {
        indexInputHolder.run();
    }

    private static class IndexInputHolder implements Runnable {
        private final AtomicBoolean closed;
        private final IndexInput indexInput;
        private final FileCache cache;
        private final boolean isClone;
        private final Path path;

        IndexInputHolder(AtomicBoolean closed, IndexInput indexInput, boolean isClone, FileCache cache, Path path) {
            this.closed = closed;
            this.indexInput = indexInput;
            this.isClone = isClone;
            this.cache = cache;
            this.path = path;
        }

        @Override
        public void run() {
            try {
                if (!closed.get()) {
                    indexInput.close();
                    if (isClone) cache.decRef(path);
                    closed.set(true);
                }
            } catch (IOException e) {
                logger.error("Failed to close IndexInput while clearing phantom reachable object");
            }
        }
    }
}
