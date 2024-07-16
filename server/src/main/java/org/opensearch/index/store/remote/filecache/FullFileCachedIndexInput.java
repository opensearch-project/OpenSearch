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

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

/**
 * Extension of {@link FileCachedIndexInput} for full files for handling clones and slices
 * We maintain a clone map so that we can close them when the parent IndexInput is closed so that ref count is properly maintained in file cache
 * Closing of clones explicitly is needed as Lucene does not guarantee that it will close the clones
 * https://github.com/apache/lucene/blob/8340b01c3cc229f33584ce2178b07b8984daa6a9/lucene/core/src/java/org/apache/lucene/store/IndexInput.java#L32-L33
 * @opensearch.experimental
 */
@ExperimentalApi
public class FullFileCachedIndexInput extends FileCachedIndexInput {
    private static final Logger logger = LogManager.getLogger(FullFileCachedIndexInput.class);
    private final Set<FullFileCachedIndexInput> clones;

    public FullFileCachedIndexInput(FileCache cache, Path filePath, IndexInput underlyingIndexInput) {
        this(cache, filePath, underlyingIndexInput, false);
    }

    public FullFileCachedIndexInput(FileCache cache, Path filePath, IndexInput underlyingIndexInput, boolean isClone) {
        super(cache, filePath, underlyingIndexInput, isClone);
        clones = new HashSet<>();
    }

    /**
     * Clones the index input and returns the clone
     * Increase the ref count whenever the index input is cloned and add it to the clone map as well
     */
    @Override
    public FullFileCachedIndexInput clone() {
        FullFileCachedIndexInput clonedIndexInput = new FullFileCachedIndexInput(cache, filePath, luceneIndexInput.clone(), true);
        cache.incRef(filePath);
        clones.add(clonedIndexInput);
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
        clones.add(slicedIndexInput);
        cache.incRef(filePath);
        return slicedIndexInput;
    }

    /**
     * Closes the index input and it's clones as well
     */
    @Override
    public void close() throws IOException {
        if (!closed) {
            if (isClone) {
                cache.decRef(filePath);
            }
            clones.forEach(indexInput -> {
                try {
                    indexInput.close();
                } catch (Exception e) {
                    logger.trace("Exception while closing clone - {}", e.getMessage());
                }
            });
            try {
                luceneIndexInput.close();
            } catch (AlreadyClosedException e) {
                logger.trace("FullFileCachedIndexInput already closed");
            }
            luceneIndexInput = null;
            clones.clear();
            closed = true;
        }
    }
}
