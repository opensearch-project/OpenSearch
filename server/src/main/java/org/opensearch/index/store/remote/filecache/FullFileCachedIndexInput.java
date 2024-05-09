/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of the CachedIndexInput for NON_BLOCK files which takes in an IndexInput as parameter
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FullFileCachedIndexInput implements CachedIndexInput {

    private final IndexInput indexInput;
    private final FileCache fileCache;
    private final Path path;
    private final FileCachedIndexInput fileCachedIndexInput;
    private final AtomicBoolean isClosed;

    /**
     * Constructor - takes IndexInput as parameter
     */
    public FullFileCachedIndexInput(FileCache fileCache, Path path, IndexInput indexInput) {
        this.fileCache = fileCache;
        this.path = path;
        this.indexInput = indexInput;
        fileCachedIndexInput = new FileCachedIndexInput(fileCache, path, indexInput);
        isClosed = new AtomicBoolean(false);
    }

    /**
     * Returns the wrapped indexInput
     */
    @Override
    public IndexInput getIndexInput() {
        if (isClosed.get()) throw new AlreadyClosedException("Index input is already closed");
        return fileCachedIndexInput;
    }

    /**
     * Returns the length of the wrapped indexInput
     */
    @Override
    public long length() {
        return indexInput.length();
    }

    /**
     * Checks if the wrapped indexInput is closed
     */
    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    /**
     * Closes the wrapped indexInput
     */
    @Override
    public void close() throws Exception {
        if (!isClosed.getAndSet(true)) {
            indexInput.close();
        }
    }
}
