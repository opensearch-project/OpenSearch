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
 * Implementation of the CachedIndexInput for full files which takes in an IndexInput as parameter
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CachedFullFileIndexInput implements CachedIndexInput {
    private final FileCache fileCache;
    private final Path path;
    private final FullFileCachedIndexInput fullFileCachedIndexInput;
    private final AtomicBoolean isClosed;

    /**
     * Constructor - takes IndexInput as parameter
     */
    public CachedFullFileIndexInput(FileCache fileCache, Path path, IndexInput indexInput) {
        this.fileCache = fileCache;
        this.path = path;
        fullFileCachedIndexInput = new FullFileCachedIndexInput(fileCache, path, indexInput);
        isClosed = new AtomicBoolean(false);
    }

    /**
     * Returns the wrapped indexInput
     */
    @Override
    public IndexInput getIndexInput() {
        if (isClosed.get()) throw new AlreadyClosedException("Index input is already closed");
        return fullFileCachedIndexInput;
    }

    /**
     * Returns the length of the wrapped indexInput
     */
    @Override
    public long length() {
        return fullFileCachedIndexInput.length();
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
            fullFileCachedIndexInput.close();
        }
    }
}
