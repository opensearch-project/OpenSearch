/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of the CachedIndexInput for NON_BLOCK files which takes in an IndexInput as parameter
 */
public class NonBlockCachedIndexInput implements CachedIndexInput {

    private final IndexInput indexInput;
    private final AtomicBoolean isClosed;

    /**
     * Constructor - takes IndexInput as parameter
     */
    public NonBlockCachedIndexInput(IndexInput indexInput) {
        this.indexInput = indexInput;
        isClosed = new AtomicBoolean(false);
    }

    /**
     * Returns the wrapped indexInput
     */
    @Override
    public IndexInput getIndexInput() throws IOException {
        return indexInput;
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
            isClosed.set(true);
        }
    }
}
