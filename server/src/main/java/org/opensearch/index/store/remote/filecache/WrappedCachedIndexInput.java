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
 * Wrapper implementation of the CachedIndexInput which takes in an IndexInput as parameter
 */
public class WrappedCachedIndexInput implements CachedIndexInput {

    IndexInput indexInput;
    AtomicBoolean isClosed;

    /**
     * Constructor - takes IndexInput as parameter
     */
    public WrappedCachedIndexInput(IndexInput indexInput) {
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
        if (!isClosed()) {
            indexInput.close();
            isClosed.set(true);
        }
    }
}
