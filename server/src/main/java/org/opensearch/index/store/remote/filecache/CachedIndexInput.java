/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;

/**
 * Interface for an entry in the {@link FileCache} that can return an
 * {@link IndexInput}. Exactly how the IndexInput is created is determined by
 * the implementations.
 *
 * @opensearch.internal
 */
public interface CachedIndexInput extends AutoCloseable {
    /**
     * Gets the {@link IndexInput} this cache entry represents.
     * @return The IndexInput
     * @throws IOException if any I/O error occurs
     */
    IndexInput getIndexInput() throws IOException;

    /**
     * @return length in bytes
     */
    long length();

    /**
     * @return true if the entry is closed, false otherwise
     */
    boolean isClosed();
}
