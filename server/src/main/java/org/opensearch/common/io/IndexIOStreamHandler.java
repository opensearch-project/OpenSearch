/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * Interface for reading/writing content streams to/from - {@link T}
 * @param <T> The type of content to be read/written to stream
 *
 * @opensearch.internal
 */
public interface IndexIOStreamHandler<T> {
    /**
     * Implements logic to read content from file input stream {@code indexInput} and parse into {@link T}
     * @param indexInput file input stream
     * @return content parsed to {@link T}
     */
    T readContent(IndexInput indexInput) throws IOException;

    /**
     * Implements logic to write content from {@code content} to file output stream {@code indexOutput}
     * @param indexOutput file input stream
     */
    void writeContent(IndexOutput indexOutput, T content) throws IOException;
}
