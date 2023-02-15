/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.metadata;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.Map;

/**
 * Parser interface for Metadata. Holds methods to convert to/from file content streams to metadata object holder - {@link T}
 * @param <T> The type of metadata to be parsed
 */
public interface MetadataContentParser<T> {
    /**
     * Implements logic to read metadata content from metadata file input stream {@code indexInput} and parse into {@link T}
     * @param indexInput metadata file input stream
     * @return metadata content parsed to {@link T}
     */
    T readContent(IndexInput indexInput) throws IOException;

    /**
     * Implements logic to write metadata content from {@code content} to metadata file output stream {@code indexOutput}
     * @param indexOutput metadata file input stream
     */
    void writeContent(IndexOutput indexOutput, T content) throws IOException;

    /**
     * Implements logic to write metadata content from {@code content} to metadata file output stream {@code indexOutput}
     * This method only supports metadata content in the form {@code Map<String, String>} to support RemoteSegment store metadata content.
     * @param indexOutput metadata file input stream
     * @param content metadata content
     *
     * TODO - This will removed in future releases and only {@link #writeContent(IndexOutput, Object)} should be used.
     */
    @Deprecated
    void writeContent(IndexOutput indexOutput, Map<String, String> content) throws IOException;
}
