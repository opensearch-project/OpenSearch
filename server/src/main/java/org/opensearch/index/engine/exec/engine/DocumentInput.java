/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.engine;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;

/**
 * During indexing, the input document is parsed and we create fields which are then converted to specific data structures.
 * This class deals with encapsulating all the fields for a given document, in the implementation governed by a given data
 * format and its associated indexing engine. (e.g. Lucene would internally maintain an Iterable of IndexableField which
 * can then be consumed by underlying IndexWriter to put the document into the associated index)
 */
@ExperimentalApi
public interface DocumentInput<T> extends AutoCloseable {

    /**
     * Adds field's value to this document input. This is the method which would be invoked during document parsing
     * to create engine specific data types from parsed values (e.g. IntPoint in Lucene for an indexed integer)
     */
    void addField(MappedFieldType fieldType, Object value);

    /**
     * The final input which is built after buffering values of different fields into this document input.
     * This final input represents the type which can be consumed by the actual underlying writers.
     */
    T getFinalInput();

    /**
     * Submits this document input to the associated writer instance (if any).
     */
    WriteResult addToWriter() throws IOException;
}
