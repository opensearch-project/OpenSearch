/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;

/**
 * Represents a document input for adding fields and metadata to a writer.
 *
 * @param <T> the type of the final input representation
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DocumentInput<T> extends AutoCloseable {

    /**
     * Adds a row ID field to the document.
     *
     * @param fieldName the name of the row ID field
     * @param rowId the row ID value
     */
    void addRowIdField(String fieldName, long rowId);

    /**
     * Adds a field to the document.
     *
     * @param fieldType the mapped field type
     * @param value the field value
     */
    void addField(MappedFieldType fieldType, Object value);

    /**
     * Gets the final input representation.
     *
     * @return the final input of type T
     */
    T getFinalInput();

    /**
     * Adds this document to the writer.
     *
     * @return the write result
     * @throws IOException if an I/O error occurs
     */
    WriteResult addToWriter() throws IOException;

    /**
     * Sets the version for this document.
     *
     * @param version the version number
     */
    default void setVersion(long version) {
        // Default no-op implementations, override as needed
    }

    /**
     * Sets the sequence number for this document.
     *
     * @param seqNo the sequence number
     */
    default void setSeqNo(long seqNo) {
        // Default no-op implementations, override as needed
    }

    /**
     * Sets the primary term for this document.
     *
     * @param fieldName the field name
     * @param seqNo the sequence number
     */
    default void setPrimaryTerm(String fieldName, long seqNo) {
        // Default no-op implementations, override as needed
    }
}
