/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;
@ExperimentalApi
public interface DocumentInput<T> extends AutoCloseable {

    void addRowIdField(String fieldName, long rowId);

    void addField(MappedFieldType fieldType, Object value);

    T getFinalInput();

    WriteResult addToWriter() throws IOException;

    default void setVersion(long version) {
        // Default no-op implementations, override as needed
    }

    default void setSeqNo(long seqNo) {
        // Default no-op implementations, override as needed
    }

    default void setPrimaryTerm(String fieldName, long seqNo) {
        // Default no-op implementations, override as needed
    }
}
