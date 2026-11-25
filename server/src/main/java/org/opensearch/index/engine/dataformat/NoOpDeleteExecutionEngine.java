/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Delete engine used when no data format supports deletes.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class NoOpDeleteExecutionEngine implements DeleteExecutionEngine<DataFormat> {

    /** Shared instance. */
    public static final NoOpDeleteExecutionEngine INSTANCE = new NoOpDeleteExecutionEngine();

    /** Unsupported operation message. */
    public static final String UNSUPPORTED_MESSAGE = "Update/delete is not supported for this index: no delete-applicable data format "
        + "(requires a format such as Lucene)";

    private NoOpDeleteExecutionEngine() {}

    @Override
    public Deleter createDeleter(Writer<?> writer) {
        return null;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        return null;
    }

    @Override
    public DataFormat getDataFormat() {
        return null;
    }

    @Override
    public DeleteResult deleteDocument(DeleteInput deleteInput, Writer<?> writer) throws IOException {
        throw new IllegalArgumentException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void recordWrite(String id, DocumentLocation location) {}

    @Override
    public boolean onWriterCheckedOut(long generation) throws IOException {
        return false;
    }

    @Override
    public void close() throws IOException {}
}
