/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.delete.engine;

import org.opensearch.delete.deleter.DeleterImpl;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DeleteExecutionEngine;
import org.opensearch.index.engine.dataformat.Deleter;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of {@link DeleteExecutionEngine} for formats that do not
 * handle deletes natively (e.g. Parquet). Maintains a writer-to-deleter mapping
 * so the engine layer can retrieve the correct deleter for a given writer.
 *
 * @opensearch.experimental
 */
public class DeleteExecutionEngineImpl implements DeleteExecutionEngine<DataFormat> {

    private final DataFormat dataFormat;
    private final Map<Writer<?>, Deleter> writerToDeleter = new ConcurrentHashMap<>();

    /**
     * Creates a delete execution engine for the given data format.
     *
     * @param dataFormat the data format this engine handles deletes for
     */
    public DeleteExecutionEngineImpl(DataFormat dataFormat) {
        this.dataFormat = dataFormat;
    }

    @Override
    public Deleter createDeleter(Writer<?> writer) {
        Deleter deleter = new DeleterImpl(writer);
        writerToDeleter.put(writer, deleter);
        return deleter;
    }

    @Override
    public Deleter getDeleter(Writer<?> writer) {
        return writerToDeleter.get(writer);
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        return null;
    }

    @Override
    public DataFormat getDataFormat() {
        return dataFormat;
    }

    @Override
    public void close() throws IOException {
        for (Deleter deleter : writerToDeleter.values()) {
            deleter.close();
        }
        writerToDeleter.clear();
    }
}
