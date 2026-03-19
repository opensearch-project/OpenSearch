/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.jni.ReaderHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.EngineSearcher;

import java.io.IOException;

/**
 * DataFusion searcher — executes substrait query plans against a native DataFusion reader.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearcher implements EngineSearcher<DatafusionContext> {

    private final ReaderHandle readerHandle;

    public DatafusionSearcher(ReaderHandle readerHandle) {
        this.readerHandle = readerHandle;
    }

    @Override
    public void search(DatafusionContext context) throws IOException {
        if (context.getFilterTree() == null) {
            searchVanilla(context);
        } else {
            searchWithFilterTree(context);
        }
    }

    private void searchWithFilterTree(DatafusionContext context) {
        // TODO: wire NativeBridge — execute substrait plan, consume stream, populate context
        throw new UnsupportedOperationException("DataFusion native bridge not yet wired");
    }

    private void searchVanilla(DatafusionContext context) throws IOException {
        // TODO: wire NativeBridge — execute substrait plan, consume stream, populate context
        throw new UnsupportedOperationException("DataFusion native bridge not yet wired");
    }

    /**
     * Returns the type-safe handle to the native reader.
     * Call {@link ReaderHandle#getPointer()} only at JNI invocation time
     * to get the raw pointer with a liveness check.
     */
    public ReaderHandle getReaderHandle() {
        return readerHandle;
    }

    @Override
    public void close() {
        // ReaderHandle lifecycle is owned by DatafusionReader / EngineReaderManager,
        // not by the searcher. Do not close it here.
    }
}
