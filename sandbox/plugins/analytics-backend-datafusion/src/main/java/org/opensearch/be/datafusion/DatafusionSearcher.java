/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

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

    private final long readerPtr;

    public DatafusionSearcher(long readerPtr) {
        // TODO: initialize reader handle
        this.readerPtr = readerPtr;
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

    public long getReaderPtr() {
        return readerPtr;
    }

    @Override
    public void close() {
        // TODO : reader handle close
    }
}
