/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.index.engine.exec.read.EngineSearcherSupplier;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Searcher supplier which provides the point in time datafusion reader for search operations
 */
public abstract class DatafusionSearcherSupplier extends EngineSearcherSupplier<org.opensearch.datafusion.search.DatafusionSearcher> {

    private final Function<
        org.opensearch.datafusion.search.DatafusionSearcher,
        org.opensearch.datafusion.search.DatafusionSearcher> wrapper;
    private final AtomicBoolean released = new AtomicBoolean(false);

    public DatafusionSearcherSupplier(
        Function<org.opensearch.datafusion.search.DatafusionSearcher, org.opensearch.datafusion.search.DatafusionSearcher> wrapper
    ) {
        this.wrapper = wrapper;
    }

    public final org.opensearch.datafusion.search.DatafusionSearcher acquireSearcher(String source) {
        if (released.get()) {
            throw new AlreadyClosedException("SearcherSupplier was closed");
        }
        final org.opensearch.datafusion.search.DatafusionSearcher searcher = acquireSearcherInternal(source);
        return searcher;
        // TODO apply wrapper
    }

    @Override
    public final void close() {
        if (released.compareAndSet(false, true)) {
            doClose();
        } else {
            assert false : "SearchSupplier was released twice";
        }
    }

    protected abstract void doClose();

    protected abstract org.opensearch.datafusion.search.DatafusionSearcher acquireSearcherInternal(String source);

}
