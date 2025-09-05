/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineSearcherSupplier;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public abstract class DatafusionSearcherSupplier extends EngineSearcherSupplier<DatafusionSearcher> {

    private final Function<DatafusionSearcher, DatafusionSearcher> wrapper;
    private final AtomicBoolean released = new AtomicBoolean(false);

    public DatafusionSearcherSupplier(Function<DatafusionSearcher, DatafusionSearcher> wrapper) {
        this.wrapper = wrapper;
    }

    public final DatafusionSearcher acquireSearcher(String source) {
        if (released.get()) {
            throw new AlreadyClosedException("SearcherSupplier was closed");
        }
        final DatafusionSearcher searcher = acquireSearcherInternal(source);
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

    protected abstract DatafusionSearcher acquireSearcherInternal(String source);

}
