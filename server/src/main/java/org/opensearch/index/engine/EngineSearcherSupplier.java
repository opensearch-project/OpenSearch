/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Supplier for point-in-time searchers.
 * <p>
 * Acquires a reader reference on creation and releases it on close.
 * Searchers obtained from this supplier share the same point-in-time view.
 *
 * @param <S> the searcher type
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class EngineSearcherSupplier<S extends EngineSearcher<?>> implements Releasable {
    private final AtomicBoolean released = new AtomicBoolean(false);

    public S acquireSearcher(String source) {
        if (released.get()) {
            throw new AlreadyClosedException("SearcherSupplier was closed");
        }
        return acquireSearcherInternal(source);
    }

    @Override
    public final void close() {
        if (released.compareAndSet(false, true)) {
            doClose();
        }
    }

    protected abstract S acquireSearcherInternal(String source);

    protected abstract void doClose();
}
