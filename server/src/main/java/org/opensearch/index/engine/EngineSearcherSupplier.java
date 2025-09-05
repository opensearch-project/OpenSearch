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

@ExperimentalApi
public abstract class EngineSearcherSupplier<T extends EngineSearcher> implements Releasable {
    private final AtomicBoolean released = new AtomicBoolean(false);

    /**
     * Acquire a searcher for the given source.
     */
    public T acquireSearcher(String source) {
        if (released.get()) {
            throw new AlreadyClosedException("SearcherSupplier was closed");
        }
        return acquireSearcherInternal(source);
    }

    protected abstract T acquireSearcherInternal(String source);

    protected abstract void doClose();
}
