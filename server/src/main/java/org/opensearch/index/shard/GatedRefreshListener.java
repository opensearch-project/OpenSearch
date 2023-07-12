/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.action.ActionListener;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Delegate listener that delegates the call iff the required permits are obtained. Once the listener is closed, no
 * future calls to delgate should be allowed
 */
public abstract class GatedRefreshListener implements ReferenceManager.RefreshListener, Closeable {

    private final AtomicBoolean closed = new AtomicBoolean();

    private static final int TOTAL_PERMITS = Integer.MAX_VALUE;

    private final Semaphore semaphore = new Semaphore(TOTAL_PERMITS);

    @Override
    public final void afterRefresh(boolean didRefresh) throws IOException {
        if (closed.get() == false && acquirePermit()) {
            ActionListener<Void> actionListener = new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    semaphore.release();
                }

                @Override
                public void onFailure(Exception e) {
                    semaphore.release();
                }
            };
            afterRefresh(didRefresh, actionListener);
        }
    }

    protected final boolean acquirePermit() {
        return semaphore.tryAcquire();
    }

    protected abstract void afterRefresh(boolean didRefresh, ActionListener<Void> actionListener);

    @Override
    public final void close() throws IOException {
        try {
            if (semaphore.tryAcquire(TOTAL_PERMITS, 10, TimeUnit.MINUTES)) {
                boolean result = closed.compareAndSet(false, true);
                assert result;
                assert semaphore.availablePermits() == 0;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to close the gated listener due to ", e);
        }
    }
}
