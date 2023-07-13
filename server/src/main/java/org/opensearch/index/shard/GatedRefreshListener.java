/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.search.ReferenceManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class GatedRefreshListener implements ReferenceManager.RefreshListener, Closeable {

    private final AtomicBoolean closed = new AtomicBoolean();

    private static final int TOTAL_PERMITS = Integer.MAX_VALUE;

    private final Semaphore semaphore = new Semaphore(TOTAL_PERMITS);

    protected Runnable getPermitWrappedTask(Runnable runnable) {
        if (closed.get() == false && semaphore.tryAcquire()) {
            return () -> {
                try {
                    runnable.run();
                } finally {
                    semaphore.release();
                }
            };
        }
        return () -> {};
    }

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
