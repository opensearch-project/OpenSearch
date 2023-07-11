/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.Nullable;
import org.opensearch.common.metrics.MeanMetric;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Delegate listener that delegates the call iff the required permits are obtained. Once the listener is closed, no
 * future calls to delgate should be allowed*
 */
public class GatedDelegateRefreshListener implements ReferenceManager.RefreshListener, Closeable {

    private static final int TOTAL_PERMITS = Integer.MAX_VALUE;
    private final Semaphore semaphore = new Semaphore(TOTAL_PERMITS, true);
    private final AtomicBoolean closed = new AtomicBoolean();
    private ReferenceManager.RefreshListener delegateListener;
    @Nullable
    private MeanMetric refreshListenerMetrics;

    /**
     * The ctor for gated delegate listener*
     * @param delegateListener the delegate listener
     * @param refreshListenerMetrics an optional refresh listener metrics
     */
    GatedDelegateRefreshListener(ReferenceManager.RefreshListener delegateListener, @Nullable MeanMetric refreshListenerMetrics) {
        this.delegateListener = delegateListener;
        //TODO instrument metrics for listeners
        this.refreshListenerMetrics = refreshListenerMetrics;
    }

    @Override
    public void beforeRefresh() throws IOException {
        handleDelegate(() -> {
            try {
                delegateListener.beforeRefresh();
            } catch (IOException e) {
                throw new RuntimeException("Failed to execute before refresh due to ", e);
            }
        });
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        handleDelegate(() -> {
            try {
                delegateListener.afterRefresh(didRefresh);
            } catch (IOException e) {
                throw new RuntimeException("Failed to execute after refresh due to ", e);
            }
        });
    }

    private void handleDelegate(Runnable delegate) {
        assert Thread.holdsLock(this);
        if (closed.get() == false) {
            try {
                if (semaphore.tryAcquire(1, 0, TimeUnit.SECONDS)) {
                    try {
                        delegate.run();
                    } finally {
                        semaphore.release(1);
                    }
                } else {
                    // this should never happen, if it does something is deeply wrong
                    throw new TimeoutException("failed to obtain permit but operations are not delayed");
                }
            } catch(InterruptedException | TimeoutException e){
                throw new RuntimeException("Failed to handle delegate due to ", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (semaphore.tryAcquire(TOTAL_PERMITS, 10, TimeUnit.MINUTES)) {
                boolean result = closed.compareAndSet(false, true);
                assert result;
                assert semaphore.availablePermits() == 0;
            } else {
                throw new TimeoutException("timeout while blocking operations");
            }
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException("Failed to close the gated listener due to ", e);
        }
    }
}
