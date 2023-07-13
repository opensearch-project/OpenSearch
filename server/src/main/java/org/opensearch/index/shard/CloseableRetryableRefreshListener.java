/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * RefreshListener that runs afterRefresh method if and only if there are atleast one permit available. Once the listener
 * is closed, all the permits are acquired and there are no available permits to afterRefresh. This abstract class provides
 * necessary abstract methods to schedule retry.
 */
public abstract class CloseableRetryableRefreshListener implements ReferenceManager.RefreshListener, Closeable {

    private static final int TOTAL_PERMITS = Integer.MAX_VALUE;

    private final Semaphore semaphore = new Semaphore(TOTAL_PERMITS);

    private final ThreadPool retryThreadPool;

    public CloseableRetryableRefreshListener(ThreadPool retryThreadPool) {
        this.retryThreadPool = retryThreadPool;
    }

    @Override
    public final void afterRefresh(boolean didRefresh) throws IOException {
        if (semaphore.tryAcquire()) {
            boolean shouldRetry;
            try {
                shouldRetry = performAfterRefresh(didRefresh);
            } finally {
                semaphore.release();
            }
            scheduleRetry(shouldRetry);
        }
    }

    protected String getRetryThreadPoolName() {
        return null;
    }

    protected TimeValue getNextRetryInterval() {
        return null;
    }

    private void scheduleRetry(TimeValue interval, String retryThreadPool) {
        if (this.retryThreadPool == null
            || interval == null
            || retryThreadPool == null
            || ThreadPool.THREAD_POOL_TYPES.containsKey(retryThreadPool) == false
            || interval == TimeValue.MINUS_ONE) {
            return;
        }
        this.retryThreadPool.schedule(() -> {
            if (semaphore.tryAcquire()) {
                boolean shouldRetry;
                try {
                    shouldRetry = doRetry();
                } finally {
                    semaphore.release();
                }
                scheduleRetry(shouldRetry);
            }
        }, interval, retryThreadPool);
    }

    private void scheduleRetry(boolean shouldRetry) {
        if (shouldRetry) {
            scheduleRetry(getNextRetryInterval(), getRetryThreadPoolName());
        }
    }

    /**
     * Override this method to provide the code that needs to be run on retry.
     *
     * @return true if a retry is needed. By default, it returns false which means there are no retries.
     */
    protected boolean doRetry() {
        // By default it is no-op so that the listeners that do not need this, dont have to implement this.
        return false;
    }

    /**
     * This method needs to be overridden and be provided with what needs to be run on after refresh.
     *
     * @param didRefresh true if the refresh opened a new reference
     * @return true if a retry is needed else false.
     */
    protected abstract boolean performAfterRefresh(boolean didRefresh);

    @Override
    public final void close() throws IOException {
        try {
            if (semaphore.tryAcquire(TOTAL_PERMITS, 10, TimeUnit.MINUTES)) {
                assert semaphore.availablePermits() == 0;
            } else {
                throw new RuntimeException("timeout while closing gated refresh listener");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
