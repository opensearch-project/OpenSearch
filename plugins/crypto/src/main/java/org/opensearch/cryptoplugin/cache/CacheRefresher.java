/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.cryptoplugin.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class CacheRefresher extends AbstractAsyncTask {
    private static final Logger LOG = LogManager.getLogger(CacheRefresher.class);
    private final String threadpoolName;

    private final Set<CacheRefreshable> refreshables;

    public CacheRefresher(String threadpoolName, ThreadPool threadpool) {
        super(LOG, threadpool, TimeValue.timeValueMinutes(1), true);
        this.threadpoolName = threadpoolName;
        refreshables = Collections.synchronizedSet(new HashSet<>());
        rescheduleIfNecessary();
    }

    public synchronized void register(CacheRefreshable refreshable) {
        // To ensure refreshable is refreshed, and it works before registration.
        if (refreshable.shouldRefresh()) {
            refreshable.refresh();
        }
        refreshables.add(refreshable);
    }

    public synchronized void deregister(CacheRefreshable refreshable) {
        refreshables.remove(refreshable);
    }

    @Override
    protected boolean mustReschedule() {
        return !isClosed();
    }

    @Override
    protected void runInternal() {
        refreshables.forEach(refreshable -> {
            if (refreshable.shouldRefresh()) {
                refreshable.refresh();
            }
        });
    }

    @Override
    protected String getThreadPool() {
        return threadpoolName;
    }

}
