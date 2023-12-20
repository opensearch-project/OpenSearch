/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporter;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Service responsible for gathering, analyzing, storing and exporting data related to
 * search queries, based on certain dimensions.
 *
 * @param <R> The type of record that stores in the service
 * @param <S> The type of Collection that holds the aggregated data
 * @param <E> The type of exporter that exports the aggregated and processed data
 *
 * @opensearch.internal
 */
public abstract class QueryInsightsService<R extends SearchQueryRecord<?>, S extends Collection<R>, E extends QueryInsightsExporter<R>>
    extends AbstractLifecycleComponent {
    private static final Logger log = LogManager.getLogger(QueryInsightsService.class);
    private boolean enabled;

    /** The internal store that holds the query insight data */
    @Nullable
    protected S store;

    /** The exporter that exports the query insight data to certain sink */
    @Nullable
    protected E exporter;

    /** The internal OpenSearch thread pool that execute async processing and exporting tasks*/
    private final ThreadPool threadPool;
    private volatile Scheduler.Cancellable scheduledFuture;

    @Inject
    public QueryInsightsService(ThreadPool threadPool, @Nullable S store, @Nullable E exporter) {
        this.threadPool = threadPool;
        this.store = store;
        this.exporter = exporter;
    }

    /**
     * Ingest one record to the query insight store
     *
     * @param record the record to ingest
     */
    protected void ingestQueryData(R record) {
        if (this.store != null) {
            this.store.add(record);
        }
    }

    /**
     * Get all records that are in the query insight store,
     * By default, return the records in sorted order.
     *
     * @return List of the records that are in the query insight store
     * @throws IllegalArgumentException if query insight is disabled in the cluster
     */
    public List<R> getQueryData() throws IllegalArgumentException {
        if (!enabled) {
            throw new IllegalArgumentException("Cannot get query data when query insight feature is not enabled.");
        }
        clearOutdatedData();
        List<R> queries = new ArrayList<>(store);
        queries.sort(Collections.reverseOrder());
        return queries;
    }

    /**
     * Clear all outdated data in the store
     */
    public abstract void clearOutdatedData();

    /**
     * Clear all data in the store
     */
    public void clearAllData() {
        store.clear();
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean getEnabled() {
        return this.enabled;
    }

    /**
     * Start the Query Insight Service.
     */
    @Override
    protected void doStart() {
        if (exporter != null && exporter.getEnabled()) {
            scheduledFuture = threadPool.scheduleWithFixedDelay(
                this::doExportAndClear,
                exporter.getExportInterval(),
                ThreadPool.Names.GENERIC
            );
        }
    }

    /**
     * Stop the Query Insight Service
     */
    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
            if (exporter != null && exporter.getEnabled()) {
                doExportAndClear();
            }
        }
    }

    private void doExportAndClear() {
        List<R> storedData = getQueryData();
        try {
            exporter.export(storedData);
            clearAllData();
            log.debug(String.format(Locale.ROOT, "finish exporting query insight data to sink %s", storedData));
        } catch (Exception e) {
            throw new RuntimeException(String.format(Locale.ROOT, "failed to export query insight data to sink, error: %s", e));
        }
    }

    @Override
    protected void doClose() {}
}
