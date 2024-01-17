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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporter;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
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
    /** enable insight data collection */
    private boolean enableCollect;

    /** enable insight data export */
    private boolean enableExport;

    /** The internal store that holds the query insight data */
    @Nullable
    protected S store;

    /** The exporter that exports the query insight data to certain sink */
    @Nullable
    protected E exporter;

    /** The export interval of this exporter, default to 1 day */
    protected TimeValue exportInterval = QueryInsightsSettings.MIN_EXPORT_INTERVAL;

    /** The internal OpenSearch thread pool that execute async processing and exporting tasks*/
    protected final ThreadPool threadPool;
    protected volatile Scheduler.Cancellable scheduledFuture;

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
        if (enableCollect && this.store != null) {
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
        if (!enableCollect) {
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
     * Restart the exporter with new config
     */
    public abstract void resetExporter(boolean enabled, QueryInsightsExporterType type, String identifier);

    /**
     * Clear all data in the store
     */
    public void clearAllData() {
        store.clear();
    }

    public void setEnableCollect(boolean enableCollect) {
        this.enableCollect = enableCollect;
    }

    public boolean getEnableCollect() {
        return this.enableCollect;
    }

    public void setEnableExport(boolean enableExport) {
        this.enableExport = enableExport;
    }

    public boolean getEnableExport() {
        return this.enableExport;
    }

    /**
     * Start the Query Insight Service.
     */
    @Override
    protected void doStart() {
        if (exporter != null && getEnableExport()) {
            scheduledFuture = threadPool.scheduleWithFixedDelay(this::doExport, exportInterval, ThreadPool.Names.GENERIC);
        }
    }

    /**
     * Stop the Query Insight Service
     */
    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
            if (exporter != null && getEnableExport()) {
                doExport();
            }
        }
    }

    private void doExport() {
        List<R> storedData = getQueryData();
        try {
            exporter.export(storedData);
            log.debug(String.format(Locale.ROOT, "finish exporting query insight data to sink %s", storedData));
        } catch (Exception e) {
            throw new RuntimeException(String.format(Locale.ROOT, "failed to export query insight data to sink, error: %s", e));
        }
    }

    @Override
    protected void doClose() {}

    public TimeValue getExportInterval() {
        return exportInterval;
    }

    /**
     * Set the export interval for the exporter.
     *
     * @param interval export interval
     */
    public void setExportInterval(TimeValue interval) {
        this.exportInterval = interval;
    }
}
