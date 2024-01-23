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
import org.opensearch.action.search.SearchType;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporter;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterType;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsLocalIndexExporter;
import org.opensearch.plugin.insights.rules.model.SearchQueryLatencyRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.threadpool.ThreadPool;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_LOCAL_INDEX_MAPPING;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.MIN_EXPORT_INTERVAL;

/**
 * Service responsible for gathering, analyzing, storing and exporting
 * top N queries with high latency data for search queries
 *
 * @opensearch.internal
 */
public class TopQueriesByLatencyService extends QueryInsightsService<
    SearchQueryLatencyRecord,
    PriorityBlockingQueue<SearchQueryLatencyRecord>,
    QueryInsightsExporter<SearchQueryLatencyRecord>> {
    private static final Logger log = LogManager.getLogger(TopQueriesByLatencyService.class);

    private static final TimeValue delay = TimeValue.ZERO;

    private int topNSize = QueryInsightsSettings.DEFAULT_TOP_N_SIZE;

    private TimeValue windowSize = TimeValue.timeValueSeconds(QueryInsightsSettings.DEFAULT_WINDOW_SIZE);

    private final ClusterService clusterService;
    private final Client client;

    /**
     * Create the TopQueriesByLatencyService Object
     * @param threadPool The OpenSearch thread pool to run async tasks
     * @param clusterService The clusterService of this node
     * @param client The OpenSearch Client
     */
    @Inject
    public TopQueriesByLatencyService(ThreadPool threadPool, ClusterService clusterService, Client client) {
        super(threadPool, new PriorityBlockingQueue<>(), null);
        this.clusterService = clusterService;
        this.client = client;
    }

    /**
     * Ingest the query data into to the top N queries with latency store
     *
     * @param timestamp The timestamp of the query.
     * @param searchType The manner at which the search operation is executed. see {@link SearchType}
     * @param source The search source that was executed by the query.
     * @param totalShards Total number of shards as part of the search query across all indices
     * @param indices The indices involved in the search query
     * @param propertyMap Extra attributes and information about a search query
     * @param phaseLatencyMap Contains phase level latency information in a search query
     * @param tookInNanos Total search request took time in nanoseconds
     */
    public void ingestQueryData(
        final Long timestamp,
        final SearchType searchType,
        final String source,
        final int totalShards,
        final String[] indices,
        final Map<String, Object> propertyMap,
        final Map<String, Long> phaseLatencyMap,
        final Long tookInNanos
    ) {
        if (timestamp <= 0) {
            log.error(
                String.format(
                    Locale.ROOT,
                    "Invalid timestamp %s when ingesting query data to compute top n queries with latency",
                    timestamp
                )
            );
            return;
        }
        if (totalShards <= 0) {
            log.error(
                String.format(
                    Locale.ROOT,
                    "Invalid totalShards %s when ingesting query data to compute top n queries with latency",
                    totalShards
                )
            );
            return;
        }
        this.threadPool.schedule(() -> {
            clearOutdatedData();
            super.ingestQueryData(
                new SearchQueryLatencyRecord(timestamp, searchType, source, totalShards, indices, propertyMap, phaseLatencyMap, tookInNanos)
            );
            // remove top elements for fix sizing priority queue
            if (this.store.size() > this.getTopNSize()) {
                this.store.poll();
            }
        }, delay, ThreadPool.Names.GENERIC);

        log.debug(String.format(Locale.ROOT, "successfully ingested: %s", this.store));
    }

    @Override
    public void clearOutdatedData() {
        store.removeIf(record -> record.getTimestamp() < System.currentTimeMillis() - windowSize.getMillis());
    }

    /**
     * Set the top N size for TopQueriesByLatencyService service.
     * @param size the top N size to set
     */
    public void setTopNSize(int size) {
        this.topNSize = size;
    }

    /**
     * Validate the top N size based on the internal constrains
     * @param size the wanted top N size
     */
    public void validateTopNSize(int size) {
        if (size > QueryInsightsSettings.MAX_N_SIZE) {
            throw new IllegalArgumentException(
                "Top N size setting ["
                    + QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE.getKey()
                    + "]"
                    + " should be smaller than max top N size ["
                    + QueryInsightsSettings.MAX_N_SIZE
                    + "was ("
                    + size
                    + " > "
                    + QueryInsightsSettings.MAX_N_SIZE
                    + ")"
            );
        }
    }

    /**
     * Get the top N size set for TopQueriesByLatencyService
     * @return the top N size
     */
    public int getTopNSize() {
        return this.topNSize;
    }

    /**
     * Set the window size for TopQueriesByLatencyService
     * @param windowSize window size to set
     */
    public void setWindowSize(TimeValue windowSize) {
        this.windowSize = windowSize;
    }

    /**
     * Validate if the window size is valid, based on internal constrains.
     * @param windowSize the window size to validate
     */
    public void validateWindowSize(TimeValue windowSize) {
        if (windowSize.compareTo(QueryInsightsSettings.MAX_WINDOW_SIZE) > 0) {
            throw new IllegalArgumentException(
                "Window size setting ["
                    + QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE.getKey()
                    + "]"
                    + " should be smaller than max window size ["
                    + QueryInsightsSettings.MAX_WINDOW_SIZE
                    + "was ("
                    + windowSize
                    + " > "
                    + QueryInsightsSettings.MAX_WINDOW_SIZE
                    + ")"
            );
        }
    }

    /**
     * Get the window size set for TopQueriesByLatencyService
     * @return the window size
     */
    public TimeValue getWindowSize() {
        return this.windowSize;
    }

    /**
     * Set the exporter type to export data generated in TopQueriesByLatencyService
     * @param type The type of exporter, defined in {@link QueryInsightsExporterType}
     */
    public void setExporterType(QueryInsightsExporterType type) {
        resetExporter(
            getEnableExport(),
            type,
            clusterService.getClusterSettings().get(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_IDENTIFIER)
        );
    }

    /**
     * Set if the exporter is enabled
     * @param enabled if the exporter is enabled
     */
    public void setExporterEnabled(boolean enabled) {
        super.setEnableExport(enabled);
        resetExporter(
            enabled,
            clusterService.getClusterSettings().get(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_TYPE),
            clusterService.getClusterSettings().get(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_IDENTIFIER)
        );
    }

    /**
     * Set the identifier of this exporter, which will be used when exporting the data
     *
     * For example, for local index exporter, this identifier would be used to define the index name
     * @param identifier the identifier for the exporter
     */
    public void setExporterIdentifier(String identifier) {
        resetExporter(
            getEnableExport(),
            clusterService.getClusterSettings().get(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_TYPE),
            identifier
        );
    }

    /**
     * Set the export interval for the exporter
     * @param interval export interval
     */
    public void setExportInterval(TimeValue interval) {
        super.setExportInterval(interval);
        resetExporter(
            getEnableExport(),
            clusterService.getClusterSettings().get(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_TYPE),
            clusterService.getClusterSettings().get(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_IDENTIFIER)
        );
    }

    /**
     * Validate if the export interval is valid, based on internal constrains.
     * @param exportInterval the export interval to validate
     */
    public void validateExportInterval(TimeValue exportInterval) {
        if (exportInterval.getSeconds() < MIN_EXPORT_INTERVAL.getSeconds()) {
            throw new IllegalArgumentException(
                "Export Interval setting ["
                    + QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_INTERVAL.getKey()
                    + "]"
                    + " should not be smaller than minimal export interval size ["
                    + MIN_EXPORT_INTERVAL
                    + "]"
                    + "was ("
                    + exportInterval
                    + " < "
                    + MIN_EXPORT_INTERVAL
                    + ")"
            );
        }
    }

    /**
     * Reset the exporter with new config
     *
     * This function can be used to enable/disable an exporter, change the type of the exporter,
     * or change the identifier of the exporter.
     * @param enabled the enable flag to set on the exporter
     * @param type The QueryInsightsExporterType to set on the exporter
     * @param identifier the Identifier to set on the exporter
     */
    public void resetExporter(boolean enabled, QueryInsightsExporterType type, String identifier) {
        this.stop();
        this.exporter = null;

        if (!enabled) {
            return;
        }
        if (type.equals(QueryInsightsExporterType.LOCAL_INDEX)) {
            this.exporter = new QueryInsightsLocalIndexExporter<>(
                clusterService,
                client,
                identifier,
                TopQueriesByLatencyService.class.getClassLoader().getResourceAsStream(DEFAULT_LOCAL_INDEX_MAPPING)
            );
        }
        this.start();
    }

}
