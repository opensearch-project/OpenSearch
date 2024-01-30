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
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.threadpool.ThreadPool;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Service responsible for gathering, analyzing, storing and exporting
 * top N queries with high latency data for search queries
 *
 * @opensearch.internal
 */
public class QueryInsightsService {
    private static final Logger log = LogManager.getLogger(QueryInsightsService.class);

    private static final TimeValue delay = TimeValue.ZERO;
    /**
     * The internal OpenSearch thread pool that execute async processing and exporting tasks
     */
    private final ThreadPool threadPool;

    /**
     * enable insight data collection
     */
    private final Map<MetricType, Boolean> enableCollect = new HashMap<>();

    private int topNSize = QueryInsightsSettings.DEFAULT_TOP_N_SIZE;

    private TimeValue windowSize = TimeValue.timeValueSeconds(QueryInsightsSettings.DEFAULT_WINDOW_SIZE);
    /**
     * The internal thread-safe store that holds the top n queries insight data, by different MetricType
     */
    private final Map<MetricType, PriorityBlockingQueue<SearchQueryRecord>> topQueriesStores;

    /**
     * The internal store that holds historical top n queries insight data by different MetricType in the last window
     */
    private final Map<MetricType, List<SearchQueryRecord>> topQueriesHistoryStores;
    /**
     * window start timestamp for each top queries collector
     */
    private final Map<MetricType, Long> topQueriesWindowStart;

    /**
     * Create the TopQueriesByLatencyService Object
     *
     * @param threadPool     The OpenSearch thread pool to run async tasks
     */
    @Inject
    public QueryInsightsService(ThreadPool threadPool) {
        topQueriesStores = new HashMap<>();
        topQueriesHistoryStores = new HashMap<>();
        topQueriesWindowStart = new HashMap<>();
        for (MetricType metricType : MetricType.allMetricTypes()) {
            topQueriesStores.put(metricType, new PriorityBlockingQueue<>(topNSize, (a, b) -> SearchQueryRecord.compare(a, b, metricType)));
            topQueriesHistoryStores.put(metricType, new ArrayList<>());
            topQueriesWindowStart.put(metricType, -1L);
        }
        this.threadPool = threadPool;
    }

    /**
     * Ingest the query data into in-memory stores
     *
     * @param record the record to ingest
     */
    public void addRecord(SearchQueryRecord record) {
        for (MetricType metricType : record.getMeasurements().keySet()) {
            this.threadPool.schedule(() -> {
                if (!topQueriesStores.containsKey(metricType)) {
                    return;
                }
                // add the record to corresponding priority queues to calculate top n queries insights
                PriorityBlockingQueue<SearchQueryRecord> store = topQueriesStores.get(metricType);
                checkAndResetWindow(metricType, record.getTimestamp());
                if (record.getTimestamp() > topQueriesWindowStart.get(metricType)) {
                    store.add(record);
                    // remove top elements for fix sizing priority queue
                    if (store.size() > this.getTopNSize()) {
                        store.poll();
                    }
                }
            }, delay, QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR);
        }
    }

    private void checkAndResetWindow(MetricType metricType, Long timestamp) {
        Long windowStart = calculateWindowStart(timestamp);
        // reset window if the current window is outdated
        if (topQueriesWindowStart.get(metricType) < windowStart) {
            resetWindow(metricType, windowStart);
        }
    }

    private synchronized void resetWindow(MetricType metricType, Long newWindowStart) {
        // rotate the current window to history store only if the data belongs to the last window
        if (topQueriesWindowStart.get(metricType) == newWindowStart - windowSize.getMillis()) {
            topQueriesHistoryStores.put(metricType, new ArrayList<>(topQueriesStores.get(metricType)));
        } else {
            topQueriesHistoryStores.get(metricType).clear();
        }
        topQueriesStores.get(metricType).clear();
        topQueriesWindowStart.put(metricType, newWindowStart);
    }

    /**
     * Get all top queries records that are in the current query insight store, based on the input MetricType
     * Optionally include top N records from the last window.
     *
     * By default, return the records in sorted order.
     *
     * @param metricType {@link MetricType}
     * @param includeLastWindow if the top N queries from the last window should be included
     * @return List of the records that are in the query insight store
     * @throws IllegalArgumentException if query insight is disabled in the cluster
     */
    public List<SearchQueryRecord> getTopNRecords(MetricType metricType, boolean includeLastWindow) throws IllegalArgumentException {
        if (!enableCollect.containsKey(metricType) || !enableCollect.get(metricType)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Cannot get query data when query insight feature is not enabled for MetricType [%s].",
                    metricType
                )
            );
        }
        checkAndResetWindow(metricType, System.currentTimeMillis());
        List<SearchQueryRecord> queries = new ArrayList<>(topQueriesStores.get(metricType));
        if (includeLastWindow) {
            queries.addAll(topQueriesHistoryStores.get(metricType));
        }
        return Stream.of(queries)
            .flatMap(Collection::stream)
            .sorted((a, b) -> SearchQueryRecord.compare(a, b, metricType) * -1)
            .collect(Collectors.toList());
    }

    /**
     * Set flag to enable or disable Query Insights data collection
     *
     * @param metricType {@link MetricType}
     * @param enable Flag to enable or disable Query Insights data collection
     */
    public void enableCollection(MetricType metricType, boolean enable) {
        this.enableCollect.put(metricType, enable);
        // set topQueriesWindowStart to enable top n queries collection
        if (enable) {
            topQueriesWindowStart.put(metricType, calculateWindowStart(System.currentTimeMillis()));
        }
    }

    /**
     * Get if the Query Insights data collection is enabled for a MetricType
     *
     * @param metricType {@link MetricType}
     * @return if the Query Insights data collection is enabled
     */
    public boolean isCollectionEnabled(MetricType metricType) {
        return this.enableCollect.containsKey(metricType) && this.enableCollect.get(metricType);
    }

    /**
     * Set the top N size for TopQueriesByLatencyService service.
     *
     * @param size the top N size to set
     */
    public void setTopNSize(int size) {
        this.topNSize = size;
    }

    /**
     * Validate the top N size based on the internal constrains
     *
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
     *
     * @return the top N size
     */
    public int getTopNSize() {
        return this.topNSize;
    }

    /**
     * Set the window size for TopQueriesByLatencyService
     *
     * @param windowSize window size to set
     */
    public void setWindowSize(TimeValue windowSize) {
        this.windowSize = windowSize;
        for (MetricType metricType : MetricType.allMetricTypes()) {
            topQueriesWindowStart.put(metricType, -1L);
        }
    }

    /**
     * Validate if the window size is valid, based on internal constrains.
     *
     * @param windowSize the window size to validate
     */
    public void validateWindowSize(TimeValue windowSize) {
        if (windowSize.compareTo(QueryInsightsSettings.MAX_WINDOW_SIZE) > 0
            || windowSize.compareTo(QueryInsightsSettings.MIN_WINDOW_SIZE) < 0) {
            throw new IllegalArgumentException(
                "Window size setting ["
                    + QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE.getKey()
                    + "]"
                    + " should be between ["
                    + QueryInsightsSettings.MAX_WINDOW_SIZE
                    + ","
                    + QueryInsightsSettings.MAX_WINDOW_SIZE
                    + "]"
                    + "was ("
                    + windowSize
                    + ")"
            );
        }
        if (!(QueryInsightsSettings.VALID_WINDOW_SIZES_IN_MINUTES.contains(windowSize) || windowSize.getMinutes() % 60 == 0)) {
            throw new IllegalArgumentException(
                "Window size setting ["
                    + QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE.getKey()
                    + "]"
                    + " should be multiple of 1 hour, or one of "
                    + QueryInsightsSettings.VALID_WINDOW_SIZES_IN_MINUTES
                    + ", was ("
                    + windowSize
                    + ")"
            );
        }
    }

    /**
     * Get the size of top N queries store
     * @param metricType {@link MetricType}
     * @return top N queries store size
     */
    public int getTopNStoreSize(MetricType metricType) {
        return topQueriesStores.get(metricType).size();
    }

    /**
     * Get the size of top N queries history store
     * @param metricType {@link MetricType}
     * @return top N queries history store size
     */
    public int getTopNHistoryStoreSize(MetricType metricType) {
        return topQueriesHistoryStores.get(metricType).size();
    }

    private Long calculateWindowStart(Long timestamp) {
        LocalDateTime currentTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("UTC"));
        LocalDateTime windowStartTime = currentTime.truncatedTo(ChronoUnit.HOURS);
        while (!windowStartTime.plusMinutes(windowSize.getMinutes()).isAfter(currentTime)) {
            windowStartTime = windowStartTime.plusMinutes(windowSize.getMinutes());
        }
        return windowStartTime.toInstant(ZoneOffset.UTC).getEpochSecond() * 1000;
    }
}
