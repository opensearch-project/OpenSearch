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
import org.opensearch.plugin.insights.core.exporter.QueryInsightsLocalIndexExporter;
import org.opensearch.plugin.insights.rules.model.SearchQueryLatencyRecord;
import org.opensearch.threadpool.ThreadPool;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Service responsible for gathering, analyzing, storing and exporting
 * top N queries with high latency data for search queries
 *
 * @opensearch.internal
 */
public class TopQueriesByLatencyService extends QueryInsightsService<
    SearchQueryLatencyRecord,
    PriorityBlockingQueue<SearchQueryLatencyRecord>,
    QueryInsightsLocalIndexExporter<SearchQueryLatencyRecord>> {
    private static final Logger log = LogManager.getLogger(TopQueriesByLatencyService.class);

    /** Default window size in seconds to keep the top N queries with latency data in query insight store */
    public static final int DEFAULT_WINDOW_SIZE = 60;

    /** Default top N size to keep the data in query insight store */
    public static final int DEFAULT_TOP_N_SIZE = 3;

    private int topNSize = DEFAULT_TOP_N_SIZE;

    private TimeValue windowSize = TimeValue.timeValueSeconds(DEFAULT_WINDOW_SIZE);

    @Inject
    public TopQueriesByLatencyService(ThreadPool threadPool, ClusterService clusterService, Client client) {
        super(threadPool, new PriorityBlockingQueue<>(), null);
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
     */
    public void ingestQueryData(
        final Long timestamp,
        final SearchType searchType,
        final String source,
        final int totalShards,
        final String[] indices,
        final Map<String, Object> propertyMap,
        final Map<String, Long> phaseLatencyMap
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
        super.ingestQueryData(
            new SearchQueryLatencyRecord(timestamp, searchType, source, totalShards, indices, propertyMap, phaseLatencyMap)
        );
        // remove top elements for fix sizing priority queue
        if (this.store.size() > this.getTopNSize()) {
            this.store.poll();
        }
        log.debug(String.format(Locale.ROOT, "successfully ingested: %s", this.store));
    }

    @Override
    public void clearOutdatedData() {
        store.removeIf(record -> record.getTimestamp() < System.currentTimeMillis() - windowSize.getMillis());
    }

    public void setTopNSize(int size) {
        this.topNSize = size;
    }

    public void setWindowSize(TimeValue windowSize) {
        this.windowSize = windowSize;
    }

    public int getTopNSize() {
        return this.topNSize;
    }

    public TimeValue getWindowSize() {
        return this.windowSize;
    }

}
