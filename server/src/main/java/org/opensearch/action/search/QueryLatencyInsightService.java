/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Service responsible for gathering, analyzing, storing and exporting
 * top N queries with high latency data for search queries
 *
 * @opensearch.internal
 */
public class QueryLatencyInsightService extends QueryInsightService<
    SearchQueryLatencyRecord,
    PriorityBlockingQueue<SearchQueryLatencyRecord>,
    QueryInsightLocalIndexExporter<SearchQueryLatencyRecord>
    > {
    private static final Logger log = LogManager.getLogger(QueryLatencyInsightService.class);

    /** Default window size in seconds to keep the top N queries with latency data in query insight store */
    private static final int DEFAULT_WINDOW_SIZE = 60;

    /** Default top N size to keep the data in query insight store */
    private static final int DEFAULT_TOP_N_SIZE = 3;
    public static final String TOP_N_LATENCY_QUERIES_PREFIX = TOP_N_QUERIES_PREFIX + ".latency";
    public static final Setting<Boolean> TOP_N_LATENCY_QUERIES_ENABLED = Setting.boolSetting(
        TOP_N_LATENCY_QUERIES_PREFIX + ".enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> TOP_N_LATENCY_QUERIES_SIZE = Setting.intSetting(
        TOP_N_LATENCY_QUERIES_PREFIX + ".top_n_size",
        DEFAULT_TOP_N_SIZE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> TOP_N_LATENCY_QUERIES_WINDOW_SIZE = Setting.intSetting(
        TOP_N_LATENCY_QUERIES_PREFIX + ".window_size",
        DEFAULT_WINDOW_SIZE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private int topNSize = DEFAULT_TOP_N_SIZE;

    private TimeValue windowSize = TimeValue.timeValueSeconds(DEFAULT_WINDOW_SIZE);


    @Inject
    public QueryLatencyInsightService(
        ClusterService clusterService,
        Client client,
        ThreadPool threadPool
    ) {
        super(
            threadPool,
            new PriorityBlockingQueue<>(),
            null
            );

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(TOP_N_LATENCY_QUERIES_ENABLED, this::setEnabled);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(TOP_N_LATENCY_QUERIES_SIZE, this::setTopNSize);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(TOP_N_LATENCY_QUERIES_WINDOW_SIZE, this::setWindowSize);
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
            log.error(String.format("Invalid timestamp %s when ingesting query data to compute top n queries with latency", timestamp));
            return;
        }
        if (totalShards <= 0) {
            log.error(String.format("Invalid totalShards %s when ingesting query data to compute top n queries with latency", totalShards));
            return;
        }
        super.ingestQueryData(new SearchQueryLatencyRecord(
            timestamp,
            searchType,
            source,
            totalShards,
            indices,
            propertyMap,
            phaseLatencyMap
        ));
        // remove top elements for fix sizing priority queue
        if (this.store.size() > this.getTopNSize()) {
            this.store.poll();
        }
        log.debug(String.format("successfully ingested: %s", this.store));
    }

    @Override
    public void clearOutdatedData() {
        store.removeIf(record ->
            record.getTimestamp() < System.currentTimeMillis() - windowSize.getMillis()
        );
    }

    public void setTopNSize(int size) {
        this.topNSize = size;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = TimeValue.timeValueSeconds(windowSize);
    }

    public int getTopNSize() {
        return this.topNSize;
    }

    public TimeValue getWindowSize() {
        return this.windowSize;
    }

}
