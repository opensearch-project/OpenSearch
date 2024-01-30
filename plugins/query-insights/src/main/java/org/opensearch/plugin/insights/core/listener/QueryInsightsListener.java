/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.Measurement;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE;

/**
 * The listener for top N queries by latency
 *
 * @opensearch.internal
 */
public final class QueryInsightsListener extends SearchRequestOperationsListener {
    private static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    private static final Logger log = LogManager.getLogger(QueryInsightsListener.class);

    private final QueryInsightsService queryInsightsService;

    /**
     * Constructor for QueryInsightsListener
     *
     * @param clusterService The Node's cluster service.
     * @param queryInsightsService The topQueriesByLatencyService associated with this listener
     */
    @Inject
    public QueryInsightsListener(ClusterService clusterService, QueryInsightsService queryInsightsService) {
        this.queryInsightsService = queryInsightsService;
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(TOP_N_LATENCY_QUERIES_ENABLED, v -> this.setEnabled(MetricType.LATENCY, v));
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TOP_N_LATENCY_QUERIES_SIZE,
                this.queryInsightsService::setTopNSize,
                this.queryInsightsService::validateTopNSize
            );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TOP_N_LATENCY_QUERIES_WINDOW_SIZE,
                this.queryInsightsService::setWindowSize,
                this.queryInsightsService::validateWindowSize
            );
        this.setEnabled(MetricType.LATENCY, clusterService.getClusterSettings().get(TOP_N_LATENCY_QUERIES_ENABLED));
        this.queryInsightsService.setTopNSize(clusterService.getClusterSettings().get(TOP_N_LATENCY_QUERIES_SIZE));
        this.queryInsightsService.setWindowSize(clusterService.getClusterSettings().get(TOP_N_LATENCY_QUERIES_WINDOW_SIZE));
    }

    /**
     * Enable or disable metric collection for {@link MetricType}
     *
     * @param metricType {@link MetricType}
     * @param enabled boolean
     */
    public void setEnabled(MetricType metricType, boolean enabled) {
        this.queryInsightsService.enableCollection(metricType, enabled);

        // disable QueryInsightsListener only if collection for all metrics are disabled.
        if (!enabled) {
            for (MetricType t : MetricType.allMetricTypes()) {
                if (this.queryInsightsService.isCollectionEnabled(t)) {
                    return;
                }
            }
            super.setEnabled(false);
        } else {
            super.setEnabled(true);
        }
    }

    @Override
    public boolean isEnabled() {
        return super.isEnabled();
    }

    @Override
    public void onPhaseStart(SearchPhaseContext context) {}

    @Override
    public void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {}

    @Override
    public void onPhaseFailure(SearchPhaseContext context) {}

    @Override
    public void onRequestStart(SearchRequestContext searchRequestContext) {}

    @Override
    public void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        SearchRequest request = context.getRequest();
        try {
            Map<MetricType, Measurement<? extends Number>> measurements = new HashMap<>();
            if (queryInsightsService.isCollectionEnabled(MetricType.LATENCY)) {
                measurements.put(
                    MetricType.LATENCY,
                    new Measurement<>(
                        MetricType.LATENCY.name(),
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - searchRequestContext.getAbsoluteStartNanos())
                    )
                );
            }
            Map<Attribute, Object> attributes = new HashMap<>();
            attributes.put(Attribute.SEARCH_TYPE, request.searchType().toString().toLowerCase(Locale.ROOT));
            attributes.put(Attribute.SOURCE, request.source().toString(FORMAT_PARAMS));
            attributes.put(Attribute.TOTAL_SHARDS, context.getNumShards());
            attributes.put(Attribute.INDICES, request.indices());
            attributes.put(Attribute.PHASE_LATENCY_MAP, searchRequestContext.phaseTookMap());
            SearchQueryRecord record = new SearchQueryRecord(request.getOrCreateAbsoluteStartMillis(), measurements, attributes);
            queryInsightsService.addRecord(record);
        } catch (Exception e) {
            log.error(String.format(Locale.ROOT, "fail to ingest query insight data, error: %s", e));
        }
    }
}
