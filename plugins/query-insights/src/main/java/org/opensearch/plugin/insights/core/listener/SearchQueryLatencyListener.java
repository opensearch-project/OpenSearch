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
import org.opensearch.plugin.insights.core.service.TopQueriesByLatencyService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_ENABLED;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_IDENTIFIER;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_INTERVAL;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_TYPE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE;

/**
 * The listener for top N queries by latency
 *
 * @opensearch.internal
 */
public final class SearchQueryLatencyListener extends SearchRequestOperationsListener {
    private static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    private static final Logger log = LogManager.getLogger(SearchQueryLatencyListener.class);

    private final TopQueriesByLatencyService topQueriesByLatencyService;

    @Inject
    public SearchQueryLatencyListener(ClusterService clusterService, TopQueriesByLatencyService topQueriesByLatencyService) {
        this.topQueriesByLatencyService = topQueriesByLatencyService;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TOP_N_LATENCY_QUERIES_ENABLED, this::setEnabled);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TOP_N_LATENCY_QUERIES_SIZE,
                this.topQueriesByLatencyService::setTopNSize,
                this.topQueriesByLatencyService::validateTopNSize
            );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TOP_N_LATENCY_QUERIES_WINDOW_SIZE,
                this.topQueriesByLatencyService::setWindowSize,
                this.topQueriesByLatencyService::validateWindowSize
            );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(TOP_N_LATENCY_QUERIES_EXPORTER_TYPE, this.topQueriesByLatencyService::setExporterType);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                TOP_N_LATENCY_QUERIES_EXPORTER_INTERVAL,
                this.topQueriesByLatencyService::setExportInterval,
                this.topQueriesByLatencyService::validateExportInterval
            );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(TOP_N_LATENCY_QUERIES_EXPORTER_IDENTIFIER, this.topQueriesByLatencyService::setExporterIdentifier);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(TOP_N_LATENCY_QUERIES_EXPORTER_ENABLED, this.topQueriesByLatencyService::setExporterEnabled);

        this.setEnabled(clusterService.getClusterSettings().get(TOP_N_LATENCY_QUERIES_ENABLED));
        this.topQueriesByLatencyService.setTopNSize(clusterService.getClusterSettings().get(TOP_N_LATENCY_QUERIES_SIZE));
        this.topQueriesByLatencyService.setWindowSize(clusterService.getClusterSettings().get(TOP_N_LATENCY_QUERIES_WINDOW_SIZE));
    }

    @Override
    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        this.topQueriesByLatencyService.setEnableCollect(enabled);
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
            topQueriesByLatencyService.ingestQueryData(
                request.getOrCreateAbsoluteStartMillis(),
                request.searchType(),
                request.source().toString(FORMAT_PARAMS),
                context.getNumShards(),
                request.indices(),
                new HashMap<>(),
                searchRequestContext.phaseTookMap(),
                System.nanoTime() - searchRequestContext.getAbsoluteStartNanos()
            );
        } catch (Exception e) {
            log.error(String.format(Locale.ROOT, "fail to ingest query insight data, error: %s", e));
        }
    }
}
