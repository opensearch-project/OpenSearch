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
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.tasks.Task;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.getTopNEnabledSetting;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.getTopNSizeSetting;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.getTopNWindowSizeSetting;

/**
 * The listener for query insights services.
 * It forwards query-related data to the appropriate query insights stores,
 * either for each request or for each phase.
 *
 * @opensearch.internal
 */
public final class QueryInsightsListener extends SearchRequestOperationsListener {
    private static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    private static final Logger log = LogManager.getLogger(QueryInsightsListener.class);

    private final QueryInsightsService queryInsightsService;
    private final ClusterService clusterService;

    /**
     * Constructor for QueryInsightsListener
     *
     * @param clusterService The Node's cluster service.
     * @param queryInsightsService The topQueriesByLatencyService associated with this listener
     */
    @Inject
    public QueryInsightsListener(final ClusterService clusterService, final QueryInsightsService queryInsightsService) {
        this.clusterService = clusterService;
        this.queryInsightsService = queryInsightsService;
        // Setting endpoints set up for top n queries, including enabling top n queries, window size and top n size
        // Expected metricTypes are Latency, CPU and Memory.
        for (MetricType type : MetricType.allMetricTypes()) {
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(getTopNEnabledSetting(type), v -> this.setEnableTopQueries(type, v));
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(
                    getTopNSizeSetting(type),
                    v -> this.queryInsightsService.setTopNSize(type, v),
                    v -> this.queryInsightsService.validateTopNSize(type, v)
                );
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(
                    getTopNWindowSizeSetting(type),
                    v -> this.queryInsightsService.setWindowSize(type, v),
                    v -> this.queryInsightsService.validateWindowSize(type, v)
                );

            this.setEnableTopQueries(type, clusterService.getClusterSettings().get(getTopNEnabledSetting(type)));
            this.queryInsightsService.validateTopNSize(type, clusterService.getClusterSettings().get(getTopNSizeSetting(type)));
            this.queryInsightsService.setTopNSize(type, clusterService.getClusterSettings().get(getTopNSizeSetting(type)));
            this.queryInsightsService.validateWindowSize(type, clusterService.getClusterSettings().get(getTopNWindowSizeSetting(type)));
            this.queryInsightsService.setWindowSize(type, clusterService.getClusterSettings().get(getTopNWindowSizeSetting(type)));
        }
    }

    /**
     * Enable or disable top queries insights collection for {@link MetricType}
     * This function will enable or disable the corresponding listeners
     * and query insights services.
     *
     * @param metricType {@link MetricType}
     * @param enabled boolean
     */
    public void setEnableTopQueries(final MetricType metricType, final boolean enabled) {
        boolean isAllMetricsDisabled = !queryInsightsService.isEnabled();
        this.queryInsightsService.enableCollection(metricType, enabled);
        if (!enabled) {
            // disable QueryInsightsListener only if all metrics collections are disabled now.
            if (!queryInsightsService.isEnabled()) {
                super.setEnabled(false);
                this.queryInsightsService.stop();
            }
        } else {
            super.setEnabled(true);
            // restart QueryInsightsListener only if none of metrics collections is enabled before.
            if (isAllMetricsDisabled) {
                this.queryInsightsService.stop();
                this.queryInsightsService.start();
            }
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
    public void onPhaseFailure(SearchPhaseContext context, Throwable cause) {}

    @Override
    public void onRequestStart(SearchRequestContext searchRequestContext) {}

    @Override
    public void onRequestEnd(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        constructSearchQueryRecord(context, searchRequestContext);
    }

    @Override
    public void onRequestFailure(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        constructSearchQueryRecord(context, searchRequestContext);
    }

    private void constructSearchQueryRecord(final SearchPhaseContext context, final SearchRequestContext searchRequestContext) {
        SearchTask searchTask = context.getTask();
        List<TaskResourceInfo> tasksResourceUsages = searchRequestContext.getPhaseResourceUsage();
        if (clusterService.getTaskResourceTrackingService() != null) {
            clusterService.getTaskResourceTrackingService().refreshResourceStats(searchTask);
        }
        tasksResourceUsages.add(
            new TaskResourceInfo(
                searchTask.getAction(),
                searchTask.getId(),
                searchTask.getParentTaskId().getId(),
                clusterService.localNode().getId(),
                searchTask.getTotalResourceStats()
            )
        );

        final SearchRequest request = context.getRequest();
        try {
            Map<MetricType, Number> measurements = new HashMap<>();
            if (queryInsightsService.isCollectionEnabled(MetricType.LATENCY)) {
                measurements.put(
                    MetricType.LATENCY,
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - searchRequestContext.getAbsoluteStartNanos())
                );
            }
            if (queryInsightsService.isCollectionEnabled(MetricType.CPU)) {
                measurements.put(
                    MetricType.CPU,
                    tasksResourceUsages.stream().map(a -> a.getTaskResourceUsage().getCpuTimeInNanos()).mapToLong(Long::longValue).sum()
                );
            }
            if (queryInsightsService.isCollectionEnabled(MetricType.MEMORY)) {
                measurements.put(
                    MetricType.MEMORY,
                    tasksResourceUsages.stream().map(a -> a.getTaskResourceUsage().getMemoryInBytes()).mapToLong(Long::longValue).sum()
                );
            }
            Map<Attribute, Object> attributes = new HashMap<>();
            attributes.put(Attribute.SEARCH_TYPE, request.searchType().toString().toLowerCase(Locale.ROOT));
            attributes.put(Attribute.SOURCE, request.source().toString(FORMAT_PARAMS));
            attributes.put(Attribute.TOTAL_SHARDS, context.getNumShards());
            attributes.put(Attribute.INDICES, request.indices());
            attributes.put(Attribute.PHASE_LATENCY_MAP, searchRequestContext.phaseTookMap());
            attributes.put(Attribute.TASK_RESOURCE_USAGES, tasksResourceUsages);

            Map<String, Object> labels = new HashMap<>();
            // Retrieve user provided label if exists
            String userProvidedLabel = context.getTask().getHeader(Task.X_OPAQUE_ID);
            if (userProvidedLabel != null) {
                labels.put(Task.X_OPAQUE_ID, userProvidedLabel);
            }
            attributes.put(Attribute.LABELS, labels);
            // construct SearchQueryRecord from attributes and measurements
            SearchQueryRecord record = new SearchQueryRecord(request.getOrCreateAbsoluteStartMillis(), measurements, attributes);
            queryInsightsService.addRecord(record);
        } catch (Exception e) {
            log.error(String.format(Locale.ROOT, "fail to ingest query insight data, error: %s", e));
        }
    }

}
