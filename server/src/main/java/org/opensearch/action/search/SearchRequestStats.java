/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Request level search stats to track coordinator level node search latencies
 *
 * @opensearch.api
 */
@PublicApi(since = "2.11.0")
public final class SearchRequestStats extends SearchRequestOperationsListener {
    Map<SearchPhaseName, StatsHolder> phaseStatsMap = new EnumMap<>(SearchPhaseName.class);
    StatsHolder tookStatsHolder;

    public static final String SEARCH_REQUEST_STATS_ENABLED_KEY = "search.request_stats_enabled";
    public static final Setting<Boolean> SEARCH_REQUEST_STATS_ENABLED = Setting.boolSetting(
        SEARCH_REQUEST_STATS_ENABLED_KEY,
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    @Inject
    public SearchRequestStats(ClusterSettings clusterSettings) {
        this.setEnabled(clusterSettings.get(SEARCH_REQUEST_STATS_ENABLED));
        clusterSettings.addSettingsUpdateConsumer(SEARCH_REQUEST_STATS_ENABLED, this::setEnabled);
        tookStatsHolder = new StatsHolder();
        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            phaseStatsMap.put(searchPhaseName, new StatsHolder());
        }
    }

    public long getPhaseCurrent(SearchPhaseName searchPhaseName) {
        return phaseStatsMap.get(searchPhaseName).current.count();
    }

    public long getPhaseTotal(SearchPhaseName searchPhaseName) {
        return phaseStatsMap.get(searchPhaseName).total.count();
    }

    public long getPhaseMetric(SearchPhaseName searchPhaseName) {
        return phaseStatsMap.get(searchPhaseName).timing.sum();
    }

    public long getTookCurrent() {
        return tookStatsHolder.current.count();
    }

    public long getTookTotal() {
        return tookStatsHolder.total.count();
    }

    public long getTookMetric() {
        return tookStatsHolder.timing.sum();
    }

    @Override
    protected void onPhaseStart(SearchPhaseContext context) {
        phaseStatsMap.get(context.getCurrentPhase().getSearchPhaseName()).current.inc();
    }

    @Override
    protected void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        StatsHolder phaseStats = phaseStatsMap.get(context.getCurrentPhase().getSearchPhaseName());
        phaseStats.current.dec();
        phaseStats.total.inc();
        phaseStats.timing.inc(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - context.getCurrentPhase().getStartTimeInNanos()));
    }

    @Override
    protected void onPhaseFailure(SearchPhaseContext context, Throwable cause) {
        phaseStatsMap.get(context.getCurrentPhase().getSearchPhaseName()).current.dec();
    }

    @Override
    protected void onRequestStart(SearchRequestContext searchRequestContext) {
        tookStatsHolder.current.inc();
    }

    @Override
    protected void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        tookStatsHolder.current.dec();
        tookStatsHolder.total.inc();
        tookStatsHolder.timing.inc(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - searchRequestContext.getAbsoluteStartNanos()));
    }

    @Override
    protected void onRequestFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        tookStatsHolder.current.dec();
    }

    /**
     * Holder of statistics values
     *
     * @opensearch.internal
     */

    public static final class StatsHolder {
        CounterMetric current = new CounterMetric();
        CounterMetric total = new CounterMetric();
        MeanMetric timing = new MeanMetric();
    }
}
