/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.inject.Inject;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Request level search stats to track coordinator level node search latencies
 *
 * @opensearch.internal
 */
public final class SearchRequestStats implements SearchRequestOperationsListener {
    public StatsHolder totalStats = new StatsHolder();

    ConcurrentHashMap<SearchPhaseName, Consumer<SearchPhaseContext>> phaseEndTracker = new ConcurrentHashMap<>();
    ConcurrentHashMap<SearchPhaseName, Consumer<SearchPhaseContext>> phaseStartTracker = new ConcurrentHashMap<>();
    ConcurrentHashMap<SearchPhaseName, Consumer<SearchPhaseContext>> phaseFailureTracker = new ConcurrentHashMap<>();

    @Inject
    public SearchRequestStats() {
        for (SearchPhaseName searchPhase : SearchPhaseName.values()) {
            phaseEndTracker.put(searchPhase, searchPhaseContext -> {
                if (SearchPhaseName.DFS_QUERY.equals(SearchPhaseName.getSearchPhaseName(searchPhaseContext.getCurrentPhase().getName()))) {
                    totalStats.queryCurrentMap.get(SearchPhaseName.QUERY).dec();
                    totalStats.queryTotalMap.get(SearchPhaseName.QUERY).inc();
                    totalStats.queryMetricMap.get(SearchPhaseName.QUERY)
                        .inc(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - searchPhaseContext.getCurrentPhase().getStartTimeInNanos()));
                } else {
                    totalStats.queryCurrentMap.get(searchPhase).dec();
                    totalStats.queryTotalMap.get(searchPhase).inc();
                    totalStats.queryMetricMap.get(searchPhase)
                        .inc(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - searchPhaseContext.getCurrentPhase().getStartTimeInNanos()));
                }
            });
            phaseStartTracker.put(searchPhase, searchPhaseContext -> {
                if (SearchPhaseName.DFS_QUERY.equals(SearchPhaseName.getSearchPhaseName(searchPhaseContext.getCurrentPhase().getName()))) {
                    totalStats.queryCurrentMap.get(SearchPhaseName.QUERY).inc();
                } else {
                    totalStats.queryCurrentMap.get(searchPhase).inc();
                }
            });
            phaseFailureTracker.put(searchPhase, searchPhaseContext -> {
                if (SearchPhaseName.DFS_QUERY.equals(SearchPhaseName.getSearchPhaseName(searchPhaseContext.getCurrentPhase().getName()))) {
                    totalStats.queryCurrentMap.get(SearchPhaseName.QUERY).dec();
                } else {
                    totalStats.queryCurrentMap.get(searchPhase).dec();
                }
            });
        }
    }

    public long getPhaseCurrent(SearchPhaseName searchPhaseName) {
        return totalStats.queryCurrentMap.computeIfAbsent(searchPhaseName, searchPhase -> new CounterMetric()).count();
    }

    public long getPhaseTotal(SearchPhaseName searchPhaseName) {
        return totalStats.queryTotalMap.computeIfAbsent(searchPhaseName, searchPhase -> new CounterMetric()).count();
    }

    public long getPhaseMetric(SearchPhaseName searchPhaseName) {
        return totalStats.queryMetricMap.computeIfAbsent(searchPhaseName, searchPhase -> new MeanMetric()).sum();
    }

    @Override
    public void onPhaseStart(SearchPhaseContext context) {
        Optional.ofNullable(SearchPhaseName.getSearchPhaseName(context.getCurrentPhase().getName())).map(searchPhaseName -> {
            phaseStartTracker.get(searchPhaseName).accept(context);
            return null;
        });
    }

    @Override
    public void onPhaseEnd(SearchPhaseContext context) {
        Optional.ofNullable(SearchPhaseName.getSearchPhaseName(context.getCurrentPhase().getName())).map(searchPhaseName -> {
            phaseEndTracker.get(searchPhaseName).accept(context);
            return null;
        });
    }

    @Override
    public void onPhaseFailure(SearchPhaseContext context) {
        Optional.ofNullable(SearchPhaseName.getSearchPhaseName(context.getCurrentPhase().getName())).map(searchPhaseName -> {
            phaseFailureTracker.get(searchPhaseName).accept(context);
            return null;
        });
    }

    /**
     * Holder of statistics values
     *
     * @opensearch.internal
     */

    public static final class StatsHolder {

        Map<SearchPhaseName, CounterMetric> queryCurrentMap = new ConcurrentHashMap<>();
        Map<SearchPhaseName, CounterMetric> queryTotalMap = new ConcurrentHashMap<>();
        Map<SearchPhaseName, MeanMetric> queryMetricMap = new ConcurrentHashMap<>();

        StatsHolder() {
            for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
                if (SearchPhaseName.DFS_QUERY.equals(searchPhaseName)) {
                    continue;
                }
                queryCurrentMap.put(searchPhaseName, new CounterMetric());
                queryTotalMap.put(searchPhaseName, new CounterMetric());
                queryMetricMap.put(searchPhaseName, new MeanMetric());
            }
        }

        public Map<SearchPhaseName, CounterMetric> getQueryCurrentMap() {
            return queryCurrentMap;
        }

        public Map<SearchPhaseName, CounterMetric> getQueryTotalMap() {
            return queryTotalMap;
        }

        public Map<SearchPhaseName, MeanMetric> getQueryMetricMap() {
            return queryMetricMap;
        }
    }
}
