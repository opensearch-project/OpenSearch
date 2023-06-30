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

import java.util.function.Consumer;

/**
 * Coordinator level search stats
 *
 * @opensearch.internal
 */
public final class SearchCoordinatorStats implements SearchRequestOperationsListener {
    public StatsHolder totalStats = new StatsHolder();

    // private final CounterMetric openContexts = new CounterMetric();

    // private volatile Map<String, StatsHolder> groupStats = emptyMap();

    @Inject
    public SearchCoordinatorStats() {}

    public long getDFSPreQueryMetric() {
        return totalStats.dfsPreQueryMetric.sum();
    }

    public long getDFSPreQueryCurrent() {
        return totalStats.dfsPreQueryCurrent.count();
    }

    public long getDFSPreQueryTotal() {
        return totalStats.dfsPreQueryTotal.count();
    }

    public long getCanMatchMetric() {
        return totalStats.canMatchMetric.sum();
    }

    public long getCanMatchCurrent() {
        return totalStats.canMatchCurrent.count();
    }

    public long getCanMatchTotal() {
        return totalStats.canMatchTotal.count();
    }

    public long getQueryMetric() {
        return totalStats.queryMetric.sum();
    }

    public long getQueryCurrent() {
        return totalStats.queryCurrent.count();
    }

    public long getQueryTotal() {
        return totalStats.queryTotal.count();
    }

    public long getFetchMetric() {
        return totalStats.fetchMetric.sum();
    }

    public long getFetchCurrent() {
        return totalStats.fetchCurrent.count();
    }

    public long getFetchTotal() {
        return totalStats.fetchTotal.count();
    }

    public long getExpandSearchMetric() {
        return totalStats.expandSearchMetric.sum();
    }

    public long getExpandSearchCurrent() {
        return totalStats.expandSearchCurrent.count();
    }

    public long getExpandSearchTotal() {
        return totalStats.expandSearchTotal.count();
    }

    private void computeStats(SearchPhaseContext searchPhaseContext, Consumer<StatsHolder> consumer) {
        consumer.accept(totalStats);
    }

    @Override
    public void onDFSPreQueryPhaseStart(SearchPhaseContext context) {
        computeStats(context, statsHolder -> { statsHolder.dfsPreQueryCurrent.inc(); });
    }

    @Override
    public void onDFSPreQueryPhaseEnd(SearchPhaseContext context, long tookTime) {
        computeStats(context, statsHolder -> {
            totalStats.dfsPreQueryCurrent.dec();
            totalStats.dfsPreQueryTotal.inc();
            totalStats.dfsPreQueryMetric.inc(tookTime);
        });
    }

    @Override
    public void onDFSPreQueryPhaseFailure(SearchPhaseContext context) {
        return;
    }

    @Override
    public void onCanMatchPhaseStart(SearchPhaseContext context) {
        computeStats(context, statsHolder -> { statsHolder.canMatchCurrent.inc(); });
    }

    @Override
    public void onCanMatchPhaseEnd(SearchPhaseContext context, long tookTime) {
        computeStats(context, statsHolder -> {
            totalStats.canMatchCurrent.dec();
            totalStats.canMatchTotal.inc();
            totalStats.canMatchMetric.inc(tookTime);
        });
    }

    @Override
    public void onCanMatchPhaseFailure(SearchPhaseContext context) {
        return;
    }

    @Override
    public void onQueryPhaseStart(SearchPhaseContext context) {
        computeStats(context, statsHolder -> { statsHolder.queryCurrent.inc(); });
    }

    @Override
    public void onQueryPhaseEnd(SearchPhaseContext context, long tookTime) {
        computeStats(context, statsHolder -> {
            totalStats.queryCurrent.dec();
            totalStats.queryTotal.inc();
            totalStats.queryMetric.inc(tookTime);
        });
    }

    @Override
    public void onQueryPhaseFailure(SearchPhaseContext context) {
        return;
    }

    @Override
    public void onFetchPhaseStart(SearchPhaseContext context) {
        computeStats(context, statsHolder -> { totalStats.fetchCurrent.inc(); });
    }

    @Override
    public void onFetchPhaseEnd(SearchPhaseContext context, long tookTime) {
        computeStats(context, statsHolder -> {
            totalStats.fetchCurrent.dec();
            totalStats.fetchTotal.inc();
            totalStats.fetchMetric.inc(tookTime);
        });
    }

    @Override
    public void onFetchPhaseFailure(SearchPhaseContext context) {
        return;
    }

    @Override
    public void onExpandSearchPhaseStart(SearchPhaseContext context) {
        computeStats(context, statsHolder -> { totalStats.expandSearchCurrent.inc(); });
    }

    @Override
    public void onExpandSearchPhaseEnd(SearchPhaseContext context, long tookTime) {
        computeStats(context, statsHolder -> {
            totalStats.expandSearchCurrent.dec();
            totalStats.expandSearchTotal.inc();
            totalStats.expandSearchMetric.inc(tookTime);
        });
    }

    @Override
    public void onExpandSearchPhaseFailure(SearchPhaseContext context) {
        return;
    }

    public static final class StatsHolder {
        public MeanMetric dfsPreQueryMetric = new MeanMetric();
        public CounterMetric dfsPreQueryCurrent = new CounterMetric();
        public CounterMetric dfsPreQueryTotal = new CounterMetric();
        public MeanMetric canMatchMetric = new MeanMetric();
        public CounterMetric canMatchCurrent = new CounterMetric();
        public CounterMetric canMatchTotal = new CounterMetric();
        public MeanMetric queryMetric = new MeanMetric();
        public CounterMetric queryCurrent = new CounterMetric();
        public CounterMetric queryTotal = new CounterMetric();
        public MeanMetric fetchMetric = new MeanMetric();
        public CounterMetric fetchCurrent = new CounterMetric();
        public CounterMetric fetchTotal = new CounterMetric();
        public MeanMetric expandSearchMetric = new MeanMetric();
        public CounterMetric expandSearchCurrent = new CounterMetric();
        public CounterMetric expandSearchTotal = new CounterMetric();
    }
}
