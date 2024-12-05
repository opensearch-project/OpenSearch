/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.search.stats;

import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;
import org.opensearch.common.regex.Regex;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;

/**
 * Shard level search stats
 *
 * @opensearch.internal
 */
public final class ShardSearchStats implements SearchOperationListener {

    private final StatsHolder totalStats = new StatsHolder();
    private final CounterMetric openContexts = new CounterMetric();
    private volatile Map<String, StatsHolder> groupsStats = emptyMap();

    /**
     * Returns the stats, including group specific stats. If the groups are null/0 length, then nothing
     * is returned for them. If they are set, then only groups provided will be returned, or
     * {@code _all} for all groups.
     */
    public SearchStats stats(String... groups) {
        SearchStats.Stats total = totalStats.stats();
        Map<String, SearchStats.Stats> groupsSt = null;
        if (CollectionUtils.isEmpty(groups) == false) {
            groupsSt = new HashMap<>(groupsStats.size());
            if (groups.length == 1 && groups[0].equals("_all")) {
                for (Map.Entry<String, StatsHolder> entry : groupsStats.entrySet()) {
                    groupsSt.put(entry.getKey(), entry.getValue().stats());
                }
            } else {
                for (Map.Entry<String, StatsHolder> entry : groupsStats.entrySet()) {
                    if (Regex.simpleMatch(groups, entry.getKey())) {
                        groupsSt.put(entry.getKey(), entry.getValue().stats());
                    }
                }
            }
        }
        return new SearchStats(total, openContexts.count(), groupsSt);
    }

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        computeStats(searchContext, statsHolder -> {
            if (searchContext.hasOnlySuggest()) {
                statsHolder.suggestCurrent.inc();
            } else {
                statsHolder.queryCurrent.inc();
                if (searchContext.shouldUseConcurrentSearch()) {
                    statsHolder.concurrentQueryCurrent.inc();
                }
            }
        });
    }

    @Override
    public void onFailedQueryPhase(SearchContext searchContext) {
        computeStats(searchContext, statsHolder -> {
            if (searchContext.hasOnlySuggest()) {
                statsHolder.suggestCurrent.dec();
                assert statsHolder.suggestCurrent.count() >= 0;
            } else {
                statsHolder.queryCurrent.dec();
                assert statsHolder.queryCurrent.count() >= 0;
                if (searchContext.shouldUseConcurrentSearch()) {
                    statsHolder.concurrentQueryCurrent.dec();
                    assert statsHolder.concurrentQueryCurrent.count() >= 0;
                }
            }
        });
    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        computeStats(searchContext, statsHolder -> {
            if (searchContext.hasOnlySuggest()) {
                statsHolder.suggestMetric.inc(tookInNanos);
                statsHolder.suggestCurrent.dec();
                assert statsHolder.suggestCurrent.count() >= 0;
            } else {
                statsHolder.queryMetric.inc(tookInNanos);
                statsHolder.queryCurrent.dec();
                assert statsHolder.queryCurrent.count() >= 0;
                if (searchContext.shouldUseConcurrentSearch()) {
                    statsHolder.concurrentQueryMetric.inc(tookInNanos);
                    statsHolder.concurrentQueryCurrent.dec();
                    assert statsHolder.concurrentQueryCurrent.count() >= 0;
                    assert searchContext.searcher().getSlices() != null;
                    statsHolder.queryConcurrencyMetric.inc(searchContext.searcher().getSlices().length);
                }
            }
        });
    }

    @Override
    public void onPreFetchPhase(SearchContext searchContext) {
        computeStats(searchContext, statsHolder -> statsHolder.fetchCurrent.inc());
    }

    @Override
    public void onFailedFetchPhase(SearchContext searchContext) {
        computeStats(searchContext, statsHolder -> statsHolder.fetchCurrent.dec());
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        computeStats(searchContext, statsHolder -> {
            statsHolder.fetchMetric.inc(tookInNanos);
            statsHolder.fetchCurrent.dec();
            assert statsHolder.fetchCurrent.count() >= 0;
        });
    }

    private void computeStats(SearchContext searchContext, Consumer<StatsHolder> consumer) {
        consumer.accept(totalStats);
        if (searchContext.groupStats() != null) {
            for (String group : searchContext.groupStats()) {
                consumer.accept(groupStats(group));
            }
        }
    }

    private StatsHolder groupStats(String group) {
        StatsHolder stats = groupsStats.get(group);
        if (stats == null) {
            synchronized (this) {
                stats = groupsStats.get(group);
                if (stats == null) {
                    stats = new StatsHolder();
                    groupsStats = MapBuilder.newMapBuilder(groupsStats).put(group, stats).immutableMap();
                }
            }
        }
        return stats;
    }

    @Override
    public void onNewReaderContext(ReaderContext readerContext) {
        openContexts.inc();
    }

    @Override
    public void onFreeReaderContext(ReaderContext readerContext) {
        openContexts.dec();
    }

    @Override
    public void onNewScrollContext(ReaderContext readerContext) {
        totalStats.scrollCurrent.inc();
    }

    @Override
    public void onFreeScrollContext(ReaderContext readerContext) {
        totalStats.scrollCurrent.dec();
        assert totalStats.scrollCurrent.count() >= 0;
        totalStats.scrollMetric.inc(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - readerContext.getStartTimeInNano()));
    }

    @Override
    public void onNewPitContext(ReaderContext readerContext) {
        totalStats.pitCurrent.inc();
    }

    @Override
    public void onFreePitContext(ReaderContext readerContext) {
        totalStats.pitCurrent.dec();
        assert totalStats.pitCurrent.count() >= 0;
        totalStats.pitMetric.inc(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - readerContext.getStartTimeInNano()));
    }

    @Override
    public void onSearchIdleReactivation() {
        totalStats.searchIdleMetric.inc();
    }

    /**
     * Holder of statistics values
     *
     * @opensearch.internal
     */
    static final class StatsHolder {
        final MeanMetric queryMetric = new MeanMetric();
        final MeanMetric concurrentQueryMetric = new MeanMetric();
        final CounterMetric queryConcurrencyMetric = new CounterMetric();
        final MeanMetric fetchMetric = new MeanMetric();
        /* We store scroll statistics in microseconds because with nanoseconds we run the risk of overflowing the total stats if there are
         * many scrolls. For example, on a system with 2^24 scrolls that have been executed, each executing for 2^10 seconds, then using
         * nanoseconds would require a numeric representation that can represent at least 2^24 * 2^10 * 10^9 > 2^24 * 2^10 * 2^29 = 2^63
         * which exceeds the largest value that can be represented by a long. By using microseconds, we enable capturing one-thousand
         * times as many scrolls (i.e., billions of scrolls which at one per second would take 32 years to occur), or scrolls that execute
         * for one-thousand times as long (i.e., scrolls that execute for almost twelve days on average).
         */
        final MeanMetric scrollMetric = new MeanMetric();
        final MeanMetric pitMetric = new MeanMetric();
        final MeanMetric suggestMetric = new MeanMetric();
        final CounterMetric queryCurrent = new CounterMetric();
        final CounterMetric concurrentQueryCurrent = new CounterMetric();
        final CounterMetric fetchCurrent = new CounterMetric();
        final CounterMetric scrollCurrent = new CounterMetric();
        final CounterMetric pitCurrent = new CounterMetric();
        final CounterMetric suggestCurrent = new CounterMetric();
        final CounterMetric searchIdleMetric = new CounterMetric();

        SearchStats.Stats stats() {
            return new SearchStats.Stats(
                queryMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(queryMetric.sum()),
                queryCurrent.count(),
                concurrentQueryMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(concurrentQueryMetric.sum()),
                concurrentQueryCurrent.count(),
                queryConcurrencyMetric.count(),
                fetchMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(fetchMetric.sum()),
                fetchCurrent.count(),
                scrollMetric.count(),
                TimeUnit.MICROSECONDS.toMillis(scrollMetric.sum()),
                scrollCurrent.count(),
                pitMetric.count(),
                TimeUnit.MICROSECONDS.toMillis(pitMetric.sum()),
                pitCurrent.count(),
                suggestMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(suggestMetric.sum()),
                suggestCurrent.count(),
                searchIdleMetric.count()
            );
        }
    }
}
