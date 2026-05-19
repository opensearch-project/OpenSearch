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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.opensearch.action.search.SearchPhase;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchRequestOperationsListenerSupport;
import org.opensearch.action.search.SearchRequestStats;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.search.stats.SearchStats.Stats;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchStatsTests extends OpenSearchTestCase implements SearchRequestOperationsListenerSupport {

    public void testShardLevelSearchGroupStats() {
        // let's create two dummy search stats with groups
        Map<String, Stats> groupStats1 = new HashMap<>();
        Map<String, Stats> groupStats2 = new HashMap<>();
        Stats.Builder defaultStats = new Stats.Builder().queryCount(1)
            .queryTimeInMillis(1)
            .queryCurrent(1)
            .queryFailed(1)
            .concurrentQueryCount(1)
            .concurrentQueryTimeInMillis(1)
            .concurrentQueryCurrent(1)
            .queryConcurrency(1)
            .fetchCount(1)
            .fetchTimeInMillis(1)
            .fetchCurrent(1)
            .scrollCount(1)
            .scrollTimeInMillis(1)
            .scrollCurrent(1)
            .suggestCount(1)
            .suggestTimeInMillis(1)
            .suggestCurrent(1)
            .pitCount(1)
            .pitTimeInMillis(1)
            .pitCurrent(1)
            .searchIdleReactivateCount(1)
            .starTreeQueryCount(1)
            .starTreeQueryTimeInMillis(1)
            .starTreeQueryCurrent(1)
            .starTreeQueryFailed(1);
        groupStats2.put("group1", defaultStats.build());
        SearchStats searchStats1 = new SearchStats(defaultStats.build(), 0, groupStats1);
        SearchStats searchStats2 = new SearchStats(defaultStats.build(), 0, groupStats2);

        // adding these two search stats and checking group stats are correct
        searchStats1.add(searchStats2);
        assertStats(groupStats1.get("group1"), 1);

        // another call, adding again ...
        searchStats1.add(searchStats2);
        assertStats(groupStats1.get("group1"), 2);

        // making sure stats2 was not affected (this would previously return 2!)
        assertStats(groupStats2.get("group1"), 1);

        // adding again would then return wrong search stats (would return 4! instead of 3)
        searchStats1.add(searchStats2);
        assertStats(groupStats1.get("group1"), 3);

        long paramValue = randomIntBetween(2, 50);

        // Testing for request stats
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats testRequestStats = new SearchRequestStats(clusterSettings);
        SearchPhaseContext ctx = mock(SearchPhaseContext.class);
        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            SearchPhase mockSearchPhase = mock(SearchPhase.class);
            when(ctx.getCurrentPhase()).thenReturn(mockSearchPhase);
            when(mockSearchPhase.getStartTimeInNanos()).thenReturn(System.nanoTime() - TimeUnit.SECONDS.toNanos(paramValue));
            when(mockSearchPhase.getSearchPhaseNameOptional()).thenReturn(Optional.of(searchPhaseName));
            for (int iterator = 0; iterator < paramValue; iterator++) {
                onPhaseStart(testRequestStats, ctx);
                onPhaseEnd(testRequestStats, ctx);
            }
        }
        searchStats1.setSearchRequestStats(testRequestStats);
        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            assertEquals(
                0,
                searchStats1.getTotal().getRequestStatsLongHolder().getRequestStatsHolder().get(searchPhaseName.getName()).current
            );
            assertEquals(
                paramValue,
                searchStats1.getTotal().getRequestStatsLongHolder().getRequestStatsHolder().get(searchPhaseName.getName()).total
            );
            assertThat(
                searchStats1.getTotal().getRequestStatsLongHolder().getRequestStatsHolder().get(searchPhaseName.getName()).timeInMillis,
                greaterThanOrEqualTo(paramValue)
            );
        }
    }

    private static void assertStats(Stats stats, long equalTo) {
        assertEquals(equalTo, stats.getQueryCount());
        assertEquals(equalTo, stats.getQueryTimeInMillis());
        assertEquals(equalTo, stats.getQueryCurrent());
        assertEquals(equalTo, stats.getQueryFailedCount());
        assertEquals(equalTo, stats.getConcurrentQueryCount());
        assertEquals(equalTo, stats.getConcurrentQueryTimeInMillis());
        assertEquals(equalTo, stats.getConcurrentQueryCurrent());
        assertEquals(equalTo, stats.getStarTreeQueryCount());
        assertEquals(equalTo, stats.getStarTreeQueryTimeInMillis());
        assertEquals(equalTo, stats.getStarTreeQueryCurrent());
        assertEquals(equalTo, stats.getStarTreeQueryFailed());
        assertEquals(equalTo, stats.getFetchCount());
        assertEquals(equalTo, stats.getFetchTimeInMillis());
        assertEquals(equalTo, stats.getFetchCurrent());
        assertEquals(equalTo, stats.getScrollCount());
        assertEquals(equalTo, stats.getScrollTimeInMillis());
        assertEquals(equalTo, stats.getScrollCurrent());
        assertEquals(equalTo, stats.getPitCount());
        assertEquals(equalTo, stats.getPitTimeInMillis());
        assertEquals(equalTo, stats.getPitCurrent());
        assertEquals(equalTo, stats.getSuggestCount());
        assertEquals(equalTo, stats.getSuggestTimeInMillis());
        assertEquals(equalTo, stats.getSuggestCurrent());
        assertEquals(equalTo, stats.getSearchIdleReactivateCount());
        // avg_concurrency is not summed up across stats
        assertEquals(1, stats.getConcurrentAvgSliceCount(), 0);
    }

    public void testNegativeRequestStats() throws Exception {
        SearchStats searchStats = new SearchStats(new Stats(), 0, new HashMap<>());

        long paramValue = randomIntBetween(2, 50);

        // Testing for request stats
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats testRequestStats = new SearchRequestStats(clusterSettings);
        SearchPhaseContext ctx = mock(SearchPhaseContext.class);
        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            SearchPhase mockSearchPhase = mock(SearchPhase.class);
            when(ctx.getCurrentPhase()).thenReturn(mockSearchPhase);
            when(mockSearchPhase.getStartTimeInNanos()).thenReturn(System.nanoTime() - TimeUnit.SECONDS.toNanos(paramValue));
            when(mockSearchPhase.getSearchPhaseNameOptional()).thenReturn(Optional.ofNullable(searchPhaseName));
            for (int iterator = 0; iterator < paramValue; iterator++) {
                onPhaseStart(testRequestStats, ctx);
                onPhaseEnd(testRequestStats, ctx);
                onPhaseEnd(testRequestStats, ctx); // call onPhaseEnd() twice to make 'current' negative
            }
        }
        searchStats.setSearchRequestStats(testRequestStats);
        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            Assert.assertNotNull(searchStats.getTotal().getRequestStatsLongHolder());
            assertEquals(
                -1 * paramValue,    // current is negative, equals -1 * paramValue (num loop iterations)
                searchStats.getTotal().getRequestStatsLongHolder().getRequestStatsHolder().get(searchPhaseName.getName()).current
            );
            assertEquals(
                2 * paramValue,
                searchStats.getTotal().getRequestStatsLongHolder().getRequestStatsHolder().get(searchPhaseName.getName()).total
            );
            assertThat(
                searchStats.getTotal().getRequestStatsLongHolder().getRequestStatsHolder().get(searchPhaseName.getName()).timeInMillis,
                greaterThanOrEqualTo(paramValue)
            );
        }

        // Ensure writeTo() does not throw error with negative 'current'
        searchStats.writeTo(new BytesStreamOutput(10));
    }
}
