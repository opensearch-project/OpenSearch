/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchRequestStatsTests extends OpenSearchTestCase {
    public void testSearchRequestStats_OnRequestFailure() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats testRequestStats = new SearchRequestStats(clusterSettings);
        SearchPhaseContext mockSearchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequestContext mockSearchRequestContext = mock(SearchRequestContext.class);

        testRequestStats.onRequestStart(mockSearchRequestContext);
        assertEquals(1, testRequestStats.getTookCurrent());
        testRequestStats.onRequestFailure(mockSearchPhaseContext, mockSearchRequestContext);
        assertEquals(0, testRequestStats.getTookCurrent());
        assertEquals(0, testRequestStats.getTookTotal());
    }

    public void testSearchRequestStats_OnRequestEnd() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats testRequestStats = new SearchRequestStats(clusterSettings);
        SearchPhaseContext mockSearchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequestContext mockSearchRequestContext = mock(SearchRequestContext.class);

        // Start request
        testRequestStats.onRequestStart(mockSearchRequestContext);
        assertEquals(1, testRequestStats.getTookCurrent());

        // Mock start time
        long tookTimeInMillis = randomIntBetween(1, 10);
        long startTimeInNanos = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(tookTimeInMillis);
        when(mockSearchRequestContext.getAbsoluteStartNanos()).thenReturn(startTimeInNanos);

        // End request
        testRequestStats.onRequestEnd(mockSearchPhaseContext, mockSearchRequestContext);
        assertEquals(0, testRequestStats.getTookCurrent());
        assertEquals(1, testRequestStats.getTookTotal());
        assertThat(testRequestStats.getTookMetric(), greaterThanOrEqualTo(tookTimeInMillis));
    }

    public void testSearchRequestPhaseFailure() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats testRequestStats = new SearchRequestStats(clusterSettings);
        SearchPhaseContext ctx = mock(SearchPhaseContext.class);
        SearchPhase mockSearchPhase = mock(SearchPhase.class);
        when(ctx.getCurrentPhase()).thenReturn(mockSearchPhase);

        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            when(mockSearchPhase.getSearchPhaseName()).thenReturn(searchPhaseName);
            testRequestStats.onPhaseStart(ctx);
            assertEquals(1, testRequestStats.getPhaseCurrent(searchPhaseName));
            testRequestStats.onPhaseFailure(ctx);
            assertEquals(0, testRequestStats.getPhaseCurrent(searchPhaseName));
        }
    }

    public void testSearchRequestStats() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats testRequestStats = new SearchRequestStats(clusterSettings);

        SearchPhaseContext ctx = mock(SearchPhaseContext.class);
        SearchPhase mockSearchPhase = mock(SearchPhase.class);
        when(ctx.getCurrentPhase()).thenReturn(mockSearchPhase);

        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            when(mockSearchPhase.getSearchPhaseName()).thenReturn(searchPhaseName);
            long tookTimeInMillis = randomIntBetween(1, 10);
            testRequestStats.onPhaseStart(ctx);
            long startTime = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(tookTimeInMillis);
            when(mockSearchPhase.getStartTimeInNanos()).thenReturn(startTime);
            assertEquals(1, testRequestStats.getPhaseCurrent(searchPhaseName));
            testRequestStats.onPhaseEnd(
                ctx,
                new SearchRequestContext(
                    new SearchRequestOperationsListener.CompositeListener(List.of(), LogManager.getLogger()),
                    new SearchRequest()
                )
            );
            assertEquals(0, testRequestStats.getPhaseCurrent(searchPhaseName));
            assertEquals(1, testRequestStats.getPhaseTotal(searchPhaseName));
            assertThat(testRequestStats.getPhaseMetric(searchPhaseName), greaterThanOrEqualTo(tookTimeInMillis));
        }
    }

    public void testSearchRequestStatsOnPhaseStartConcurrently() throws InterruptedException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats testRequestStats = new SearchRequestStats(clusterSettings);
        int numTasks = randomIntBetween(5, 50);
        Thread[] threads = new Thread[numTasks * SearchPhaseName.values().length];
        Phaser phaser = new Phaser(numTasks * SearchPhaseName.values().length + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks * SearchPhaseName.values().length);
        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            SearchPhaseContext ctx = mock(SearchPhaseContext.class);
            SearchPhase mockSearchPhase = mock(SearchPhase.class);
            when(ctx.getCurrentPhase()).thenReturn(mockSearchPhase);
            when(mockSearchPhase.getSearchPhaseName()).thenReturn(searchPhaseName);
            for (int i = 0; i < numTasks; i++) {
                threads[i] = new Thread(() -> {
                    phaser.arriveAndAwaitAdvance();
                    testRequestStats.onPhaseStart(ctx);
                    countDownLatch.countDown();
                });
                threads[i].start();
            }
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            assertEquals(numTasks, testRequestStats.getPhaseCurrent(searchPhaseName));
        }
    }

    public void testSearchRequestStatsOnPhaseEndConcurrently() throws InterruptedException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats testRequestStats = new SearchRequestStats(clusterSettings);
        int numTasks = randomIntBetween(5, 50);
        Thread[] threads = new Thread[numTasks * SearchPhaseName.values().length];
        Phaser phaser = new Phaser(numTasks * SearchPhaseName.values().length + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks * SearchPhaseName.values().length);
        Map<SearchPhaseName, Long> searchPhaseNameLongMap = new HashMap<>();
        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            SearchPhaseContext ctx = mock(SearchPhaseContext.class);
            SearchPhase mockSearchPhase = mock(SearchPhase.class);
            when(ctx.getCurrentPhase()).thenReturn(mockSearchPhase);
            when(mockSearchPhase.getSearchPhaseName()).thenReturn(searchPhaseName);
            long tookTimeInMillis = randomIntBetween(1, 10);
            long startTime = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(tookTimeInMillis);
            when(mockSearchPhase.getStartTimeInNanos()).thenReturn(startTime);
            for (int i = 0; i < numTasks; i++) {
                threads[i] = new Thread(() -> {
                    phaser.arriveAndAwaitAdvance();
                    testRequestStats.onPhaseEnd(
                        ctx,
                        new SearchRequestContext(
                            new SearchRequestOperationsListener.CompositeListener(List.of(), LogManager.getLogger()),
                            new SearchRequest()
                        )
                    );
                    countDownLatch.countDown();
                });
                threads[i].start();
            }
            searchPhaseNameLongMap.put(searchPhaseName, tookTimeInMillis);
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            assertEquals(numTasks, testRequestStats.getPhaseTotal(searchPhaseName));
            assertThat(
                testRequestStats.getPhaseMetric(searchPhaseName),
                greaterThanOrEqualTo((searchPhaseNameLongMap.get(searchPhaseName) * numTasks))
            );
        }
    }

    public void testSearchRequestStatsOnPhaseFailureConcurrently() throws InterruptedException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats testRequestStats = new SearchRequestStats(clusterSettings);
        int numTasks = randomIntBetween(5, 50);
        Thread[] threads = new Thread[numTasks * SearchPhaseName.values().length];
        Phaser phaser = new Phaser(numTasks * SearchPhaseName.values().length + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks * SearchPhaseName.values().length);
        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            SearchPhaseContext ctx = mock(SearchPhaseContext.class);
            SearchPhase mockSearchPhase = mock(SearchPhase.class);
            when(ctx.getCurrentPhase()).thenReturn(mockSearchPhase);
            when(mockSearchPhase.getSearchPhaseName()).thenReturn(searchPhaseName);
            for (int i = 0; i < numTasks; i++) {
                threads[i] = new Thread(() -> {
                    phaser.arriveAndAwaitAdvance();
                    testRequestStats.onPhaseStart(ctx);
                    testRequestStats.onPhaseFailure(ctx);
                    countDownLatch.countDown();
                });
                threads[i].start();
            }
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            assertEquals(0, testRequestStats.getPhaseCurrent(searchPhaseName));
        }
    }
}
