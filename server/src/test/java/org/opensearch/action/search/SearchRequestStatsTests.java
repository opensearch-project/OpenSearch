/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchRequestStatsTests extends OpenSearchTestCase {
    public void testSearchRequestPhaseFailure() {
        SearchRequestStats testRequestStats = new SearchRequestStats();
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
        SearchRequestStats testRequestStats = new SearchRequestStats();

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
            testRequestStats.onPhaseEnd(ctx, new SearchRequestContext());
            assertEquals(0, testRequestStats.getPhaseCurrent(searchPhaseName));
            assertEquals(1, testRequestStats.getPhaseTotal(searchPhaseName));
            assertThat(testRequestStats.getPhaseMetric(searchPhaseName), greaterThanOrEqualTo(tookTimeInMillis));
        }
    }

    public void testSearchRequestStatsOnPhaseStartConcurrently() throws InterruptedException {
        SearchRequestStats testRequestStats = new SearchRequestStats();
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
        SearchRequestStats testRequestStats = new SearchRequestStats();
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
                    testRequestStats.onPhaseEnd(ctx, new SearchRequestContext());
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
        SearchRequestStats testRequestStats = new SearchRequestStats();
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
