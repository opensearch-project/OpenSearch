/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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
            when(mockSearchPhase.getName()).thenReturn(searchPhaseName.getName());
            testRequestStats.onPhaseStart(ctx);
            assertEquals(getExpectedCount(1).apply(searchPhaseName).intValue(), testRequestStats.getPhaseCurrent(searchPhaseName));
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
            when(mockSearchPhase.getName()).thenReturn(searchPhaseName.getName());
            long tookTimeInMillis = randomIntBetween(1, 10);
            testRequestStats.onPhaseStart(ctx);
            long startTime = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(tookTimeInMillis);
            when(mockSearchPhase.getStartTimeInNanos()).thenReturn(startTime);
            assertEquals(getExpectedCount(1).apply(searchPhaseName).intValue(), testRequestStats.getPhaseCurrent(searchPhaseName));
            testRequestStats.onPhaseEnd(ctx);
            assertEquals(0, testRequestStats.getPhaseCurrent(searchPhaseName));
            assertEquals(getExpectedCount(1).apply(searchPhaseName).intValue(), testRequestStats.getPhaseTotal(searchPhaseName));
            assertThat(
                testRequestStats.getPhaseMetric(searchPhaseName),
                greaterThanOrEqualTo(getExpectedCount((int) tookTimeInMillis).apply(searchPhaseName).longValue())
            );
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
            when(mockSearchPhase.getName()).thenReturn(searchPhaseName.getName());
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
        for (SearchPhaseName searchPhaseName : getSearchPhaseNames()) {
            if (SearchPhaseName.QUERY.equals(searchPhaseName)) {
                assertEquals(numTasks * 2, testRequestStats.getPhaseCurrent(searchPhaseName));
                continue;
            } else {
                assertEquals(
                    getExpectedCount(numTasks).apply(searchPhaseName).intValue(),
                    testRequestStats.getPhaseCurrent(searchPhaseName)
                );
            }
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
            when(mockSearchPhase.getName()).thenReturn(searchPhaseName.getName());
            long tookTimeInMillis = randomIntBetween(1, 10);
            long startTime = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(tookTimeInMillis);
            when(mockSearchPhase.getStartTimeInNanos()).thenReturn(startTime);
            for (int i = 0; i < numTasks; i++) {
                threads[i] = new Thread(() -> {
                    phaser.arriveAndAwaitAdvance();
                    testRequestStats.onPhaseEnd(ctx);
                    countDownLatch.countDown();
                });
                threads[i].start();
            }
            searchPhaseNameLongMap.put(searchPhaseName, tookTimeInMillis);
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        for (SearchPhaseName searchPhaseName : getSearchPhaseNames()) {
            if (SearchPhaseName.QUERY.equals(searchPhaseName)) {
                assertEquals(numTasks * 2, testRequestStats.getPhaseTotal(searchPhaseName));
                assertThat(
                    testRequestStats.getPhaseMetric(searchPhaseName),
                    greaterThanOrEqualTo(searchPhaseNameLongMap.get(searchPhaseName) * numTasks * 2)
                );
            } else {
                assertEquals(getExpectedCount(numTasks).apply(searchPhaseName).intValue(), testRequestStats.getPhaseTotal(searchPhaseName));
                assertThat(
                    testRequestStats.getPhaseMetric(searchPhaseName),
                    greaterThanOrEqualTo(
                        getExpectedCount((int) (searchPhaseNameLongMap.get(searchPhaseName) * numTasks)).apply(searchPhaseName).longValue()
                    )
                );
            }
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
            when(mockSearchPhase.getName()).thenReturn(searchPhaseName.getName());
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
        for (SearchPhaseName searchPhaseName : getSearchPhaseNames()) {
            assertEquals(0, testRequestStats.getPhaseCurrent(searchPhaseName));
        }
    }

    public void testSearchRequestStatsWithInvalidPhaseName() {
        SearchRequestStats testRequestStats = new SearchRequestStats();
        Map<String, String> stringStringMap = new HashMap<>();
        stringStringMap.computeIfAbsent(null, s -> { return "dummy"; });
        SearchPhaseContext ctx = mock(SearchPhaseContext.class);
        SearchPhase mockSearchPhase = mock(SearchPhase.class);
        when(ctx.getCurrentPhase()).thenReturn(mockSearchPhase);
        when(mockSearchPhase.getName()).thenReturn("dummy");
        testRequestStats.onPhaseStart(ctx);
        testRequestStats.onPhaseEnd(ctx);
        testRequestStats.onPhaseFailure(ctx);
        for (SearchPhaseName searchPhaseName : getSearchPhaseNames()) {
            assertEquals(0, testRequestStats.getPhaseCurrent(searchPhaseName));
            assertEquals(0, testRequestStats.getPhaseTotal(searchPhaseName));
            assertEquals(0, testRequestStats.getPhaseMetric(searchPhaseName));
        }
    }

    private List<SearchPhaseName> getSearchPhaseNames() {
        List<SearchPhaseName> searchPhaseNames = new ArrayList<>(Arrays.asList(SearchPhaseName.values()));
        searchPhaseNames.remove(SearchPhaseName.DFS_QUERY);
        return searchPhaseNames;
    }

    private Function<SearchPhaseName, Integer> getExpectedCount(int expected) {
        Function<SearchPhaseName, Integer> currentCount = searchPhaseName -> {
            if (SearchPhaseName.DFS_QUERY.equals(searchPhaseName)) {
                return 0;
            } else {
                return expected;
            }
        };
        return currentCount;
    }
}
