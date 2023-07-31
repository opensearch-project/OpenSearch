/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;

public class SearchRequestStatsTests extends OpenSearchTestCase {
    public void testSearchRequestPhaseFailure() {
        SearchRequestStats testRequestStats = new SearchRequestStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);

        testRequestStats.onDFSPreQueryPhaseStart(ctx);
        assertEquals(1, testRequestStats.getDFSPreQueryCurrent());

        testRequestStats.onDFSPreQueryPhaseFailure(ctx);
        assertEquals(0, testRequestStats.getDFSPreQueryCurrent());

        testRequestStats.onCanMatchPhaseStart(ctx);
        assertEquals(1, testRequestStats.getCanMatchCurrent());

        testRequestStats.onCanMatchPhaseFailure(ctx);
        assertEquals(0, testRequestStats.getCanMatchCurrent());

        testRequestStats.onQueryPhaseStart(ctx);
        assertEquals(1, testRequestStats.getQueryCurrent());

        testRequestStats.onQueryPhaseFailure(ctx);
        assertEquals(0, testRequestStats.getQueryCurrent());

        testRequestStats.onFetchPhaseStart(ctx);
        assertEquals(1, testRequestStats.getFetchCurrent());

        testRequestStats.onFetchPhaseFailure(ctx);
        assertEquals(0, testRequestStats.getFetchCurrent());

        testRequestStats.onExpandSearchPhaseStart(ctx);
        assertEquals(1, testRequestStats.getExpandSearchCurrent());

        testRequestStats.onExpandSearchPhaseFailure(ctx);
        assertEquals(0, testRequestStats.getExpandSearchCurrent());
    }

    public void testSearchRequestStats() {
        SearchRequestStats testRequestStats = new SearchRequestStats();

        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        long tookTime = randomIntBetween(1, 10);

        testRequestStats.onDFSPreQueryPhaseStart(ctx);
        assertEquals(1, testRequestStats.getDFSPreQueryCurrent());

        testRequestStats.onDFSPreQueryPhaseEnd(ctx, tookTime);
        assertEquals(0, testRequestStats.getDFSPreQueryCurrent());
        assertEquals(1, testRequestStats.getDFSPreQueryTotal());
        assertEquals(tookTime, testRequestStats.getDFSPreQueryMetric());

        testRequestStats.onCanMatchPhaseStart(ctx);
        assertEquals(1, testRequestStats.getCanMatchCurrent());

        testRequestStats.onCanMatchPhaseEnd(ctx, tookTime);
        assertEquals(0, testRequestStats.getCanMatchCurrent());
        assertEquals(1, testRequestStats.getCanMatchTotal());
        assertEquals(tookTime, testRequestStats.getCanMatchMetric());

        testRequestStats.onQueryPhaseStart(ctx);
        assertEquals(1, testRequestStats.getQueryCurrent());

        testRequestStats.onQueryPhaseEnd(ctx, tookTime);
        assertEquals(0, testRequestStats.getQueryCurrent());
        assertEquals(1, testRequestStats.getQueryTotal());
        assertEquals(tookTime, testRequestStats.getQueryMetric());

        testRequestStats.onFetchPhaseStart(ctx);
        assertEquals(1, testRequestStats.getFetchCurrent());

        testRequestStats.onFetchPhaseEnd(ctx, tookTime);
        assertEquals(0, testRequestStats.getFetchCurrent());
        assertEquals(1, testRequestStats.getFetchTotal());
        assertEquals(tookTime, testRequestStats.getFetchMetric());

        testRequestStats.onExpandSearchPhaseStart(ctx);
        assertEquals(1, testRequestStats.getExpandSearchCurrent());

        testRequestStats.onExpandSearchPhaseEnd(ctx, tookTime);
        assertEquals(0, testRequestStats.getExpandSearchCurrent());
        assertEquals(1, testRequestStats.getExpandSearchTotal());
        assertEquals(tookTime, testRequestStats.getExpandSearchMetric());
    }

    public void testSearchRequestStatsOnDFSPreQueryStartConcurrently() throws InterruptedException {
        SearchRequestStats testRequestStats = new SearchRequestStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        int numTasks = randomIntBetween(5, 50);
        Thread[] threads = new Thread[numTasks];
        Phaser phaser = new Phaser(numTasks + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                testRequestStats.onDFSPreQueryPhaseStart(ctx);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        assertEquals(numTasks, testRequestStats.getDFSPreQueryCurrent());
    }

    public void testSearchRequestStatsOnCanMatchStartConcurrently() throws InterruptedException {
        SearchRequestStats testRequestStats = new SearchRequestStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        int numTasks = randomIntBetween(5, 50);
        Thread[] threads = new Thread[numTasks];
        Phaser phaser = new Phaser(numTasks + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                testRequestStats.onCanMatchPhaseStart(ctx);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        assertEquals(numTasks, testRequestStats.getCanMatchCurrent());
    }

    public void testSearchRequestStatsOnQueryStartConcurrently() throws InterruptedException {
        SearchRequestStats testRequestStats = new SearchRequestStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        int numTasks = randomIntBetween(5, 50);
        Thread[] threads = new Thread[numTasks];
        Phaser phaser = new Phaser(numTasks + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                testRequestStats.onQueryPhaseStart(ctx);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        assertEquals(numTasks, testRequestStats.getQueryCurrent());
    }

    public void testSearchRequestStatsOnFetchStartConcurrently() throws InterruptedException {
        SearchRequestStats testRequestStats = new SearchRequestStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        int numTasks = randomIntBetween(5, 50);
        Thread[] threads = new Thread[numTasks];
        Phaser phaser = new Phaser(numTasks + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                testRequestStats.onFetchPhaseStart(ctx);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        assertEquals(numTasks, testRequestStats.getFetchCurrent());
    }

    public void testSearchRequestStatsOnExpandSearchStartConcurrently() throws InterruptedException {
        SearchRequestStats testRequestStats = new SearchRequestStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        int numTasks = randomIntBetween(5, 50);
        Thread[] threads = new Thread[numTasks];
        Phaser phaser = new Phaser(numTasks + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                testRequestStats.onExpandSearchPhaseStart(ctx);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        assertEquals(numTasks, testRequestStats.getExpandSearchCurrent());
    }

    public void testSearchRequestStatsOnDFSPreQueryEndConcurrently() throws InterruptedException {
        SearchRequestStats testRequestStats = new SearchRequestStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        int numTasks = randomIntBetween(5, 50);
        long tookTime = randomIntBetween(1, 10);
        Thread[] threads = new Thread[numTasks];
        Phaser phaser = new Phaser(numTasks + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                testRequestStats.onDFSPreQueryPhaseEnd(ctx, tookTime);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        assertEquals(numTasks, testRequestStats.getDFSPreQueryTotal());
        assertEquals(tookTime * numTasks, testRequestStats.getDFSPreQueryMetric());
    }

    public void testSearchRequestStatsOnCanMatchQueryEndConcurrently() throws InterruptedException {
        SearchRequestStats testRequestStats = new SearchRequestStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        int numTasks = randomIntBetween(5, 50);
        long tookTime = randomIntBetween(1, 10);
        Thread[] threads = new Thread[numTasks];
        Phaser phaser = new Phaser(numTasks + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                testRequestStats.onCanMatchPhaseEnd(ctx, tookTime);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        assertEquals(numTasks, testRequestStats.getCanMatchTotal());
        assertEquals(tookTime * numTasks, testRequestStats.getCanMatchMetric());
    }

    public void testSearchRequestStatsOnQueryEndConcurrently() throws InterruptedException {
        SearchRequestStats testRequestStats = new SearchRequestStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        int numTasks = randomIntBetween(5, 50);
        long tookTime = randomIntBetween(1, 10);
        Thread[] threads = new Thread[numTasks];
        Phaser phaser = new Phaser(numTasks + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                testRequestStats.onQueryPhaseEnd(ctx, tookTime);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        assertEquals(numTasks, testRequestStats.getQueryTotal());
        assertEquals(tookTime * numTasks, testRequestStats.getQueryMetric());
    }

    public void testSearchRequestStatsOnFetchEndConcurrently() throws InterruptedException {
        SearchRequestStats testRequestStats = new SearchRequestStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        int numTasks = randomIntBetween(5, 50);
        long tookTime = randomIntBetween(1, 10);
        Thread[] threads = new Thread[numTasks];
        Phaser phaser = new Phaser(numTasks + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                testRequestStats.onFetchPhaseEnd(ctx, tookTime);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        assertEquals(numTasks, testRequestStats.getFetchTotal());
        assertEquals(tookTime * numTasks, testRequestStats.getFetchMetric());
    }

    public void testSearchRequestStatsOnExpandSearchEndConcurrently() throws InterruptedException {
        SearchRequestStats testRequestStats = new SearchRequestStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        int numTasks = randomIntBetween(5, 50);
        long tookTime = randomIntBetween(1, 10);
        Thread[] threads = new Thread[numTasks];
        Phaser phaser = new Phaser(numTasks + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                testRequestStats.onExpandSearchPhaseEnd(ctx, tookTime);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        assertEquals(numTasks, testRequestStats.getExpandSearchTotal());
        assertEquals(tookTime * numTasks, testRequestStats.getExpandSearchMetric());
    }
}
