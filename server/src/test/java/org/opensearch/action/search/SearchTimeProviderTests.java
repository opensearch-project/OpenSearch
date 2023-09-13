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

public class SearchTimeProviderTests extends OpenSearchTestCase {
    public void testSearchRequestPhaseFailure() {
        TransportSearchAction.SearchTimeProvider testRequestStats = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);

        testRequestStats.onDFSPreQueryPhaseStart(ctx);
        testRequestStats.onDFSPreQueryPhaseFailure(ctx);
        assertEquals(0, testRequestStats.getDFSPreQueryTotal());

        testRequestStats.onCanMatchPhaseStart(ctx);
        testRequestStats.onCanMatchPhaseFailure(ctx);
        assertEquals(0, testRequestStats.getCanMatchTotal());

        testRequestStats.onQueryPhaseStart(ctx);
        testRequestStats.onQueryPhaseFailure(ctx);
        assertEquals(0, testRequestStats.getQueryTotal());

        testRequestStats.onFetchPhaseStart(ctx);
        testRequestStats.onFetchPhaseFailure(ctx);
        assertEquals(0, testRequestStats.getFetchTotal());

        testRequestStats.onExpandSearchPhaseStart(ctx);
        testRequestStats.onExpandSearchPhaseFailure(ctx);
        assertEquals(0, testRequestStats.getExpandSearchTotal());
    }

    public void testSearchTimeProvider() {
        TransportSearchAction.SearchTimeProvider testRequestStats = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);

        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        long tookTime = randomIntBetween(1, 10);

        testRequestStats.onDFSPreQueryPhaseStart(ctx);
        testRequestStats.onDFSPreQueryPhaseEnd(ctx, tookTime);
        assertEquals(tookTime, testRequestStats.getDFSPreQueryTotal());

        testRequestStats.onCanMatchPhaseStart(ctx);
        testRequestStats.onCanMatchPhaseEnd(ctx, tookTime);
        assertEquals(tookTime, testRequestStats.getCanMatchTotal());

        testRequestStats.onQueryPhaseStart(ctx);
        testRequestStats.onQueryPhaseEnd(ctx, tookTime);
        assertEquals(tookTime, testRequestStats.getQueryTotal());

        testRequestStats.onFetchPhaseStart(ctx);
        testRequestStats.onFetchPhaseEnd(ctx, tookTime);
        testRequestStats.onFetchPhaseEnd(ctx, 10);
        assertEquals(tookTime + 10, testRequestStats.getFetchTotal());

        testRequestStats.onExpandSearchPhaseStart(ctx);
        testRequestStats.onExpandSearchPhaseEnd(ctx, tookTime);
        testRequestStats.onExpandSearchPhaseEnd(ctx, tookTime);
        assertEquals(2 * tookTime, testRequestStats.getExpandSearchTotal());
    }

    public void testSearchTimeProviderOnDFSPreQueryEndConcurrently() throws InterruptedException {
        TransportSearchAction.SearchTimeProvider testRequestStats = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
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
        assertEquals(tookTime * numTasks, testRequestStats.getDFSPreQueryTotal());
    }

    public void testSearchTimeProviderOnCanMatchQueryEndConcurrently() throws InterruptedException {
        TransportSearchAction.SearchTimeProvider testRequestStats = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
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
        assertEquals(tookTime * numTasks, testRequestStats.getCanMatchTotal());
    }

    public void testSearchTimeProviderOnQueryEndConcurrently() throws InterruptedException {
        TransportSearchAction.SearchTimeProvider testRequestStats = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
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
        assertEquals(tookTime * numTasks, testRequestStats.getQueryTotal());
    }

    public void testSearchTimeProviderOnFetchEndConcurrently() throws InterruptedException {
        TransportSearchAction.SearchTimeProvider testRequestStats = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
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
        assertEquals(tookTime * numTasks, testRequestStats.getFetchTotal());
    }

    public void testSearchTimeProviderOnExpandSearchEndConcurrently() throws InterruptedException {
        TransportSearchAction.SearchTimeProvider testRequestStats = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
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
        assertEquals(tookTime * numTasks, testRequestStats.getExpandSearchTotal());
    }
}
