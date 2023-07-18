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

public class SearchCoordinatorStatsTests extends OpenSearchTestCase {
    public void testSearchCoordinatorPhaseFailure() {
        SearchCoordinatorStats testCoordinatorStats = new SearchCoordinatorStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);

        testCoordinatorStats.onDFSPreQueryPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getDFSPreQueryCurrent());

        testCoordinatorStats.onDFSPreQueryPhaseFailure(ctx);
        assertEquals(0, testCoordinatorStats.getDFSPreQueryCurrent());

        testCoordinatorStats.onCanMatchPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getCanMatchCurrent());

        testCoordinatorStats.onCanMatchPhaseFailure(ctx);
        assertEquals(0, testCoordinatorStats.getCanMatchCurrent());

        testCoordinatorStats.onQueryPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getQueryCurrent());

        testCoordinatorStats.onQueryPhaseFailure(ctx);
        assertEquals(0, testCoordinatorStats.getQueryCurrent());

        testCoordinatorStats.onFetchPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getFetchCurrent());

        testCoordinatorStats.onFetchPhaseFailure(ctx);
        assertEquals(0, testCoordinatorStats.getFetchCurrent());

        testCoordinatorStats.onExpandSearchPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getExpandSearchCurrent());

        testCoordinatorStats.onExpandSearchPhaseFailure(ctx);
        assertEquals(0, testCoordinatorStats.getExpandSearchCurrent());
    }

    public void testSearchCoordinatorStats() {
        SearchCoordinatorStats testCoordinatorStats = new SearchCoordinatorStats();

        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        long tookTime = 10;

        testCoordinatorStats.onDFSPreQueryPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getDFSPreQueryCurrent());

        testCoordinatorStats.onDFSPreQueryPhaseEnd(ctx, 10);
        assertEquals(0, testCoordinatorStats.getDFSPreQueryCurrent());
        assertEquals(1, testCoordinatorStats.getDFSPreQueryTotal());
        assertEquals(tookTime, testCoordinatorStats.getDFSPreQueryMetric());

        testCoordinatorStats.onCanMatchPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getCanMatchCurrent());

        testCoordinatorStats.onCanMatchPhaseEnd(ctx, 10);
        assertEquals(0, testCoordinatorStats.getCanMatchCurrent());
        assertEquals(1, testCoordinatorStats.getCanMatchTotal());
        assertEquals(tookTime, testCoordinatorStats.getCanMatchMetric());

        testCoordinatorStats.onQueryPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getQueryCurrent());

        testCoordinatorStats.onQueryPhaseEnd(ctx, 10);
        assertEquals(0, testCoordinatorStats.getQueryCurrent());
        assertEquals(1, testCoordinatorStats.getQueryTotal());
        assertEquals(tookTime, testCoordinatorStats.getQueryMetric());

        testCoordinatorStats.onFetchPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getFetchCurrent());

        testCoordinatorStats.onFetchPhaseEnd(ctx, 10);
        assertEquals(0, testCoordinatorStats.getFetchCurrent());
        assertEquals(1, testCoordinatorStats.getFetchTotal());
        assertEquals(tookTime, testCoordinatorStats.getFetchMetric());

        testCoordinatorStats.onExpandSearchPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getExpandSearchCurrent());

        testCoordinatorStats.onExpandSearchPhaseEnd(ctx, 10);
        assertEquals(0, testCoordinatorStats.getExpandSearchCurrent());
        assertEquals(1, testCoordinatorStats.getExpandSearchTotal());
        assertEquals(tookTime, testCoordinatorStats.getExpandSearchMetric());
    }

    public void testSearchCoordinatorStatsMulti() throws InterruptedException {
        SearchCoordinatorStats testCoordinatorStats = new SearchCoordinatorStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        long tookTime = 10;
        int numTasks = randomIntBetween(5, 50);

        Thread[] threads = new Thread[numTasks];

        Phaser phaser = new Phaser(numTasks);

        CountDownLatch countDownLatch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                testCoordinatorStats.onDFSPreQueryPhaseStart(ctx);

                phaser.arriveAndAwaitAdvance();
                testCoordinatorStats.onDFSPreQueryPhaseEnd(ctx, tookTime);

                phaser.arriveAndAwaitAdvance();
                testCoordinatorStats.onCanMatchPhaseStart(ctx);

                phaser.arriveAndAwaitAdvance();
                testCoordinatorStats.onCanMatchPhaseEnd(ctx, tookTime);

                phaser.arriveAndAwaitAdvance();
                testCoordinatorStats.onQueryPhaseStart(ctx);

                phaser.arriveAndAwaitAdvance();
                testCoordinatorStats.onQueryPhaseEnd(ctx, tookTime);

                phaser.arriveAndAwaitAdvance();
                testCoordinatorStats.onFetchPhaseStart(ctx);

                phaser.arriveAndAwaitAdvance();
                testCoordinatorStats.onFetchPhaseEnd(ctx, tookTime);

                phaser.arriveAndAwaitAdvance();
                testCoordinatorStats.onExpandSearchPhaseStart(ctx);

                phaser.arriveAndAwaitAdvance();
                testCoordinatorStats.onExpandSearchPhaseEnd(ctx, tookTime);
                countDownLatch.countDown();
            });
            threads[i].start();
        }

        countDownLatch.await();

        assertEquals(numTasks, testCoordinatorStats.getDFSPreQueryTotal());
        assertEquals(numTasks * tookTime, testCoordinatorStats.getDFSPreQueryMetric());

        assertEquals(numTasks, testCoordinatorStats.getCanMatchTotal());
        assertEquals(numTasks * tookTime, testCoordinatorStats.getCanMatchMetric());

        assertEquals(numTasks, testCoordinatorStats.getQueryTotal());
        assertEquals(numTasks * tookTime, testCoordinatorStats.getQueryMetric());

        assertEquals(numTasks, testCoordinatorStats.getFetchTotal());
        assertEquals(numTasks * tookTime, testCoordinatorStats.getFetchMetric());

        assertEquals(numTasks, testCoordinatorStats.getExpandSearchTotal());
        assertEquals(numTasks * tookTime, testCoordinatorStats.getExpandSearchMetric());
    }
}
