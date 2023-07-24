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
        long tookTime = 10;

        testRequestStats.onDFSPreQueryPhaseStart(ctx);
        assertEquals(1, testRequestStats.getDFSPreQueryCurrent());

        testRequestStats.onDFSPreQueryPhaseEnd(ctx, 10);
        assertEquals(0, testRequestStats.getDFSPreQueryCurrent());
        assertEquals(1, testRequestStats.getDFSPreQueryTotal());
        assertEquals(tookTime, testRequestStats.getDFSPreQueryMetric());

        testRequestStats.onCanMatchPhaseStart(ctx);
        assertEquals(1, testRequestStats.getCanMatchCurrent());

        testRequestStats.onCanMatchPhaseEnd(ctx, 10);
        assertEquals(0, testRequestStats.getCanMatchCurrent());
        assertEquals(1, testRequestStats.getCanMatchTotal());
        assertEquals(tookTime, testRequestStats.getCanMatchMetric());

        testRequestStats.onQueryPhaseStart(ctx);
        assertEquals(1, testRequestStats.getQueryCurrent());

        testRequestStats.onQueryPhaseEnd(ctx, 10);
        assertEquals(0, testRequestStats.getQueryCurrent());
        assertEquals(1, testRequestStats.getQueryTotal());
        assertEquals(tookTime, testRequestStats.getQueryMetric());

        testRequestStats.onFetchPhaseStart(ctx);
        assertEquals(1, testRequestStats.getFetchCurrent());

        testRequestStats.onFetchPhaseEnd(ctx, 10);
        assertEquals(0, testRequestStats.getFetchCurrent());
        assertEquals(1, testRequestStats.getFetchTotal());
        assertEquals(tookTime, testRequestStats.getFetchMetric());

        testRequestStats.onExpandSearchPhaseStart(ctx);
        assertEquals(1, testRequestStats.getExpandSearchCurrent());

        testRequestStats.onExpandSearchPhaseEnd(ctx, 10);
        assertEquals(0, testRequestStats.getExpandSearchCurrent());
        assertEquals(1, testRequestStats.getExpandSearchTotal());
        assertEquals(tookTime, testRequestStats.getExpandSearchMetric());
    }

    public void testSearchRequestStatsMulti() throws InterruptedException {
        SearchRequestStats testRequestStats = new SearchRequestStats();
        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        long tookTime = 10;
        int numTasks = randomIntBetween(5, 50);

        Thread[] threads = new Thread[numTasks];

        Phaser phaser = new Phaser(numTasks);

        CountDownLatch countDownLatch = new CountDownLatch(numTasks);
        for (int i = 0; i < numTasks; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                testRequestStats.onDFSPreQueryPhaseStart(ctx);

                phaser.arriveAndAwaitAdvance();
                testRequestStats.onDFSPreQueryPhaseEnd(ctx, tookTime);

                phaser.arriveAndAwaitAdvance();
                testRequestStats.onCanMatchPhaseStart(ctx);

                phaser.arriveAndAwaitAdvance();
                testRequestStats.onCanMatchPhaseEnd(ctx, tookTime);

                phaser.arriveAndAwaitAdvance();
                testRequestStats.onQueryPhaseStart(ctx);

                phaser.arriveAndAwaitAdvance();
                testRequestStats.onQueryPhaseEnd(ctx, tookTime);

                phaser.arriveAndAwaitAdvance();
                testRequestStats.onFetchPhaseStart(ctx);

                phaser.arriveAndAwaitAdvance();
                testRequestStats.onFetchPhaseEnd(ctx, tookTime);

                phaser.arriveAndAwaitAdvance();
                testRequestStats.onExpandSearchPhaseStart(ctx);

                phaser.arriveAndAwaitAdvance();
                testRequestStats.onExpandSearchPhaseEnd(ctx, tookTime);
                countDownLatch.countDown();
            });
            threads[i].start();
        }

        countDownLatch.await();

        assertEquals(numTasks, testRequestStats.getDFSPreQueryTotal());
        assertEquals(numTasks * tookTime, testRequestStats.getDFSPreQueryMetric());

        assertEquals(numTasks, testRequestStats.getCanMatchTotal());
        assertEquals(numTasks * tookTime, testRequestStats.getCanMatchMetric());

        assertEquals(numTasks, testRequestStats.getQueryTotal());
        assertEquals(numTasks * tookTime, testRequestStats.getQueryMetric());

        assertEquals(numTasks, testRequestStats.getFetchTotal());
        assertEquals(numTasks * tookTime, testRequestStats.getFetchMetric());

        assertEquals(numTasks, testRequestStats.getExpandSearchTotal());
        assertEquals(numTasks * tookTime, testRequestStats.getExpandSearchMetric());
    }
}
