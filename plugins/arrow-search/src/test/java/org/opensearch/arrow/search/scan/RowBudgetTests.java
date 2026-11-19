/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

public class RowBudgetTests extends OpenSearchTestCase {

    public void testClaimsStopAtTheLimit() {
        ScanState.RowBudget budget = new ScanState.RowBudget(10);
        assertEquals(4, budget.claim(4));
        assertEquals(6, budget.remaining());
        assertEquals(6, budget.claim(8));
        assertTrue(budget.exhausted());
        assertEquals(0, budget.claim(1));
        assertEquals(0, budget.remaining());
    }

    public void testNoLimitNeverRunsOut() {
        ScanState.RowBudget budget = new ScanState.RowBudget(Long.MAX_VALUE);
        assertEquals(1 << 20, budget.claim(1 << 20));
        assertFalse(budget.exhausted());
    }

    public void testZeroLimitIsExhaustedAtOnce() {
        ScanState.RowBudget budget = new ScanState.RowBudget(0);
        assertTrue(budget.exhausted());
        assertEquals(0, budget.claim(5));
    }

    public void testConcurrentSlicesNeverClaimMoreThanTheLimit() throws Exception {
        int slices = randomIntBetween(2, 8);
        long limit = randomLongBetween(1, 100_000);
        int[] claimSizes = new int[slices];
        for (int s = 0; s < slices; s++) {
            claimSizes[s] = randomIntBetween(1, 1024);
        }
        ScanState.RowBudget budget = new ScanState.RowBudget(limit);
        AtomicLong granted = new AtomicLong();
        CyclicBarrier start = new CyclicBarrier(slices);
        List<Thread> threads = new ArrayList<>();
        for (int s = 0; s < slices; s++) {
            int claimSize = claimSizes[s];
            threads.add(Thread.ofVirtual().start(() -> {
                try {
                    start.await();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                while (budget.exhausted() == false) {
                    granted.addAndGet(budget.claim(claimSize));
                }
            }));
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(limit, granted.get());
        assertEquals(0, budget.remaining());
    }
}
