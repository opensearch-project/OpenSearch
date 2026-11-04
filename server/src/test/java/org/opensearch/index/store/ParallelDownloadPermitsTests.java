/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.test.OpenSearchTestCase;

public class ParallelDownloadPermitsTests extends OpenSearchTestCase {

    public void testAcquireReleaseWithinBudget() {
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(2);
        assertEquals(2, permits.getMaxPermits());
        assertTrue(permits.tryAcquire());
        assertTrue(permits.tryAcquire());
        assertFalse("budget exhausted", permits.tryAcquire());
        permits.release();
        assertTrue(permits.tryAcquire());
        permits.release();
        permits.release();
        assertEquals(2, permits.availablePermits());
    }

    public void testZeroBudgetNeverGrants() {
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(0);
        assertFalse(permits.tryAcquire());
        assertEquals(0, permits.getMaxPermits());
    }

    public void testGrowBudget() {
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(1);
        assertTrue(permits.tryAcquire());
        assertFalse(permits.tryAcquire());
        permits.setMaxPermits(3);
        assertEquals(3, permits.getMaxPermits());
        assertTrue(permits.tryAcquire());
        assertTrue(permits.tryAcquire());
        assertFalse(permits.tryAcquire());
    }

    public void testShrinkBudgetTakesEffectAsPermitsAreReleased() {
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(3);
        assertTrue(permits.tryAcquire());
        assertTrue(permits.tryAcquire());
        assertTrue(permits.tryAcquire());
        permits.setMaxPermits(1);
        assertEquals(1, permits.getMaxPermits());
        // The three held permits are not revoked, but nothing new is granted until enough are returned.
        permits.release();
        assertFalse(permits.tryAcquire());
        permits.release();
        assertFalse(permits.tryAcquire());
        permits.release();
        assertTrue(permits.tryAcquire());
        assertFalse(permits.tryAcquire());
    }

    public void testNegativeBudgetRejected() {
        expectThrows(IllegalArgumentException.class, () -> new ParallelDownloadPermits(-1));
        expectThrows(IllegalArgumentException.class, () -> new ParallelDownloadPermits(1).setMaxPermits(-1));
    }
}
