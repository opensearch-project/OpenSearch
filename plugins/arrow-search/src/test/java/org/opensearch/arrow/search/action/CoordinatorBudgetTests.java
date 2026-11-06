/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.action;

import org.opensearch.common.lease.Releasable;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicInteger;

public class CoordinatorBudgetTests extends OpenSearchTestCase {

    public void testReservesWhatIsFreeAboveTheMinimum() {
        CoordinatorBudget budget = new CoordinatorBudget(() -> 100);
        CoordinatorBudget.Reservation first = budget.reserve(64, 10);
        assertEquals(64, first.bytes());
        CoordinatorBudget.Reservation second = budget.reserve(64, 10);
        assertEquals(36, second.bytes());
        assertNull(budget.reserve(64, 10));
        assertEquals(100, budget.reserved());
    }

    public void testBytesReturnOnceTheSearchAndItsBatchesReleasedThem() {
        CoordinatorBudget budget = new CoordinatorBudget(() -> 100);
        CoordinatorBudget.Reservation reservation = budget.reserve(64, 10);
        Releasable batch = reservation.holdBatch(64);
        assertEquals("a running search keeps its whole reservation", 64, budget.reserved());

        assertTrue("a batch the client has not read still holds the reservation", reservation.endSearch());
        assertFalse(reservation.isReleased());
        assertEquals(64, budget.reserved());
        assertNull(budget.reserve(64, 40));

        batch.close();
        assertTrue(reservation.isReleased());
        assertEquals(0, budget.reserved());
    }

    public void testOnlyTheBatchesStillHeldCountOnceTheSearchEnded() {
        CoordinatorBudget budget = new CoordinatorBudget(() -> 100);
        CoordinatorBudget.Reservation reservation = budget.reserve(64, 10);
        Releasable first = reservation.holdBatch(10);
        Releasable second = reservation.holdBatch(10);
        assertEquals(64, budget.reserved());

        assertTrue(reservation.endSearch());
        assertEquals("the unread batches count, not what the search reserved for its streams", 20, budget.reserved());
        assertNotNull("the freed bytes admit other searches", budget.reserve(80, 80));
        first.close();
        first.close();
        assertEquals(90, budget.reserved());
        second.close();
        assertTrue(reservation.isReleased());
        assertEquals(80, budget.reserved());
    }

    public void testEndingTheSearchAgainHasNoEffect() {
        CoordinatorBudget budget = new CoordinatorBudget(() -> 100);
        CoordinatorBudget.Reservation reservation = budget.reserve(64, 10);
        Releasable batch = reservation.holdBatch(10);
        assertTrue(reservation.endSearch());
        assertFalse(reservation.endSearch());
        assertFalse(reservation.isReleased());
        assertEquals("the batch still holds its bytes", 10, budget.reserved());
        batch.close();
        assertEquals(0, budget.reserved());
    }

    public void testNoBatchHoldsAReservationWhoseSearchEnded() {
        CoordinatorBudget budget = new CoordinatorBudget(() -> 100);
        CoordinatorBudget.Reservation reservation = budget.reserve(64, 10);
        Releasable batch = reservation.holdBatch(10);
        reservation.endSearch();
        assertNull(reservation.holdBatch(10));
        assertEquals(10, budget.reserved());
        batch.close();
        assertNull(reservation.holdBatch(10));
        assertEquals(0, budget.reserved());
    }

    public void testReleaseCallbackRunsOnce() {
        CoordinatorBudget budget = new CoordinatorBudget(() -> 100);
        CoordinatorBudget.Reservation reservation = budget.reserve(64, 10);
        AtomicInteger released = new AtomicInteger();
        reservation.whenReleased(released::incrementAndGet);
        Releasable batch = reservation.holdBatch(10);
        reservation.endSearch();
        assertEquals(0, released.get());
        batch.close();
        assertEquals(1, released.get());

        AtomicInteger late = new AtomicInteger();
        reservation.whenReleased(late::incrementAndGet);
        assertEquals("a callback registered after the release runs at once", 1, late.get());
        assertEquals(1, released.get());
    }
}
