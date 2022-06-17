/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.opensearch.test.OpenSearchTestCase;

public class AdjustableSemaphoreTests extends OpenSearchTestCase {

    public void testAdjustableSemaphore() {
        AdjustableSemaphore sem = new AdjustableSemaphore(1, randomBoolean());
        assertEquals(1, sem.availablePermits());
        assertTrue(sem.tryAcquire());
        assertEquals(0, sem.availablePermits());
        assertFalse(sem.tryAcquire());
        assertEquals(0, sem.availablePermits());

        // increase the number of max permits to 2
        sem.setMaxPermits(2);
        assertEquals(1, sem.availablePermits());
        assertTrue(sem.tryAcquire());
        assertEquals(0, sem.availablePermits());

        // release all current permits
        sem.release();
        assertEquals(1, sem.availablePermits());
        sem.release();
        assertEquals(2, sem.availablePermits());

        // reduce number of max permits to 1
        sem.setMaxPermits(1);
        assertEquals(1, sem.availablePermits());
        // set back to 2
        sem.setMaxPermits(2);
        assertEquals(2, sem.availablePermits());

        // take both permits and reduce max permits
        assertTrue(sem.tryAcquire());
        assertTrue(sem.tryAcquire());
        assertEquals(0, sem.availablePermits());
        assertFalse(sem.tryAcquire());
        sem.setMaxPermits(1);
        assertEquals(-1, sem.availablePermits());
        assertFalse(sem.tryAcquire());

        // release one permit
        sem.release();
        assertEquals(0, sem.availablePermits());
        assertFalse(sem.tryAcquire());

        // release second permit
        sem.release();
        assertEquals(1, sem.availablePermits());
        assertTrue(sem.tryAcquire());
    }
}
