/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.concurrent;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicInteger;

public class RefCountedReleasableTests extends OpenSearchTestCase {

    private AtomicInteger testRef;
    private RefCountedReleasable<AtomicInteger> testObject;

    @Before
    public void setup() {
        testRef = new AtomicInteger(0);
        testObject = new RefCountedReleasable<>("test", testRef, testRef::incrementAndGet);
    }

    public void testInitialState() {
        assertEquals("test", testObject.getName());
        assertEquals(testRef, testObject.get());
        assertEquals(testRef, testObject.get());
        assertEquals(0, testObject.get().get());
        assertEquals(1, testObject.refCount());
    }

    public void testIncRef() {
        testObject.incRef();
        assertEquals(2, testObject.refCount());
        assertEquals(0, testObject.get().get());
    }

    public void testCloseWithoutInternal() {
        testObject.incRef();
        assertEquals(2, testObject.refCount());
        testObject.close();
        assertEquals(1, testObject.refCount());
        assertEquals(0, testObject.get().get());
    }

    public void testCloseWithInternal() {
        assertEquals(1, testObject.refCount());
        testObject.close();
        assertEquals(0, testObject.refCount());
        assertEquals(1, testObject.get().get());
    }

    public void testIncRefAfterClose() {
        assertEquals(1, testObject.refCount());
        testObject.close();
        assertEquals(0, testObject.refCount());
        assertEquals(1, testObject.get().get());
        assertThrows(IllegalStateException.class, () -> testObject.incRef());
    }
}
