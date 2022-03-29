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

import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicInteger;

public class GatedAutoCloseableTests extends OpenSearchTestCase {

    private AtomicInteger testRef;
    private GatedAutoCloseable<AtomicInteger> testObject;

    @Before
    public void setup() {
        testRef = new AtomicInteger(0);
        testObject = new GatedAutoCloseable<>(testRef, testRef::incrementAndGet);
    }

    public void testGet() {
        assertEquals(0, testObject.get().get());
    }

    public void testClose() {
        testObject.close();
        assertEquals(1, testObject.get().get());
    }

    public void testIdempotent() {
        testObject.close();
        testObject.close();
        assertEquals(1, testObject.get().get());
    }
}
