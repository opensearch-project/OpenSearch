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

import org.opensearch.common.util.concurrent.RefCounted;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AutoCloseableRefCountedTests extends OpenSearchTestCase {

    private RefCounted mockRefCounted;
    private AutoCloseableRefCounted<RefCounted> testObject;

    @Before
    public void setup() {
        mockRefCounted = mock(RefCounted.class);
        testObject = new AutoCloseableRefCounted<>(mockRefCounted);
    }

    public void testGet() {
        assertEquals(mockRefCounted, testObject.get());
    }

    public void testClose() {
        testObject.close();
        verify(mockRefCounted, atMostOnce()).decRef();
    }

    public void testIdempotent() {
        testObject.close();
        testObject.close();
        verify(mockRefCounted, atMostOnce()).decRef();
    }
}
