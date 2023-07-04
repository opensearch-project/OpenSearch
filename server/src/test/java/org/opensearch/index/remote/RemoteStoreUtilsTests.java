/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.test.OpenSearchTestCase;

public class RemoteStoreUtilsTests extends OpenSearchTestCase {

    public void testInvertToStrInvalid() {
        assertThrows(IllegalArgumentException.class, () -> RemoteStoreUtils.invertLong(-1));
    }

    public void testInvertToStrValid() {
        assertEquals("9223372036854774573", RemoteStoreUtils.invertLong(1234));
        assertEquals("0000000000000001234", RemoteStoreUtils.invertLong(9223372036854774573L));
    }

    public void testInvertToLongInvalid() {
        assertThrows(IllegalArgumentException.class, () -> RemoteStoreUtils.invertLong("-5"));
    }

    public void testInvertToLongValid() {
        assertEquals(1234, RemoteStoreUtils.invertLong("9223372036854774573"));
        assertEquals(9223372036854774573L, RemoteStoreUtils.invertLong("0000000000000001234"));
    }

    public void testinvert() {
        assertEquals(0, RemoteStoreUtils.invertLong(RemoteStoreUtils.invertLong(0)));
        assertEquals(Long.MAX_VALUE, RemoteStoreUtils.invertLong(RemoteStoreUtils.invertLong(Long.MAX_VALUE)));
        for (int i = 0; i < 10; i++) {
            long num = randomLongBetween(1, Long.MAX_VALUE);
            assertEquals(num, RemoteStoreUtils.invertLong(RemoteStoreUtils.invertLong(num)));
        }
    }
}
