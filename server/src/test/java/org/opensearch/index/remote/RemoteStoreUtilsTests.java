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

    public void testGetSegmentNameForCfeFile() {
        assertEquals("_foo", RemoteStoreUtils.getSegmentName("_foo.cfe"));
    }

    public void testGetSegmentNameForDvmFile() {
        assertEquals("_bar", RemoteStoreUtils.getSegmentName("_bar_1_Lucene90_0.dvm"));
    }

    public void testGetSegmentNameWeirdSegmentNameOnlyUnderscore() {
        // Validate behaviour when segment name contains delimiters only
        assertEquals("_", RemoteStoreUtils.getSegmentName("_.dvm"));
    }

    public void testGetSegmentNameUnderscoreDelimiterOverrides() {
        // Validate behaviour when segment name contains delimiters only
        assertEquals("_", RemoteStoreUtils.getSegmentName("___.dvm"));
    }

    public void testGetSegmentNameException() {
        assertThrows(IllegalArgumentException.class, () -> RemoteStoreUtils.getSegmentName("dvd"));
    }
}
