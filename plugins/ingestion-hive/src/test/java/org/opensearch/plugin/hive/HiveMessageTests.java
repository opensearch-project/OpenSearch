/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.opensearch.test.OpenSearchTestCase;

public class HiveMessageTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        byte[] payload = "{\"name\":\"test\"}".getBytes();
        HiveMessage message = new HiveMessage(payload, 1000L);

        assertArrayEquals(payload, message.getPayload());
        assertEquals(1000L, message.getTimestamp().longValue());
    }

    public void testNullTimestamp() {
        byte[] payload = "{\"name\":\"test\"}".getBytes();
        HiveMessage message = new HiveMessage(payload, null);

        assertArrayEquals(payload, message.getPayload());
        assertNull(message.getTimestamp());
    }
}
