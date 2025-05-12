/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

public class KafkaMessageTests extends OpenSearchTestCase {
    public void testConstructorAndGetters() {
        byte[] key = { 1, 2, 3 };
        byte[] payload = { 4, 5, 6 };

        KafkaMessage message = new KafkaMessage(key, payload, 1000L);

        Assert.assertArrayEquals(key, message.getKey());
        Assert.assertArrayEquals(payload, message.getPayload());
        Assert.assertEquals(1000L, message.getTimestamp().longValue());
    }

    public void testConstructorWithNullKey() {
        byte[] payload = { 4, 5, 6 };

        KafkaMessage message = new KafkaMessage(null, payload, null);

        assertNull(message.getKey());
        Assert.assertArrayEquals(payload, message.getPayload());
        Assert.assertNull(message.getTimestamp());
    }
}
