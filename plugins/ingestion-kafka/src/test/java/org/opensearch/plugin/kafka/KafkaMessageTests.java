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

        KafkaMessage message = new KafkaMessage(key, payload);

        Assert.assertArrayEquals(key, message.getKey());
        Assert.assertArrayEquals(payload, message.getPayload());
    }

    public void testConstructorWithNullKey() {
        byte[] payload = { 4, 5, 6 };

        KafkaMessage message = new KafkaMessage(null, payload);

        assertNull(message.getKey());
        Assert.assertArrayEquals(payload, message.getPayload());
    }
}
