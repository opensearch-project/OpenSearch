/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

public class KinesisMessageTests extends OpenSearchTestCase {
    public void testConstructorAndGetters() {
        byte[] payload = { 1, 2, 3 };
        KinesisMessage message = new KinesisMessage(payload, 1000L);

        Assert.assertArrayEquals("Payload should be correctly initialized and returned", payload, message.getPayload());
        Assert.assertEquals(1000L, message.getTimestamp().longValue());
    }

    public void testConstructorWithNullPayload() {
        KinesisMessage message = new KinesisMessage(null, null);

        Assert.assertNull("Payload should be null", message.getPayload());
        Assert.assertNull("Timestamp should be null", message.getTimestamp());
    }
}
