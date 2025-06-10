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

import java.util.HashMap;
import java.util.Map;

public class KinesisConsumerFactoryTests extends OpenSearchTestCase {
    public void testConstructorAndGetters() {
        KinesisConsumerFactory factory = new KinesisConsumerFactory();
        Assert.assertNull("Config should be null before initialization", factory.config);
    }

    public void testInitializeWithValidParams() {
        KinesisConsumerFactory factory = new KinesisConsumerFactory();
        Map<String, Object> params = new HashMap<>();
        params.put("region", "us-west-2");
        params.put("stream", "testStream");
        params.put("secret_key", "testSecretKey");
        params.put("access_key", "testAccessKey");

        factory.initialize(params);

        Assert.assertNotNull("Config should be initialized", factory.config);
        Assert.assertEquals("Region should be correctly initialized", "us-west-2", factory.config.getRegion());
        Assert.assertEquals("Stream should be correctly initialized", "testStream", factory.config.getStream());
    }

    public void testInitializeWithNullParams() {
        KinesisConsumerFactory factory = new KinesisConsumerFactory();
        try {
            factory.initialize(null);
            Assert.fail("Initialization should throw an exception when params is null");
        } catch (NullPointerException e) {
            Assert.assertEquals("Cannot invoke \"java.util.Map.get(Object)\" because \"configuration\" is null", e.getMessage());
        }
    }

    public void testParsePointerFromString() {
        KinesisConsumerFactory factory = new KinesisConsumerFactory();
        SequenceNumber sequenceNumber = factory.parsePointerFromString("12345");

        Assert.assertNotNull("Sequence number should be parsed", sequenceNumber);
        Assert.assertEquals("Sequence number should be correctly parsed", "12345", sequenceNumber.getSequenceNumber());
    }
}
