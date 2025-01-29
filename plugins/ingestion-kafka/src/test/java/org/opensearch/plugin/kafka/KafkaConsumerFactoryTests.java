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

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerFactoryTests extends OpenSearchTestCase {
    public void testInitialize() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "test-topic");
        params.put("bootstrap_servers", "localhost:9092");

        factory.initialize(params);

        KafkaSourceConfig config = factory.config;
        Assert.assertNotNull("Config should be initialized", config);
        Assert.assertEquals("Topic should be correctly initialized", "test-topic", config.getTopic());
        Assert.assertEquals("Bootstrap servers should be correctly initialized", "localhost:9092", config.getBootstrapServers());
    }

    public void testParsePointerFromString() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        KafkaOffset offset = factory.parsePointerFromString("12345");

        Assert.assertNotNull("Offset should be parsed", offset);
        Assert.assertEquals("Offset value should be correctly parsed", 12345L, offset.getOffset());
    }
}
