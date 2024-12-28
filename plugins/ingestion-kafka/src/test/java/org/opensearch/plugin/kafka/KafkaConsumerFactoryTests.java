/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.junit.Assert;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class KafkaConsumerFactoryTests extends OpenSearchTestCase {
    public void testInitialize() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "test-topic");
        params.put("bootstrapServers", "localhost:9092");

        factory.initialize(params);

        KafkaSourceConfig config = factory.config;
        Assert.assertNotNull("Config should be initialized", config);
        Assert.assertEquals("Topic should be correctly initialized", "test-topic", config.getTopic());
        Assert.assertEquals("Bootstrap servers should be correctly initialized", "localhost:9092", config.getBootstrapServers());
    }

    public void testCreateShardConsumer() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "test-topic");
        params.put("bootstrapServers", "localhost:9092");

        factory.initialize(params);

        KafkaPartitionConsumer mockConsumer = mock(KafkaPartitionConsumer.class);
        when(mockConsumer.getClientId()).thenReturn("client1");
        when(mockConsumer.getShardId()).thenReturn(1);
        
        factory = spy(factory);
        doReturn(mockConsumer).when(factory).createShardConsumer("client1", 1);

        KafkaPartitionConsumer consumer = factory.createShardConsumer("client1", 1);

        Assert.assertNotNull("Consumer should be created", consumer);
        Assert.assertEquals("Client ID should be correctly set", "client1", consumer.getClientId());
        Assert.assertEquals("Shard ID should be correctly set", 1, consumer.getShardId());
    }

    public void testParsePointerFromString() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        KafkaOffset offset = factory.parsePointerFromString("12345");

        Assert.assertNotNull("Offset should be parsed", offset);
        Assert.assertEquals("Offset value should be correctly parsed", 12345L, offset.getOffset());
    }
}
