/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConsumerFactoryTests extends OpenSearchTestCase {
    public void testInitialize() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "test-topic");
        params.put("bootstrap_servers", "localhost:9092");

        factory.initialize(new IngestionSource.Builder("KAFKA").setParams(params).build());

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

    public void testParsePartitionOffsetFromString() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        KafkaOffset offset = factory.parsePointerFromString("3:42");

        Assert.assertNotNull("Offset should be parsed", offset);
        Assert.assertTrue("Should be KafkaPartitionOffset", offset instanceof KafkaPartitionOffset);
        KafkaPartitionOffset partitionOffset = (KafkaPartitionOffset) offset;
        Assert.assertEquals("Partition should be correctly parsed", 3, partitionOffset.getPartition());
        Assert.assertEquals("Offset should be correctly parsed", 42L, partitionOffset.getOffset());
    }

    public void testGetSourcePartitionCountBeforeInitialize() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        // Before initialize, config is null — calling getSourcePartitionCount should fail gracefully
        expectThrows(AssertionError.class, factory::getSourcePartitionCount);
    }

    public void testCreateMultiPartitionShardConsumerSinglePartition() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "test-topic");
        params.put("bootstrap_servers", "localhost:9092");
        factory.initialize(new IngestionSource.Builder("KAFKA").setParams(params).build());

        // Single partition delegates to createShardConsumer — will fail connecting to Kafka
        // but we verify it doesn't throw UnsupportedOperationException
        try {
            factory.createMultiPartitionShardConsumer("test-client", 0, List.of(0));
            fail("Expected exception connecting to Kafka");
        } catch (UnsupportedOperationException e) {
            fail("Single partition should not throw UnsupportedOperationException");
        } catch (Exception e) {
            // Expected — Kafka broker not available in unit test
        }
    }

    public void testCreateMultiPartitionShardConsumerMultiplePartitions() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "test-topic");
        params.put("bootstrap_servers", "localhost:9092");
        factory.initialize(new IngestionSource.Builder("KAFKA").setParams(params).build());

        // Multiple partitions creates KafkaMultiPartitionConsumer — will fail connecting to Kafka
        // but we verify it doesn't throw UnsupportedOperationException
        try {
            factory.createMultiPartitionShardConsumer("test-client", 0, List.of(0, 4, 8));
            fail("Expected exception connecting to Kafka");
        } catch (UnsupportedOperationException e) {
            fail("Multi-partition should not throw UnsupportedOperationException");
        } catch (Exception e) {
            // Expected — Kafka broker not available in unit test
        }
    }
}
