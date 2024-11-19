/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.ingest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.engine.InternalEngine;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.KafkaContainer;

import java.util.Properties;

public class IngestionEngineTests extends EngineTestCase
{
    private static final String topicName = "test";

    KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        kafka.start();
        prepareKafkaData();
    }


    private void prepareKafkaData() {
        KafkaUtils.createTopic(topicName,1, "localhost:9092");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer
            <String, String>(props);
        producer.send(new ProducerRecord<String, String>(topicName, "1", "value1"));
        producer.send(new ProducerRecord<String, String>(topicName, "2", "value2"));

    }

    public void testIngestionEngineStart() {
        InternalEngine internalEngine = engine;
        EngineConfig engineConfig = internalEngine.config();
        engineConfig.getIndexSettings().getIndexMetadata().getIngestionSourceConfig();
        IngestionEngine ingestionEngine = new IngestionEngine(engineConfig);
    }
}
