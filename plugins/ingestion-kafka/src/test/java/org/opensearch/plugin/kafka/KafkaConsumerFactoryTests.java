/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerFactoryTests extends OpenSearchTestCase {
    public void testCreateShardConsumerWithNullMetadata() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        expectThrows(NullPointerException.class, () -> factory.createShardConsumer("test-client", 0, (IndexMetadata) null));
    }

    public void testParsePointerFromString() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        KafkaOffset offset = factory.parsePointerFromString("12345");

        Assert.assertNotNull("Offset should be parsed", offset);
        Assert.assertEquals("Offset value should be correctly parsed", 12345L, offset.getOffset());
    }

    public void testConfiguratorsAdjustTheConsumerPropertiesInOrder() {
        IndexMetadata indexMetadata = IndexMetadata.builder("orders")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        List<String> calls = new ArrayList<>();
        KafkaClientConfigurator first = (index, clientType, properties) -> {
            calls.add("first " + index.getIndex().getName() + " " + clientType);
            properties.put("security.protocol", "SSL");
        };
        KafkaClientConfigurator second = (index, clientType, properties) -> {
            calls.add("second " + properties.get("security.protocol"));
            properties.put("security.protocol", "SASL_SSL");
        };

        Properties properties = new Properties();
        new KafkaConsumerFactory(List.of(first, second)).customizer(indexMetadata).accept(properties);

        assertEquals(List.of("first orders CONSUMER", "second SSL"), calls);
        assertEquals("SASL_SSL", properties.get("security.protocol"));
    }

    public void testWithoutConfiguratorsThePropertiesAreUnchanged() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        new KafkaConsumerFactory().customizer(null).accept(properties);
        assertEquals(1, properties.size());
    }
}
