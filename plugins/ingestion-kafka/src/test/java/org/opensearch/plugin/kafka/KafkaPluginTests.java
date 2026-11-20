/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Properties;

public class KafkaPluginTests extends OpenSearchTestCase {

    public void testExtensionsReachTheConsumerFactory() {
        KafkaClientConfigurator configurator = (index, clientType, properties) -> properties.put("security.protocol", "SASL_SSL");
        KafkaPlugin plugin = new KafkaPlugin();
        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                assertEquals(KafkaClientConfigurator.class, extensionPointType);
                return (List<T>) List.of(configurator);
            }
        });

        KafkaConsumerFactory factory = (KafkaConsumerFactory) plugin.getIngestionConsumerFactories().get(KafkaPlugin.TYPE);
        Properties properties = new Properties();
        factory.customizer(null).accept(properties);
        assertEquals("SASL_SSL", properties.get("security.protocol"));
    }

    public void testWithoutExtensionsTheFactoryHasNoConfigurators() {
        KafkaConsumerFactory factory = (KafkaConsumerFactory) new KafkaPlugin().getIngestionConsumerFactories().get(KafkaPlugin.TYPE);
        Properties properties = new Properties();
        factory.customizer(null).accept(properties);
        assertTrue(properties.isEmpty());
    }
}
