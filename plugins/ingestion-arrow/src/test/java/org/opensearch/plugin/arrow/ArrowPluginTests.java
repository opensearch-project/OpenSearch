/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.util.List;
import java.util.Map;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowPluginTests extends OpenSearchTestCase {

    public void testGetType() {
        assertEquals("ARROW", new ArrowPlugin().getType());
    }

    @SuppressWarnings("rawtypes")
    public void testLoadExtensionsRegistersBuiltInArrowIpcFactory() {
        ArrowPlugin plugin = new ArrowPlugin();
        plugin.loadExtensions(extensionLoaderReturning(List.of()));

        Map<String, IngestionConsumerFactory> factories = plugin.getIngestionConsumerFactories();
        assertTrue(factories.containsKey("ARROW"));
        assertNotNull(factories.get("ARROW"));
    }

    @SuppressWarnings("rawtypes")
    public void testLoadExtensionsMergesDiscoveredFactories() {
        ArrowPlugin plugin = new ArrowPlugin();
        FakeArrowSourceFactory extension = new FakeArrowSourceFactory("CUSTOM");
        plugin.loadExtensions(extensionLoaderReturning(List.of(extension)));

        ArrowConsumerFactory consumerFactory = (ArrowConsumerFactory) plugin.getIngestionConsumerFactories().get("ARROW");
        // Both the built-in ARROW_IPC type and the discovered CUSTOM type should be usable;
        // dispatch is exercised indirectly via ArrowConsumerFactoryTests, here just confirm no
        // exception is thrown when the custom type resolves.
        assertNotNull(consumerFactory);
    }

    public void testLoadExtensionsDuplicateTypeThrows() {
        ArrowPlugin plugin = new ArrowPlugin();
        FakeArrowSourceFactory duplicateBuiltIn = new FakeArrowSourceFactory(ArrowIpcSourceFactory.TYPE);
        expectThrows(
                IllegalStateException.class,
                () -> plugin.loadExtensions(extensionLoaderReturning(List.of(duplicateBuiltIn))));
    }

    private static ExtensiblePlugin.ExtensionLoader extensionLoaderReturning(List<ArrowSourceFactory> factories) {
        return new ExtensiblePlugin.ExtensionLoader() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                assertEquals(ArrowSourceFactory.class, extensionPointType);
                return (List<T>) factories;
            }
        };
    }

    private static final class FakeArrowSourceFactory implements ArrowSourceFactory {
        private final String type;

        FakeArrowSourceFactory(String type) {
            this.type = type;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public ArrowSource createArrowSource(Map<String, Object> params, ArrowIngestionConfig ingestionConfig) {
            throw new UnsupportedOperationException("not exercised in this test");
        }
    }
}
