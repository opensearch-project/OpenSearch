/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.subdirectory;

import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class SubdirectoryStorePluginTests extends OpenSearchTestCase {

    public void testPluginInstantiation() {
        SubdirectoryStorePlugin plugin = new SubdirectoryStorePlugin();
        assertNotNull(plugin);
    }

    public void testGetStoreFactories() {
        SubdirectoryStorePlugin plugin = new SubdirectoryStorePlugin();
        Map<String, IndexStorePlugin.StoreFactory> factories = plugin.getStoreFactories();

        assertNotNull(factories);
        assertTrue(factories.containsKey("subdirectory_store"));

        IndexStorePlugin.StoreFactory factory = factories.get("subdirectory_store");
        assertNotNull(factory);
        assertTrue(factory instanceof SubdirectoryStorePlugin.SubdirectoryStoreFactory);
    }

}
