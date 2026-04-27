/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import org.opensearch.test.OpenSearchTestCase;

public class IcebergCatalogPluginTests extends OpenSearchTestCase {

    public void testPluginCreation() throws Exception {
        try (IcebergCatalogPlugin plugin = new IcebergCatalogPlugin()) {
            assertNotNull(plugin);
            assertFalse(plugin.getSettings().isEmpty());
        }
    }
}
