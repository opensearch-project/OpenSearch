/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Placeholder test for the tiered-storage module skeleton.
 * Real tests will be added with implementation PRs.
 */
public class TieredStoragePluginTests extends OpenSearchTestCase {

    public void testPluginClassExists() {
        // Verify the plugin class can be loaded
        assertNotNull(TieredStoragePlugin.class);
    }
}
