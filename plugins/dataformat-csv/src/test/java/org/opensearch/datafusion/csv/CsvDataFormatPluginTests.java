/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for the CSV data format plugin.
 */
public class CsvDataFormatPluginTests extends OpenSearchTestCase {

    /**
     * Test that the plugin can be instantiated.
     */
    public void testPluginInstantiation() {
        CsvDataFormatPlugin plugin = new CsvDataFormatPlugin();
        assertNotNull("Plugin should not be null", plugin);
    }
}
