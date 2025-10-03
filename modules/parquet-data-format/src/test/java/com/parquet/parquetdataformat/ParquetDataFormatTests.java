/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package com.parquet.parquetdataformat;

import com.parquet.parquetdataformat.bridge.RustBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class ParquetDataFormatTests extends OpenSearchTestCase {

    public void testIngestion() throws IOException {
        // Test only basic functionality without Arrow operations
        try {
            // Create plugin but don't call complex operations
            ParquetDataFormatPlugin plugin = new ParquetDataFormatPlugin();
            plugin.indexDataToParquetEngine();
            
        } catch (UnsatisfiedLinkError e) {
            fail("Native library not loaded properly: " + e.getMessage());
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        }
    }
}
