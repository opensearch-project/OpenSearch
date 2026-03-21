/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.engine;

import org.opensearch.test.OpenSearchTestCase;

public class ParquetDataFormatTests extends OpenSearchTestCase {

    public void testName() {
        assertEquals("parquet", new ParquetDataFormat().name());
    }

    public void testPriority() {
        assertEquals(0L, new ParquetDataFormat().priority());
    }

    public void testSupportedFieldsEmpty() {
        assertTrue(new ParquetDataFormat().supportedFields().isEmpty());
    }
}
