/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.engine;

import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.test.OpenSearchTestCase;

public class ParquetDataFormatTests extends OpenSearchTestCase {

    public void testName() {
        assertEquals("parquet", new ParquetDataFormat().name());
    }

    public void testPriority() {
        assertEquals(0L, new ParquetDataFormat().priority());
    }

    public void testSupportedFieldsContainsColumnarStorage() {
        var fields = new ParquetDataFormat().supportedFields();
        assertFalse(fields.isEmpty());
        for (var ftc : fields) {
            assertTrue(ftc.capabilities().contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
        }
    }
}
