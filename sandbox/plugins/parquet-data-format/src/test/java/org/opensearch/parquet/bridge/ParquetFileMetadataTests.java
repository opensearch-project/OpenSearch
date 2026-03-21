/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.test.OpenSearchTestCase;

public class ParquetFileMetadataTests extends OpenSearchTestCase {

    public void testAccessors() {
        ParquetFileMetadata metadata = new ParquetFileMetadata(2, 100L, "test-writer");
        assertEquals(2, metadata.version());
        assertEquals(100L, metadata.numRows());
        assertEquals("test-writer", metadata.createdBy());
    }

    public void testToString() {
        ParquetFileMetadata metadata = new ParquetFileMetadata(1, 50L, "writer-v1");
        String result = metadata.toString();
        assertTrue(result.contains("version=1"));
        assertTrue(result.contains("numRows=50"));
        assertTrue(result.contains("createdBy='writer-v1'"));
    }

    public void testRecordEquality() {
        ParquetFileMetadata a = new ParquetFileMetadata(1, 10L, "w");
        ParquetFileMetadata b = new ParquetFileMetadata(1, 10L, "w");
        ParquetFileMetadata c = new ParquetFileMetadata(2, 10L, "w");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }
}
