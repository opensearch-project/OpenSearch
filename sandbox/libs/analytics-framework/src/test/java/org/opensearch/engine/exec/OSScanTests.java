/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.exec;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class OSScanTests extends OpenSearchTestCase {

    public void testOSScanIsImmutable() {
        Schema schema = new Schema(List.of(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        OSScan scan = new OSScan("test_index", List.of("name"), schema);

        assertEquals("test_index", scan.indexName());
        assertEquals(List.of("name"), scan.columnNames());
        assertEquals(schema, scan.outputSchema());
        assertTrue(scan.children().isEmpty());
    }

    public void testColumnNamesDefensivelyCopied() {
        Schema schema = new Schema(List.of(new Field("a", FieldType.nullable(new ArrowType.Utf8()), null)));
        List<String> cols = new java.util.ArrayList<>(List.of("a"));
        OSScan scan = new OSScan("idx", cols, schema);
        cols.add("b");
        assertEquals(1, scan.columnNames().size());
    }

    public void testWriteToReadFromRoundTrip() throws IOException {
        Schema schema = new Schema(
            List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("age", FieldType.nullable(new ArrowType.Int(64, true)), null)
            )
        );
        OSScan original = new OSScan("test_index", List.of("name", "age"), schema);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        OSNode deserialized = OSNode.readFrom(in);

        assertTrue(deserialized instanceof OSScan);
        OSScan result = (OSScan) deserialized;
        assertEquals("test_index", result.indexName());
        assertEquals(List.of("name", "age"), result.columnNames());
        assertEquals(2, result.outputSchema().getFields().size());
        assertEquals("name", result.outputSchema().getFields().get(0).getName());
        assertEquals("age", result.outputSchema().getFields().get(1).getName());
    }
}
