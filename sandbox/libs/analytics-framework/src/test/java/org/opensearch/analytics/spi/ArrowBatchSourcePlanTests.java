/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.ColumnKind;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class ArrowBatchSourcePlanTests extends OpenSearchTestCase {

    public void testSchemaAndWireRoundTrip() throws Exception {
        List<InputColumn> columns = List.of(
            new InputColumn("long", ColumnKind.LONG),
            new InputColumn("keyword", ColumnKind.KEYWORD),
            new InputColumn("timestamp", ColumnKind.TIMESTAMP),
            new InputColumn("boolean", ColumnKind.BOOLEAN),
            new InputColumn("float", ColumnKind.FLOAT),
            new InputColumn("double", ColumnKind.DOUBLE),
            new InputColumn("binary", ColumnKind.BINARY),
            new InputColumn("ip", ColumnKind.IP),
            new InputColumn("many_ip", ColumnKind.IP, true)
        );
        ArrowBatchSourcePlan original = new ArrowBatchSourcePlan("input-0", new byte[] { 1, 2 }, columns);
        Schema schema = original.inputSchema();

        assertTrue(schema.findField("long").getType() instanceof ArrowType.Int);
        assertTrue(schema.findField("keyword").getType() instanceof ArrowType.Utf8View);
        assertTrue(schema.findField("timestamp").getType() instanceof ArrowType.Timestamp);
        assertTrue(schema.findField("boolean").getType() instanceof ArrowType.Bool);
        assertEquals(FloatingPointPrecision.SINGLE, ((ArrowType.FloatingPoint) schema.findField("float").getType()).getPrecision());
        assertEquals(FloatingPointPrecision.DOUBLE, ((ArrowType.FloatingPoint) schema.findField("double").getType()).getPrecision());
        assertTrue(schema.findField("binary").getType() instanceof ArrowType.Binary);
        assertTrue(schema.findField("ip").getType() instanceof ArrowType.Binary);
        assertTrue(schema.findField("many_ip").getType() instanceof ArrowType.List);
        assertTrue(schema.findField("many_ip").getChildren().get(0).getType() instanceof ArrowType.Binary);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ArrowBatchSourcePlan decoded = new ArrowBatchSourcePlan(in);
                assertEquals(original.inputId(), decoded.inputId());
                assertArrayEquals(original.planBytes(), decoded.planBytes());
                assertEquals(columns, decoded.inputColumns());
            }
        }
    }
}
