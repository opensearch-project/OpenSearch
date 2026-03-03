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

public class OSNativeEngineTests extends OpenSearchTestCase {

    private Schema testSchema() {
        return new Schema(List.of(new Field("col", FieldType.nullable(new ArrowType.Utf8()), null)));
    }

    private OSScan testChild() {
        return new OSScan("test_index", List.of("col"), testSchema());
    }

    public void testConstructionAndAccessors() {
        Schema schema = testSchema();
        OSScan child = testChild();
        byte[] plan = new byte[] { 1, 2, 3 };

        OSNativeEngine engine = new OSNativeEngine("datafusion", plan, child, schema);

        assertEquals("datafusion", engine.engineName());
        assertArrayEquals(new byte[] { 1, 2, 3 }, engine.serializedPlan());
        assertSame(child, engine.child());
        assertSame(schema, engine.outputSchema());
    }

    public void testChildrenReturnsSingleChild() {
        OSNativeEngine engine = new OSNativeEngine("datafusion", new byte[] { 1 }, testChild(), testSchema());
        assertEquals(1, engine.children().size());
        assertSame(engine.child(), engine.children().get(0));
    }

    public void testSerializedPlanDefensiveCopyOnConstruction() {
        byte[] plan = new byte[] { 1, 2, 3 };
        OSNativeEngine engine = new OSNativeEngine("datafusion", plan, testChild(), testSchema());

        // Mutate the original array
        plan[0] = 99;

        // Engine should still have the original values (defensive copy on construction)
        assertEquals(1, engine.serializedPlan()[0]);
    }

    public void testSerializedPlanDefensiveCopyOnRead() {
        byte[] plan = new byte[] { 1, 2, 3 };
        OSNativeEngine engine = new OSNativeEngine("datafusion", plan, testChild(), testSchema());

        // Get the plan and mutate it
        byte[] returned = engine.serializedPlan();
        returned[0] = 99;

        // Engine should still have the original values (defensive copy on read)
        assertEquals(1, engine.serializedPlan()[0]);
    }

    public void testVisitorDispatch() {
        OSNativeEngine engine = new OSNativeEngine("datafusion", new byte[] { 1 }, testChild(), testSchema());
        ExecNodeVisitor<String, Void> visitor = new ExecNodeVisitor<>() {
            @Override
            public String visitNativeEngine(OSNativeEngine node, Void context) {
                return "visited_native_engine";
            }
        };
        assertEquals("visited_native_engine", engine.accept(visitor, null));
    }

    public void testVisitorDefaultThrows() {
        OSNativeEngine engine = new OSNativeEngine("datafusion", new byte[] { 1 }, testChild(), testSchema());
        ExecNodeVisitor<String, Void> visitor = new ExecNodeVisitor<>() {
        };
        expectThrows(UnsupportedOperationException.class, () -> engine.accept(visitor, null));
    }

    public void testWriteToReadFromRoundTrip() throws IOException {
        Schema schema = testSchema();
        OSScan child = testChild();
        byte[] planBytes = new byte[] { 10, 20, 30, 40 };
        OSNativeEngine original = new OSNativeEngine("datafusion", planBytes, child, schema);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        OSNode deserialized = OSNode.readFrom(in);

        assertTrue(deserialized instanceof OSNativeEngine);
        OSNativeEngine result = (OSNativeEngine) deserialized;
        assertEquals("datafusion", result.engineName());
        assertArrayEquals(new byte[] { 10, 20, 30, 40 }, result.serializedPlan());
        assertTrue(result.child() instanceof OSScan);
        assertEquals("test_index", ((OSScan) result.child()).indexName());
    }

    public void testReadFromUnknownTagThrows() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeByte((byte) 99);

        StreamInput in = out.bytes().streamInput();
        expectThrows(IOException.class, () -> OSNode.readFrom(in));
    }
}
