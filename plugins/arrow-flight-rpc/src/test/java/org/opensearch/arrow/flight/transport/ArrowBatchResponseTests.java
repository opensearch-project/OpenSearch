/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class ArrowBatchResponseTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private Schema schema;

    static class TestResponse extends ArrowBatchResponse {
        TestResponse(VectorSchemaRoot root) {
            super(root);
        }

        TestResponse(StreamInput in) throws IOException {
            super(in);
        }
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
        schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    }

    @After
    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testGetRootReturnsProducerRoot() {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        TestResponse response = new TestResponse(root);
        assertSame(root, response.getRoot());
        root.close();
    }

    public void testWriteToIsNoOp() throws IOException {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        TestResponse response = new TestResponse(root);
        StreamOutput mockOut = mock(StreamOutput.class);
        response.writeTo(mockOut);
        verifyNoInteractions(mockOut);
        root.close();
    }

    public void testTransferToMovesBuffers() {
        VectorSchemaRoot src = VectorSchemaRoot.create(schema, allocator);
        IntVector srcVec = (IntVector) src.getVector("val");
        srcVec.allocateNew();
        srcVec.setSafe(0, 42);
        srcVec.setSafe(1, 99);
        srcVec.setValueCount(2);
        src.setRowCount(2);

        VectorSchemaRoot dst = VectorSchemaRoot.create(schema, allocator);
        TestResponse response = new TestResponse(src);
        response.transferTo(dst);

        assertEquals(2, dst.getRowCount());
        IntVector dstVec = (IntVector) dst.getVector("val");
        assertEquals(42, dstVec.get(0));
        assertEquals(99, dstVec.get(1));

        // Source should be empty after transfer — both at vector and root level
        assertEquals(0, srcVec.getValueCount());
        assertEquals(0, src.getRowCount());

        src.close();
        dst.close();
    }

    /**
     * After transfer, closing the source must not affect the destination — the destination owns
     * its buffers. This is the invariant FlightTransportResponse relies on to decouple the
     * returned response from FlightStream's shared, reused root.
     */
    public void testDestinationSurvivesSourceClose() {
        VectorSchemaRoot src = VectorSchemaRoot.create(schema, allocator);
        IntVector srcVec = (IntVector) src.getVector("val");
        srcVec.allocateNew();
        srcVec.setSafe(0, 7);
        srcVec.setSafe(1, 13);
        srcVec.setValueCount(2);
        src.setRowCount(2);

        VectorSchemaRoot dst = VectorSchemaRoot.create(schema, allocator);
        new TestResponse(src).transferTo(dst);

        // Close the source — simulates FlightStream clearing/closing its shared root.
        src.close();

        assertEquals(2, dst.getRowCount());
        IntVector dstVec = (IntVector) dst.getVector("val");
        assertEquals(2, dstVec.getValueCount());
        assertEquals(7, dstVec.get(0));
        assertEquals(13, dstVec.get(1));

        dst.close();
    }

    public void testTransferToWithMultipleVectors() {
        Schema multiSchema = new Schema(
            List.of(
                new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("b", FieldType.nullable(new ArrowType.Int(32, true)), null)
            )
        );

        VectorSchemaRoot src = VectorSchemaRoot.create(multiSchema, allocator);
        ((IntVector) src.getVector("a")).allocateNew();
        ((IntVector) src.getVector("a")).setSafe(0, 1);
        ((IntVector) src.getVector("a")).setValueCount(1);
        ((IntVector) src.getVector("b")).allocateNew();
        ((IntVector) src.getVector("b")).setSafe(0, 2);
        ((IntVector) src.getVector("b")).setValueCount(1);
        src.setRowCount(1);

        VectorSchemaRoot dst = VectorSchemaRoot.create(multiSchema, allocator);
        new TestResponse(src).transferTo(dst);

        assertEquals(1, dst.getRowCount());
        assertEquals(1, ((IntVector) dst.getVector("a")).get(0));
        assertEquals(2, ((IntVector) dst.getVector("b")).get(0));

        src.close();
        dst.close();
    }
}
