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
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class VectorStreamInputTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private NamedWriteableRegistry registry;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
        registry = new NamedWriteableRegistry(Collections.emptyList());
    }

    @After
    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testByteSerializedReadsFromSharedRoot() throws IOException {
        VectorSchemaRoot shared = newByteSerializedRoot();
        try (VectorStreamInput input = VectorStreamInput.forByteSerialized(shared, registry)) {
            assertTrue(input instanceof VectorStreamInput.ByteSerialized);
            assertSame("ByteSerialized holds the shared root — no transfer", shared, input.getRoot());
        }
        shared.close();
    }

    public void testNativeArrowTransfersIntoOwnedRoot() throws IOException {
        VectorSchemaRoot shared = newNativeArrowRoot();
        IntVector srcVec = (IntVector) shared.getVector("val");
        srcVec.allocateNew();
        srcVec.setSafe(0, 42);
        srcVec.setValueCount(1);
        shared.setRowCount(1);

        try (VectorStreamInput input = VectorStreamInput.forNativeArrow(shared, registry)) {
            assertTrue(input instanceof VectorStreamInput.NativeArrow);
            assertNotSame("NativeArrow transfers into a fresh owned root", shared, input.getRoot());
            IntVector dstVec = (IntVector) input.getRoot().getVector("val");
            assertEquals(1, input.getRoot().getRowCount());
            assertEquals(42, dstVec.get(0));
            assertEquals("source must be drained", 0, shared.getRowCount());

            // Close the shared root immediately — owned root must survive.
            shared.close();
            assertEquals("owned root survives shared-root close", 42, dstVec.get(0));
            input.getRoot().close();
        }
    }

    public void testByteSerializedCloseIsNoOpOnSharedRoot() throws IOException {
        VectorSchemaRoot shared = newByteSerializedRoot();
        VarBinaryVector vec = (VarBinaryVector) shared.getVector("0");
        vec.allocateNew();
        vec.setSafe(0, new byte[] { 1, 2, 3 });
        vec.setValueCount(1);
        shared.setRowCount(1);

        VectorStreamInput input = VectorStreamInput.forByteSerialized(shared, registry);
        input.close();

        // Shared root must remain fully usable — FlightStream owns its lifecycle.
        assertEquals(1, shared.getRowCount());
        assertEquals(3, ((VarBinaryVector) shared.getVector("0")).get(0).length);
        shared.close();
    }

    public void testNativeArrowCloseIsNoOpOnOwnedRoot() throws IOException {
        VectorSchemaRoot shared = newNativeArrowRoot();
        IntVector srcVec = (IntVector) shared.getVector("val");
        srcVec.allocateNew();
        srcVec.setSafe(0, 7);
        srcVec.setValueCount(1);
        shared.setRowCount(1);

        VectorStreamInput input = VectorStreamInput.forNativeArrow(shared, registry);
        VectorSchemaRoot owned = input.getRoot();
        input.close();

        // Owned root must remain usable — ArrowBatchResponse owns it after handoff.
        assertEquals(1, owned.getRowCount());
        assertEquals(7, ((IntVector) owned.getVector("val")).get(0));
        owned.close();
        shared.close();
    }

    public void testByteSerializedReadsBytesFromSharedVector() throws IOException {
        VectorSchemaRoot shared = newByteSerializedRoot();
        VarBinaryVector vec = (VarBinaryVector) shared.getVector("0");
        vec.allocateNew();
        vec.setSafe(0, new byte[] { 10, 20, 30 });
        vec.setValueCount(1);
        shared.setRowCount(1);

        try (VectorStreamInput input = VectorStreamInput.forByteSerialized(shared, registry)) {
            assertEquals((byte) 10, input.readByte());
            assertEquals((byte) 20, input.readByte());
            assertEquals((byte) 30, input.readByte());
        }
        shared.close();
    }

    public void testNativeArrowRejectsByteReads() throws IOException {
        VectorSchemaRoot shared = newNativeArrowRoot();
        try (VectorStreamInput input = VectorStreamInput.forNativeArrow(shared, registry)) {
            expectThrows(UnsupportedOperationException.class, input::readByte);
            expectThrows(UnsupportedOperationException.class, () -> input.readBytes(new byte[1], 0, 1));
            input.getRoot().close();
        }
        shared.close();
    }

    private VectorSchemaRoot newByteSerializedRoot() {
        Schema schema = new Schema(List.of(new Field("0", FieldType.nullable(new ArrowType.Binary()), null)));
        return VectorSchemaRoot.create(schema, allocator);
    }

    private VectorSchemaRoot newNativeArrowRoot() {
        Schema schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        return VectorSchemaRoot.create(schema, allocator);
    }
}
