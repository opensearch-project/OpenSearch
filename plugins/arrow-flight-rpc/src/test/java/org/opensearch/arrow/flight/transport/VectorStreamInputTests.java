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
            assertSame("ByteSerialized holds the stream root — no transfer", shared, input.getRoot());
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

        VectorStreamInput.NativeArrow input = (VectorStreamInput.NativeArrow) VectorStreamInput.forNativeArrow(shared, registry);
        try {
            assertNotSame("NativeArrow transfers into a fresh consumer root", shared, input.getRoot());
            IntVector dstVec = (IntVector) input.getRoot().getVector("val");
            assertEquals(1, input.getRoot().getRowCount());
            assertEquals(42, dstVec.get(0));
            assertEquals("source must be drained", 0, shared.getRowCount());

            // Close the stream root immediately — consumer root must survive.
            shared.close();
            assertEquals("consumer root survives stream root close", 42, dstVec.get(0));

            // Simulate ArrowBatchResponse taking ownership, then close the consumer root on the response side.
            input.claimOwnership();
        } finally {
            input.close(); // no-op after claimOwnership
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

    public void testNativeArrowCloseReleasesRootIfNotTransferred() throws IOException {
        // read() throws or never runs: the consumer root must be released by close(), not leaked.
        VectorSchemaRoot shared = newNativeArrowRoot();
        IntVector srcVec = (IntVector) shared.getVector("val");
        srcVec.allocateNew();
        srcVec.setSafe(0, 7);
        srcVec.setValueCount(1);
        shared.setRowCount(1);

        long beforeClose;
        try (VectorStreamInput.NativeArrow input = (VectorStreamInput.NativeArrow) VectorStreamInput.forNativeArrow(shared, registry)) {
            beforeClose = allocator.getAllocatedMemory();
            assertTrue("consumer root should hold memory before close", beforeClose > 0);
        }
        // After try-with-resources: close() ran, transferred==false, root should be released.
        assertTrue(
            "consumer root must be released when not transferred (was " + beforeClose + ", now " + allocator.getAllocatedMemory() + ")",
            allocator.getAllocatedMemory() < beforeClose
        );
        shared.close();
    }

    public void testNativeArrowCloseIsNoOpAfterMarkTransferred() throws IOException {
        // ArrowBatchResponse(StreamInput) calls claimOwnership to take ownership.
        // After that, close() must leave the root alone so the response can use it.
        VectorSchemaRoot shared = newNativeArrowRoot();
        IntVector srcVec = (IntVector) shared.getVector("val");
        srcVec.allocateNew();
        srcVec.setSafe(0, 7);
        srcVec.setValueCount(1);
        shared.setRowCount(1);

        VectorStreamInput.NativeArrow input = (VectorStreamInput.NativeArrow) VectorStreamInput.forNativeArrow(shared, registry);
        VectorSchemaRoot consumerRoot = input.getRoot();
        input.claimOwnership();
        input.close();

        // Consumer root must remain usable — ArrowBatchResponse owns it after handoff.
        assertEquals(1, consumerRoot.getRowCount());
        assertEquals(7, ((IntVector) consumerRoot.getVector("val")).get(0));
        consumerRoot.close();
        shared.close();
    }

    public void testForNativeArrowRejectsEmptySchema() {
        Schema emptySchema = new Schema(List.<Field>of());
        VectorSchemaRoot shared = VectorSchemaRoot.create(emptySchema, allocator);
        try {
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> VectorStreamInput.forNativeArrow(shared, registry));
            assertTrue(e.getMessage().contains("no field vectors"));
        } finally {
            shared.close();
        }
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
            // Do not call input.getRoot().close() — the try-with-resources close() releases the
            // consumer root (transferred==false).
        }
        shared.close();
    }

    public void testReadByteEofWhenRowsExhausted() throws IOException {
        VectorSchemaRoot shared = newByteSerializedRoot();
        ((VarBinaryVector) shared.getVector("0")).allocateNew();
        shared.setRowCount(0);
        try (VectorStreamInput input = VectorStreamInput.forByteSerialized(shared, registry)) {
            expectThrows(java.io.EOFException.class, input::readByte);
        }
        shared.close();
    }

    public void testReadByteRejectsEmptyRow() throws IOException {
        VectorSchemaRoot shared = newByteSerializedRoot();
        VarBinaryVector vec = (VarBinaryVector) shared.getVector("0");
        vec.allocateNew();
        vec.setSafe(0, new byte[0]);
        vec.setValueCount(1);
        shared.setRowCount(1);
        try (VectorStreamInput input = VectorStreamInput.forByteSerialized(shared, registry)) {
            expectThrows(IOException.class, input::readByte);
        }
        shared.close();
    }

    public void testReadBytesInvalidOffsetThrows() throws IOException {
        VectorSchemaRoot shared = newByteSerializedRoot();
        try (VectorStreamInput input = VectorStreamInput.forByteSerialized(shared, registry)) {
            byte[] target = new byte[4];
            expectThrows(IllegalArgumentException.class, () -> input.readBytes(target, -1, 2));
            expectThrows(IllegalArgumentException.class, () -> input.readBytes(target, 0, -1));
            expectThrows(IllegalArgumentException.class, () -> input.readBytes(target, 3, 5));
        }
        shared.close();
    }

    public void testReadBytesSpansMultipleRowsWithLeftover() throws IOException {
        // Row 0: 3 bytes, row 1: 4 bytes. Read 5 bytes — spans both rows, leaves 2 in buffer
        // for a follow-up readByte.
        VectorSchemaRoot shared = newByteSerializedRoot();
        VarBinaryVector vec = (VarBinaryVector) shared.getVector("0");
        vec.allocateNew();
        vec.setSafe(0, new byte[] { 1, 2, 3 });
        vec.setSafe(1, new byte[] { 4, 5, 6, 7 });
        vec.setValueCount(2);
        shared.setRowCount(2);

        try (VectorStreamInput input = VectorStreamInput.forByteSerialized(shared, registry)) {
            byte[] out = new byte[5];
            input.readBytes(out, 0, 5);
            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, out);
            // Remaining buffered bytes from row 1 feed readByte.
            assertEquals((byte) 6, input.readByte());
            assertEquals((byte) 7, input.readByte());
        }
        shared.close();
    }

    public void testReadBytesEofWhenRowsExhausted() throws IOException {
        VectorSchemaRoot shared = newByteSerializedRoot();
        VarBinaryVector vec = (VarBinaryVector) shared.getVector("0");
        vec.allocateNew();
        vec.setSafe(0, new byte[] { 1, 2 });
        vec.setValueCount(1);
        shared.setRowCount(1);

        try (VectorStreamInput input = VectorStreamInput.forByteSerialized(shared, registry)) {
            byte[] out = new byte[4];
            expectThrows(java.io.EOFException.class, () -> input.readBytes(out, 0, 4));
        }
        shared.close();
    }

    public void testReadBytesRejectsEmptyRow() throws IOException {
        VectorSchemaRoot shared = newByteSerializedRoot();
        VarBinaryVector vec = (VarBinaryVector) shared.getVector("0");
        vec.allocateNew();
        vec.setSafe(0, new byte[0]);
        vec.setValueCount(1);
        shared.setRowCount(1);

        try (VectorStreamInput input = VectorStreamInput.forByteSerialized(shared, registry)) {
            byte[] out = new byte[2];
            expectThrows(IOException.class, () -> input.readBytes(out, 0, 2));
        }
        shared.close();
    }

    public void testReadBytesZeroLengthIsNoOp() throws IOException {
        VectorSchemaRoot shared = newByteSerializedRoot();
        try (VectorStreamInput input = VectorStreamInput.forByteSerialized(shared, registry)) {
            input.readBytes(new byte[4], 0, 0); // must not throw, must not advance
        }
        shared.close();
    }

    public void testReadBytesDrainsBufferThenAdvancesRow() throws IOException {
        // readByte advances row 0 into the internal buffer; readBytes must then drain the
        // buffer (1 byte left) before pulling row 1.
        VectorSchemaRoot shared = newByteSerializedRoot();
        VarBinaryVector vec = (VarBinaryVector) shared.getVector("0");
        vec.allocateNew();
        vec.setSafe(0, new byte[] { 10, 20 });
        vec.setSafe(1, new byte[] { 30, 40 });
        vec.setValueCount(2);
        shared.setRowCount(2);

        try (VectorStreamInput input = VectorStreamInput.forByteSerialized(shared, registry)) {
            assertEquals((byte) 10, input.readByte());
            byte[] out = new byte[3];
            input.readBytes(out, 0, 3);
            assertArrayEquals(new byte[] { 20, 30, 40 }, out);
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
