/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class ManagedVSRTests extends OpenSearchTestCase {

    private RootAllocator rootAllocator;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        rootAllocator = new RootAllocator();
        schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    }

    @Override
    public void tearDown() throws Exception {
        rootAllocator.close();
        super.tearDown();
    }

    public void testInitialState() {
        ManagedVSR vsr = createVSR("test-1");
        assertEquals(VSRState.ACTIVE, vsr.getState());
        assertEquals("test-1", vsr.getId());
        assertEquals(0, vsr.getRowCount());
        cleanup(vsr);
    }

    public void testSetRowCountInActive() {
        ManagedVSR vsr = createVSR("test-2");
        vsr.setRowCount(5);
        assertEquals(5, vsr.getRowCount());
        cleanup(vsr);
    }

    public void testGetVectorInActive() {
        ManagedVSR vsr = createVSR("test-3");
        FieldVector vec = vsr.getVector("val");
        assertNotNull(vec);
        assertTrue(vec instanceof IntVector);
        cleanup(vsr);
    }

    public void testGetVectorReturnsNullForUnknownField() {
        ManagedVSR vsr = createVSR("test-4");
        assertNull(vsr.getVector("nonexistent"));
        cleanup(vsr);
    }

    public void testMoveToFrozen() {
        ManagedVSR vsr = createVSR("test-5");
        vsr.moveToFrozen();
        assertEquals(VSRState.FROZEN, vsr.getState());
        vsr.close();
    }

    public void testSetRowCountThrowsInFrozen() {
        ManagedVSR vsr = createVSR("test-6");
        vsr.moveToFrozen();
        expectThrows(IllegalStateException.class, () -> vsr.setRowCount(10));
        vsr.close();
    }

    public void testGetVectorThrowsInFrozen() {
        ManagedVSR vsr = createVSR("test-7");
        vsr.moveToFrozen();
        expectThrows(IllegalStateException.class, () -> vsr.getVector("val"));
        vsr.close();
    }

    public void testMoveToFrozenThrowsIfAlreadyFrozen() {
        ManagedVSR vsr = createVSR("test-8");
        vsr.moveToFrozen();
        expectThrows(IllegalStateException.class, vsr::moveToFrozen);
        vsr.close();
    }

    public void testCloseFromFrozen() {
        ManagedVSR vsr = createVSR("test-9");
        vsr.moveToFrozen();
        vsr.close();
        assertEquals(VSRState.CLOSED, vsr.getState());
    }

    public void testCloseIsIdempotentWhenClosed() {
        ManagedVSR vsr = createVSR("test-10");
        vsr.moveToFrozen();
        vsr.close();
        vsr.close(); // should not throw
        assertEquals(VSRState.CLOSED, vsr.getState());
    }

    public void testCloseThrowsIfActive() {
        ManagedVSR vsr = createVSR("test-11");
        IllegalStateException e = expectThrows(IllegalStateException.class, vsr::close);
        assertTrue(e.getMessage().contains("must freeze first"));
        // cleanup: freeze then close
        vsr.moveToFrozen();
        vsr.close();
    }

    public void testToString() {
        ManagedVSR vsr = createVSR("test-12");
        String str = vsr.toString();
        assertTrue(str.contains("id='test-12'"));
        assertTrue(str.contains("state=ACTIVE"));
        assertTrue(str.contains("rows=0"));
        cleanup(vsr);
    }

    public void testWriteAndReadVector() {
        ManagedVSR vsr = createVSR("test-13");
        IntVector vec = (IntVector) vsr.getVector("val");
        vec.setSafe(0, 99);
        vsr.setRowCount(1);
        assertEquals(1, vsr.getRowCount());
        assertEquals(99, ((IntVector) vsr.getVector("val")).get(0));
        cleanup(vsr);
    }

    private ManagedVSR createVSR(String id) {
        BufferAllocator child = rootAllocator.newChildAllocator(id, 0, Long.MAX_VALUE);
        return new ManagedVSR(id, schema, child);
    }

    private void cleanup(ManagedVSR vsr) {
        vsr.moveToFrozen();
        vsr.close();
    }
}
