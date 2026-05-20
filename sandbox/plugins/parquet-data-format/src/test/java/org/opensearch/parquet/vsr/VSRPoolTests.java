/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.settings.Settings;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class VSRPoolTests extends OpenSearchTestCase {

    private ArrowBufferPool bufferPool;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        bufferPool = new ArrowBufferPool(Settings.EMPTY);
        schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    }

    @Override
    public void tearDown() throws Exception {
        bufferPool.close();
        super.tearDown();
    }

    public void testInitialStateHasActiveVSR() {
        VSRPool pool = new VSRPool("pool-1", schema, bufferPool, 50000);
        assertNotNull(pool.getActiveVSR());
        assertEquals(VSRState.ACTIVE, pool.getActiveVSR().getState());
        assertNull(pool.getFrozenVSR());
        pool.close();
    }

    public void testNoRotationBelowThreshold() throws IOException {
        VSRPool pool = new VSRPool("pool-2", schema, bufferPool, 50000);
        assertFalse(pool.maybeRotateActiveVSR());
        pool.close();
    }

    public void testRotationAtThreshold() throws IOException {
        VSRPool pool = new VSRPool("pool-3", schema, bufferPool, 50000);
        ManagedVSR original = pool.getActiveVSR();
        original.setRowCount(50000);
        assertTrue(pool.maybeRotateActiveVSR());

        assertNotNull(pool.getFrozenVSR());
        assertEquals(VSRState.FROZEN, pool.getFrozenVSR().getState());
        assertNotNull(pool.getActiveVSR());
        assertNotSame(original, pool.getActiveVSR());
        assertEquals(VSRState.ACTIVE, pool.getActiveVSR().getState());

        // cleanup frozen, then freeze active, then close
        pool.completeVSR(pool.getFrozenVSR());
        pool.unsetFrozenVSR();
        pool.close();
    }

    public void testRotationFailsWhenFrozenSlotOccupied() throws IOException {
        VSRPool pool = new VSRPool("pool-4", schema, bufferPool, 50000);
        pool.getActiveVSR().setRowCount(50000);
        pool.maybeRotateActiveVSR();

        pool.getActiveVSR().setRowCount(50000);
        assertFalse(pool.maybeRotateActiveVSR());

        pool.completeVSR(pool.getFrozenVSR());
        pool.unsetFrozenVSR();
        pool.close();
    }

    public void testCompleteAndUnsetFrozenVSR() throws IOException {
        VSRPool pool = new VSRPool("pool-5", schema, bufferPool, 50000);
        pool.getActiveVSR().setRowCount(50000);
        pool.maybeRotateActiveVSR();

        ManagedVSR frozen = pool.getFrozenVSR();
        assertNotNull(frozen);
        pool.completeVSR(frozen);
        assertEquals(VSRState.CLOSED, frozen.getState());
        pool.unsetFrozenVSR();
        assertNull(pool.getFrozenVSR());
        pool.close();
    }

    public void testUnsetFrozenVSRThrowsWhenNoneSet() {
        VSRPool pool = new VSRPool("pool-6", schema, bufferPool, 50000);
        expectThrows(IllegalStateException.class, pool::unsetFrozenVSR);
        pool.close();
    }

    public void testUnsetFrozenVSRThrowsWhenNotClosed() throws IOException {
        VSRPool pool = new VSRPool("pool-7", schema, bufferPool, 50000);
        pool.getActiveVSR().setRowCount(50000);
        pool.maybeRotateActiveVSR();

        // frozen VSR is in FROZEN state, not CLOSED
        expectThrows(IllegalStateException.class, pool::unsetFrozenVSR);

        pool.completeVSR(pool.getFrozenVSR());
        pool.unsetFrozenVSR();
        pool.close();
    }
}
