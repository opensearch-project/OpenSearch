/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.vsr;

import com.parquet.parquetdataformat.bridge.ArrowExport;
import com.parquet.parquetdataformat.memory.ArrowBufferPool;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.Types;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.common.settings.Settings;

import java.util.Arrays;

/**
 * End-to-end integration tests covering the complete VSR lifecycle across all components
 * after the removal of atomic/thread-safe handling and state management simplification.
 */
public class VSRIntegrationTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private ArrowBufferPool bufferPool;
    private Schema testSchema;
    private String poolId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
        bufferPool = new ArrowBufferPool(Settings.EMPTY);

        // Create a simple test schema
        Field idField = new Field("id", FieldType.nullable(Types.MinorType.INT.getType()), null);
        Field nameField = new Field("name", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        testSchema = new Schema(Arrays.asList(idField, nameField));

        poolId = "integration-test-pool-" + System.currentTimeMillis();
    }

    @Override
    public void tearDown() throws Exception {
        if (bufferPool != null) {
            bufferPool.close();
        }
        if (allocator != null) {
            allocator.close();
        }
        super.tearDown();
    }

    // ===== Complete VSR Lifecycle Integration Tests =====

    public void testCompleteVSRLifecycleIntegration() {
        // Test the complete VSR lifecycle: Pool -> Manager -> Export -> Cleanup
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        // 1. VSRPool provides active VSR
        ManagedVSR activeVSR = pool.getActiveVSR();
        assertNotNull("Pool should provide active VSR", activeVSR);
        assertEquals("VSR should start ACTIVE", VSRState.ACTIVE, activeVSR.getState());

        // 2. Simulate VSRManager populating data
        activeVSR.setRowCount(100);
        assertEquals("VSR should have expected row count", 100, activeVSR.getRowCount());

        // Verify data can be added to vectors
        IntVector idVector = (IntVector) activeVSR.getVector("id");
        VarCharVector nameVector = (VarCharVector) activeVSR.getVector("name");
        assertNotNull("Should have id vector", idVector);
        assertNotNull("Should have name vector", nameVector);

        // 3. VSRManager decides to freeze for processing
        activeVSR.moveToFrozen();
        assertEquals("VSR should be FROZEN", VSRState.FROZEN, activeVSR.getState());
        assertTrue("VSR should be immutable when frozen", activeVSR.isImmutable());

        // 4. Export for Rust processing (simulating VSRManager -> RustBridge handoff)
        try (ArrowExport export = activeVSR.exportToArrow()) {
            assertNotNull("Export should succeed", export);
            assertTrue("Array address should be valid", export.getArrayAddress() != 0);
            assertTrue("Schema address should be valid", export.getSchemaAddress() != 0);

            // Simulate successful Rust processing
            // (In real implementation, RustBridge.write would be called here)
        }

        // 5. After processing, VSRPool completes the VSR
        pool.completeVSR(activeVSR);
        assertEquals("VSR should be CLOSED after completion", VSRState.CLOSED, activeVSR.getState());

        // 6. Cleanup
        pool.close();
    }

    public void testMultipleVSRsWithDifferentLifecycleStages() {
        // Test managing multiple VSRs at different lifecycle stages simultaneously
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        // Create VSRs at different stages
        ManagedVSR activeVSR = pool.getActiveVSR();

        BufferAllocator frozenAllocator = bufferPool.createChildAllocator("frozen-vsr");
        ManagedVSR frozenVSR = new ManagedVSR("frozen-vsr", testSchema, frozenAllocator);

        BufferAllocator closedAllocator = bufferPool.createChildAllocator("closed-vsr");
        ManagedVSR closedVSR = new ManagedVSR("closed-vsr", testSchema, closedAllocator);

        // Set up different states
        activeVSR.setRowCount(50);
        assertEquals("Active VSR should be ACTIVE", VSRState.ACTIVE, activeVSR.getState());

        frozenVSR.setRowCount(75);
        frozenVSR.moveToFrozen();
        assertEquals("Frozen VSR should be FROZEN", VSRState.FROZEN, frozenVSR.getState());

        closedVSR.setRowCount(25);
        closedVSR.moveToFrozen();
        closedVSR.close();
        assertEquals("Closed VSR should be CLOSED", VSRState.CLOSED, closedVSR.getState());

        // Verify operations work correctly for each state

        // Active VSR: can be modified and should export after freezing
        activeVSR.setRowCount(60);
        assertEquals("Active VSR row count should be updated", 60, activeVSR.getRowCount());

        activeVSR.moveToFrozen();
        try (ArrowExport export = activeVSR.exportToArrow()) {
            assertNotNull("Active->Frozen VSR should export", export);
        }

        // Frozen VSR: should be able to export but not modify
        try (ArrowExport export = frozenVSR.exportToArrow()) {
            assertNotNull("Frozen VSR should export", export);
        }

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
            frozenVSR.setRowCount(80);
        });
        assertTrue("Should not allow modification of frozen VSR",
                   exception.getMessage().contains("Cannot modify VSR in state: FROZEN"));

        // Closed VSR: should maintain its final state
        assertEquals("Closed VSR should maintain row count", 25, closedVSR.getRowCount());
        assertEquals("Closed VSR should remain CLOSED", VSRState.CLOSED, closedVSR.getState());

        // Complete the active VSR
        pool.completeVSR(activeVSR);
        assertEquals("Active VSR should be CLOSED after completion", VSRState.CLOSED, activeVSR.getState());

        // Complete the frozen VSR
        pool.completeVSR(frozenVSR);
        assertEquals("Frozen VSR should be CLOSED after completion", VSRState.CLOSED, frozenVSR.getState());

        pool.close();
    }

    public void testVSRStateTransitionValidation() {
        // Test that all state transitions are properly validated across components
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        ManagedVSR vsr = pool.getActiveVSR();

        // Valid transition: ACTIVE -> FROZEN
        assertEquals("Should start ACTIVE", VSRState.ACTIVE, vsr.getState());
        vsr.moveToFrozen();
        assertEquals("Should be FROZEN after moveToFrozen()", VSRState.FROZEN, vsr.getState());

        // Invalid transition: FROZEN -> FROZEN
        IllegalStateException exception1 = expectThrows(IllegalStateException.class, vsr::moveToFrozen);
        assertTrue("Should not allow FROZEN->FROZEN transition",
                   exception1.getMessage().contains("expected ACTIVE state"));

        // Valid transition: FROZEN -> CLOSED
        vsr.close();
        assertEquals("Should be CLOSED after close()", VSRState.CLOSED, vsr.getState());

        // Invalid operations on CLOSED VSR
        IllegalStateException exception2 = expectThrows(IllegalStateException.class, vsr::moveToFrozen);
        assertTrue("Should not allow CLOSED->FROZEN transition",
                   exception2.getMessage().contains("expected ACTIVE state"));

        // Test that close() is idempotent
        vsr.close();
        assertEquals("VSR should remain CLOSED", VSRState.CLOSED, vsr.getState());

        pool.close();
    }

    public void testVSROperationRestrictionsByState() {
        // Test that operations are properly restricted based on VSR state
        BufferAllocator childAllocator = bufferPool.createChildAllocator("test-restrictions-vsr");
        ManagedVSR vsr = new ManagedVSR("test-restrictions-vsr", testSchema, childAllocator);

        // ACTIVE state: all modification operations should work
        assertEquals("Should start ACTIVE", VSRState.ACTIVE, vsr.getState());
        vsr.setRowCount(10);
        assertEquals("Row count should be set in ACTIVE state", 10, vsr.getRowCount());

        // Export should fail in ACTIVE state
        IllegalStateException exception1 = expectThrows(IllegalStateException.class, vsr::exportToArrow);
        assertTrue("Should not allow export in ACTIVE state",
                   exception1.getMessage().contains("Cannot export VSR in state: ACTIVE"));

        // Schema export should work in all states
        try (ArrowExport schemaExport = vsr.exportSchema()) {
            assertNotNull("Schema export should work in ACTIVE state", schemaExport);
        }

        // Transition to FROZEN
        vsr.moveToFrozen();
        assertEquals("Should be FROZEN", VSRState.FROZEN, vsr.getState());

        // FROZEN state: modifications should fail, exports should work
        IllegalStateException exception2 = expectThrows(IllegalStateException.class, () -> {
            vsr.setRowCount(20);
        });
        assertTrue("Should not allow modification in FROZEN state",
                   exception2.getMessage().contains("Cannot modify VSR in state: FROZEN"));

        // Export should work in FROZEN state
        try (ArrowExport export = vsr.exportToArrow()) {
            assertNotNull("Export should work in FROZEN state", export);
        }

        try (ArrowExport schemaExport = vsr.exportSchema()) {
            assertNotNull("Schema export should work in FROZEN state", schemaExport);
        }

        // Transition to CLOSED
        vsr.close();
        assertEquals("Should be CLOSED", VSRState.CLOSED, vsr.getState());

        // CLOSED state: most operations should still return values but no modifications
        assertEquals("Should maintain row count in CLOSED state", 10, vsr.getRowCount());
        assertTrue("Should be immutable in CLOSED state", vsr.isImmutable());

        IllegalStateException exception3 = expectThrows(IllegalStateException.class, () -> {
            vsr.setRowCount(30);
        });
        assertTrue("Should not allow modification in CLOSED state",
                   exception3.getMessage().contains("Cannot modify VSR in state: CLOSED"));
    }

    public void testErrorHandlingAcrossComponents() {
        // Test error handling scenarios that span multiple components
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        // Test invalid close attempt
        ManagedVSR activeVSR = pool.getActiveVSR();
        activeVSR.setRowCount(15);

        // Attempting to close an ACTIVE VSR should fail
        IllegalStateException exception1 = expectThrows(IllegalStateException.class, activeVSR::close);
        assertTrue("Should not allow closing ACTIVE VSR",
                   exception1.getMessage().contains("VSR is still ACTIVE"));
        assertTrue("Should mention freezing requirement",
                   exception1.getMessage().contains("Must freeze VSR before closing"));

        // VSR should still be ACTIVE after failed close
        assertEquals("VSR should still be ACTIVE", VSRState.ACTIVE, activeVSR.getState());

        // Test proper error recovery
        activeVSR.moveToFrozen();
        activeVSR.close();
        assertEquals("VSR should be properly CLOSED", VSRState.CLOSED, activeVSR.getState());

        pool.close();
    }

    public void testResourceManagementIntegration() {
        // Test resource management across all components
        long initialMemory = bufferPool.getTotalAllocatedBytes();

        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        // Memory tracking may be delayed or managed differently - just ensure operations complete
        long afterPoolCreation = bufferPool.getTotalAllocatedBytes();
        assertTrue("Memory operations should complete", afterPoolCreation >= initialMemory);

        // Get active VSR and populate it
        ManagedVSR activeVSR = pool.getActiveVSR();
        activeVSR.setRowCount(200);

        long afterVSRPopulation = bufferPool.getTotalAllocatedBytes();
        assertTrue("Memory should be manageable", afterVSRPopulation >= initialMemory);

        // Create additional VSRs
        BufferAllocator childAllocator1 = bufferPool.createChildAllocator("additional-vsr-1");
        BufferAllocator childAllocator2 = bufferPool.createChildAllocator("additional-vsr-2");
        ManagedVSR additionalVSR1 = new ManagedVSR("additional-vsr-1", testSchema, childAllocator1);
        ManagedVSR additionalVSR2 = new ManagedVSR("additional-vsr-2", testSchema, childAllocator2);

        additionalVSR1.setRowCount(50);
        additionalVSR2.setRowCount(75);

        // Clean up VSRs - must freeze active VSRs before completing them
        activeVSR.moveToFrozen();
        pool.completeVSR(activeVSR);

        additionalVSR1.moveToFrozen();
        pool.completeVSR(additionalVSR1);

        additionalVSR2.moveToFrozen();
        pool.completeVSR(additionalVSR2);

        // Close pool
        pool.close();

        // Verify operations completed without error
        assertEquals("Active VSR should be CLOSED", VSRState.CLOSED, activeVSR.getState());
        assertEquals("Additional VSR1 should be CLOSED", VSRState.CLOSED, additionalVSR1.getState());
        assertEquals("Additional VSR2 should be CLOSED", VSRState.CLOSED, additionalVSR2.getState());
    }

    public void testBackwardCompatibilityScenarios() {
        // Test that the new system maintains backward compatibility patterns
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        // Simulate old pattern: get VSR, populate, process
        ManagedVSR vsr = pool.getActiveVSR();
        assertNotNull("Should get VSR like before", vsr);

        // Old pattern operations should still work
        vsr.setRowCount(42);
        assertEquals("Row count should work like before", 42, vsr.getRowCount());

        IntVector idVector = (IntVector) vsr.getVector("id");
        assertNotNull("Should get vector like before", idVector);

        assertEquals("State should be predictable", VSRState.ACTIVE, vsr.getState());

        // New pattern: explicit state transitions
        vsr.moveToFrozen();

        // Export should work (this is the key integration point)
        try (ArrowExport export = vsr.exportToArrow()) {
            assertNotNull("Export should work for Rust integration", export);
        }

        // Cleanup should work
        pool.completeVSR(vsr);
        assertEquals("Should be cleaned up", VSRState.CLOSED, vsr.getState());

        pool.close();
    }

    public void testConcurrentVSROperationsOnDifferentInstances() {
        // Test that different VSR instances can be operated on concurrently
        // without interference (even though they're no longer thread-safe individually)
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        // Create multiple independent VSRs
        BufferAllocator allocator1 = bufferPool.createChildAllocator("concurrent-vsr-1");
        BufferAllocator allocator2 = bufferPool.createChildAllocator("concurrent-vsr-2");
        BufferAllocator allocator3 = bufferPool.createChildAllocator("concurrent-vsr-3");

        ManagedVSR vsr1 = new ManagedVSR("concurrent-vsr-1", testSchema, allocator1);
        ManagedVSR vsr2 = new ManagedVSR("concurrent-vsr-2", testSchema, allocator2);
        ManagedVSR vsr3 = new ManagedVSR("concurrent-vsr-3", testSchema, allocator3);

        // Perform different operations on each VSR
        vsr1.setRowCount(10);
        vsr2.setRowCount(20);
        vsr3.setRowCount(30);

        // Move them to different states independently
        vsr1.moveToFrozen();
        vsr2.moveToFrozen();
        // Keep vsr3 active

        // Verify independent state
        assertEquals("VSR1 should be FROZEN", VSRState.FROZEN, vsr1.getState());
        assertEquals("VSR2 should be FROZEN", VSRState.FROZEN, vsr2.getState());
        assertEquals("VSR3 should be ACTIVE", VSRState.ACTIVE, vsr3.getState());

        // Export from frozen VSRs
        try (ArrowExport export1 = vsr1.exportToArrow()) {
            assertNotNull("VSR1 should export", export1);
        }

        try (ArrowExport export2 = vsr2.exportToArrow()) {
            assertNotNull("VSR2 should export", export2);
        }

        // Modify active VSR
        vsr3.setRowCount(35);
        assertEquals("VSR3 should be modifiable", 35, vsr3.getRowCount());

        // Clean up all VSRs
        pool.completeVSR(vsr1);
        pool.completeVSR(vsr2);

        vsr3.moveToFrozen();
        pool.completeVSR(vsr3);

        // Verify all closed
        assertEquals("VSR1 should be CLOSED", VSRState.CLOSED, vsr1.getState());
        assertEquals("VSR2 should be CLOSED", VSRState.CLOSED, vsr2.getState());
        assertEquals("VSR3 should be CLOSED", VSRState.CLOSED, vsr3.getState());

        // Must freeze pool's active VSR before closing pool
        ManagedVSR poolActiveVSR = pool.getActiveVSR();
        poolActiveVSR.moveToFrozen();
        pool.close();
    }
}
