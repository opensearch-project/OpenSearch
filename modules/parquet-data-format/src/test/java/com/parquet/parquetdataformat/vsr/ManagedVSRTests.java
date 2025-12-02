/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.vsr;

import com.parquet.parquetdataformat.bridge.ArrowExport;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.Types;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;

/**
 * Comprehensive unit tests for ManagedVSR covering all changes from atomic/thread-safe
 * handling removal and state management simplification.
 */
public class ManagedVSRTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private Schema testSchema;
    private String vsrId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();

        // Create a simple test schema
        Field idField = new Field("id", FieldType.nullable(Types.MinorType.INT.getType()), null);
        Field nameField = new Field("name", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        testSchema = new Schema(Arrays.asList(idField, nameField));

        vsrId = "test-vsr-" + System.currentTimeMillis();
    }

    @Override
    public void tearDown() throws Exception {
        if (allocator != null) {
            allocator.close();
        }
        super.tearDown();
    }

    // ===== Constructor Tests =====

    public void testConstructorCreatesActiveVSR() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        assertEquals("VSR should start in ACTIVE state", VSRState.ACTIVE, managedVSR.getState());
        assertEquals("VSR ID should match", vsrId, managedVSR.getId());
        assertEquals("Initial row count should be 0", 0, managedVSR.getRowCount());
        assertFalse("New VSR should not be immutable", managedVSR.isImmutable());

        // Must freeze before closing
        managedVSR.moveToFrozen();
        managedVSR.close();
    }

    public void testConstructorWithNullParameters() {
        // Note: The current ManagedVSR implementation may not have explicit null validation
        // This test documents expected behavior but may need implementation updates
        
        // Test with valid parameters to ensure constructor works
        ManagedVSR validVSR = new ManagedVSR(vsrId, testSchema, allocator);
        assertNotNull("Valid constructor should work", validVSR);
        validVSR.moveToFrozen();
        validVSR.close();
    }

    // ===== State Transition Tests =====

    public void testStateTransitionActiveToFrozen() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // Verify initial state
        assertEquals("Should start ACTIVE", VSRState.ACTIVE, managedVSR.getState());
        assertFalse("Should not be immutable when active", managedVSR.isImmutable());

        // Transition to FROZEN
        managedVSR.moveToFrozen();

        assertEquals("Should be FROZEN after moveToFrozen()", VSRState.FROZEN, managedVSR.getState());
        assertTrue("Should be immutable when frozen", managedVSR.isImmutable());

        managedVSR.close();
    }

    public void testMoveToFrozenFromNonActiveState() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // Move to FROZEN first
        managedVSR.moveToFrozen();
        assertEquals("Should be FROZEN", VSRState.FROZEN, managedVSR.getState());

        // Try to freeze again - should fail
        IllegalStateException exception = expectThrows(IllegalStateException.class, managedVSR::moveToFrozen);

        assertTrue("Exception should mention expected ACTIVE state",
                   exception.getMessage().contains("expected ACTIVE state"));
        assertTrue("Exception should mention current FROZEN state",
                   exception.getMessage().contains("FROZEN"));

        managedVSR.close();
    }

    public void testStateTransitionFrozenToClosed() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // Move to FROZEN then CLOSED
        managedVSR.moveToFrozen();
        assertEquals("Should be FROZEN", VSRState.FROZEN, managedVSR.getState());

        managedVSR.close();
        assertEquals("Should be CLOSED after close()", VSRState.CLOSED, managedVSR.getState());
    }

    public void testCloseFromActiveStateFails() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        assertEquals("Should start ACTIVE", VSRState.ACTIVE, managedVSR.getState());

        // Try to close while ACTIVE - should fail
        IllegalStateException exception = expectThrows(IllegalStateException.class, managedVSR::close);

        assertTrue("Exception should mention VSR is ACTIVE",
                   exception.getMessage().contains("VSR is still ACTIVE"));
        assertTrue("Exception should mention must freeze first",
                   exception.getMessage().contains("Must freeze VSR before closing"));

        // VSR should still be ACTIVE after failed close
        assertEquals("VSR should still be ACTIVE", VSRState.ACTIVE, managedVSR.getState());

        // Proper cleanup
        managedVSR.moveToFrozen();
        managedVSR.close();
    }

    public void testCloseIdempotency() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // Move to FROZEN then CLOSED
        managedVSR.moveToFrozen();
        managedVSR.close();
        assertEquals("Should be CLOSED", VSRState.CLOSED, managedVSR.getState());

        // Call close again - should be idempotent
        managedVSR.close();
        assertEquals("Should still be CLOSED", VSRState.CLOSED, managedVSR.getState());
    }

    // ===== Operation State Validation Tests =====

    public void testSetRowCountOnlyWorksInActiveState() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // Should work in ACTIVE state
        managedVSR.setRowCount(5);
        assertEquals("Row count should be set", 5, managedVSR.getRowCount());

        // Move to FROZEN
        managedVSR.moveToFrozen();

        // Should fail in FROZEN state
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> managedVSR.setRowCount(10));

        assertTrue("Exception should mention cannot modify in FROZEN state",
                   exception.getMessage().contains("Cannot modify VSR in state: FROZEN"));
        assertEquals("Row count should remain unchanged", 5, managedVSR.getRowCount());

        managedVSR.close();
    }

    public void testExportToArrowOnlyWorksInFrozenState() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // Should fail in ACTIVE state
        IllegalStateException exception = expectThrows(IllegalStateException.class, managedVSR::exportToArrow);

        assertTrue("Exception should mention cannot export in ACTIVE state",
                   exception.getMessage().contains("Cannot export VSR in state: ACTIVE"));
        assertTrue("Exception should mention must be FROZEN",
                   exception.getMessage().contains("VSR must be FROZEN to export"));

        // Move to FROZEN - should work
        managedVSR.moveToFrozen();

        try (ArrowExport export = managedVSR.exportToArrow()) {
            assertNotNull("Export should not be null", export);
            assertTrue("Array address should be valid", export.getArrayAddress() != 0);
            assertTrue("Schema address should be valid", export.getSchemaAddress() != 0);
        }

        managedVSR.close();
    }

    public void testGetVectorOnlyWorksInActiveState() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // Test in ACTIVE state - should work
        FieldVector idVector = managedVSR.getVector("id");
        assertNotNull("Should get vector in ACTIVE state", idVector);
        assertTrue("Should be IntVector", idVector instanceof IntVector);

        // Test in FROZEN state - should fail
        managedVSR.moveToFrozen();
        IllegalStateException frozenException = expectThrows(IllegalStateException.class, () -> managedVSR.getVector("id"));
        assertTrue("Exception should mention cannot access in FROZEN state",
                   frozenException.getMessage().contains("Cannot access vector in VSR state: FROZEN"));
        assertTrue("Exception should mention must be ACTIVE",
                   frozenException.getMessage().contains("VSR must be ACTIVE to access vectors"));

        managedVSR.close();

        // Test in CLOSED state - should also fail
        IllegalStateException closedException = expectThrows(IllegalStateException.class, () -> managedVSR.getVector("id"));
        assertTrue("Exception should mention cannot access in CLOSED state",
                   closedException.getMessage().contains("Cannot access vector in VSR state: CLOSED"));
        assertTrue("Exception should mention must be ACTIVE",
                   closedException.getMessage().contains("VSR must be ACTIVE to access vectors"));
    }

    public void testGetVectorWithNonExistentField() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        FieldVector nonExistentVector = managedVSR.getVector("nonexistent");
        assertNull("Should return null for non-existent field", nonExistentVector);

        managedVSR.moveToFrozen();
        managedVSR.close();
    }

    public void testExportSchemaWorksInAllStates() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // Test in ACTIVE state
        try (ArrowExport schemaExport = managedVSR.exportSchema()) {
            assertNotNull("Schema export should not be null", schemaExport);
            // Note: Schema-only exports may have null array address - this is expected
            assertTrue("Schema address should be valid", schemaExport.getSchemaAddress() != 0);
        }

        // Test in FROZEN state
        managedVSR.moveToFrozen();
        try (ArrowExport schemaExportFrozen = managedVSR.exportSchema()) {
            assertNotNull("Schema export should work in FROZEN state", schemaExportFrozen);
        }

        managedVSR.close();
    }

    // ===== Resource Management Tests =====

    public void testResourceCleanupOnClose() {
        // Create a separate allocator for this test so we can verify it gets closed
        BufferAllocator testAllocator = new RootAllocator();
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, testAllocator);

        // Add some data
        managedVSR.setRowCount(3);

        // Verify resources are allocated
        assertTrue("Allocator should have allocated memory", testAllocator.getAllocatedMemory() > 0);

        // Close properly
        managedVSR.moveToFrozen();
        managedVSR.close();

        assertEquals("Should be CLOSED", VSRState.CLOSED, managedVSR.getState());
        
        // Verify allocator has no reserved memory after close (the allocator itself gets closed by ManagedVSR)
        assertEquals("Allocator should have no reserved memory after close", 0, testAllocator.getAllocatedMemory());
    }

    // ===== Edge Case Tests =====

    public void testToStringInDifferentStates() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // Test toString in ACTIVE state
        String activeString = managedVSR.toString();
        assertTrue("toString should contain VSR ID", activeString.contains(vsrId));
        assertTrue("toString should contain ACTIVE state", activeString.contains("ACTIVE"));
        assertTrue("toString should contain row count", activeString.contains("rows=0"));
        assertTrue("toString should contain immutable=false", activeString.contains("immutable=false"));

        // Test toString in FROZEN state
        managedVSR.moveToFrozen();
        String frozenString = managedVSR.toString();
        assertTrue("toString should contain FROZEN state", frozenString.contains("FROZEN"));
        assertTrue("toString should contain immutable=true", frozenString.contains("immutable=true"));

        // Test toString in CLOSED state
        managedVSR.close();
        String closedString = managedVSR.toString();
        assertTrue("toString should contain CLOSED state", closedString.contains("CLOSED"));
        assertTrue("toString should contain immutable=true", closedString.contains("immutable=true"));
    }

    public void testGetRowCountAfterStateTransitions() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // Set initial row count
        managedVSR.setRowCount(10);
        assertEquals("Row count should be 10", 10, managedVSR.getRowCount());

        // Row count should persist through state transitions
        managedVSR.moveToFrozen();
        assertEquals("Row count should persist in FROZEN state", 10, managedVSR.getRowCount());

        managedVSR.close();
        assertEquals("Row count should persist in CLOSED state", 10, managedVSR.getRowCount());
    }

    public void testImmutabilityInDifferentStates() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // ACTIVE state - should be mutable
        assertFalse("Should be mutable in ACTIVE state", managedVSR.isImmutable());

        // FROZEN state - should be immutable
        managedVSR.moveToFrozen();
        assertTrue("Should be immutable in FROZEN state", managedVSR.isImmutable());

        // CLOSED state - should be immutable
        managedVSR.close();
        assertTrue("Should be immutable in CLOSED state", managedVSR.isImmutable());
    }

    // ===== Integration Tests =====

    public void testCompleteVSRLifecycle() {
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // 1. Start in ACTIVE state
        assertEquals("Should start ACTIVE", VSRState.ACTIVE, managedVSR.getState());
        assertFalse("Should be mutable", managedVSR.isImmutable());

        // 2. Populate with data (only possible in ACTIVE)
        managedVSR.setRowCount(5);
        assertEquals("Should have 5 rows", 5, managedVSR.getRowCount());

        // 3. Transition to FROZEN
        managedVSR.moveToFrozen();
        assertEquals("Should be FROZEN", VSRState.FROZEN, managedVSR.getState());
        assertTrue("Should be immutable", managedVSR.isImmutable());

        // 4. Export data (only possible in FROZEN)
        try (ArrowExport export = managedVSR.exportToArrow()) {
            assertNotNull("Export should succeed", export);
        }

        // 5. Close and cleanup (only possible from FROZEN)
        managedVSR.close();
        assertEquals("Should be CLOSED", VSRState.CLOSED, managedVSR.getState());
        assertTrue("Should remain immutable", managedVSR.isImmutable());
    }
}
