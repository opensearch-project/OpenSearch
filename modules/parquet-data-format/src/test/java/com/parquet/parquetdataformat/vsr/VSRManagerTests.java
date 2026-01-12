/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.vsr;

import com.parquet.parquetdataformat.bridge.ArrowExport;
import com.parquet.parquetdataformat.bridge.ParquetFileMetadata;
import com.parquet.parquetdataformat.bridge.RustBridge;
import com.parquet.parquetdataformat.memory.ArrowBufferPool;
import com.parquet.parquetdataformat.writer.ParquetDocumentInput;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.Types;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.common.settings.Settings;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Integration tests for VSRManager covering document processing workflows and state management
 * through the VSRManager layer rather than direct ManagedVSR manipulation.
 */
public class VSRManagerTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private ArrowBufferPool bufferPool;
    private Schema testSchema;
    private String testFileName;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
        bufferPool = new ArrowBufferPool(Settings.EMPTY);

        // Create a simple test schema
        Field idField = new Field("id", FieldType.nullable(Types.MinorType.INT.getType()), null);
        Field nameField = new Field("name", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        testSchema = new Schema(Arrays.asList(idField, nameField));

        testFileName = "test-file-" + System.currentTimeMillis() + ".parquet";
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

    // ===== VSRManager Integration Tests =====

    public void testVSRManagerInitializationAndActiveVSR() throws Exception {
        // Test VSRManager initialization through constructor
        VSRManager vsrManager = new VSRManager(testFileName, testSchema, bufferPool);

        // VSRManager should have an active VSR
        assertNotNull("VSRManager should have active VSR", vsrManager.getActiveManagedVSR());
        assertEquals("Active VSR should be in ACTIVE state", VSRState.ACTIVE, vsrManager.getActiveManagedVSR().getState());

        // VSR should start with 0 rows
        assertEquals("Active VSR should start with 0 rows", 0, vsrManager.getActiveManagedVSR().getRowCount());

        // Follow proper VSRManager lifecycle: Write → Flush → Close
        // Since we haven't written data, simulate minimal data for flush
        vsrManager.getActiveManagedVSR().setRowCount(1);

        // Flush before close (transitions VSR to FROZEN)
        FlushIn flushIn = Mockito.mock(FlushIn.class);
        ParquetFileMetadata flushResult = vsrManager.flush(flushIn);
        assertNotNull("Flush should return metadata", flushResult);
        assertEquals("VSR should be FROZEN after flush", VSRState.FROZEN, vsrManager.getActiveManagedVSR().getState());

        // Now close should succeed
        vsrManager.close();
    }

    public void testDocumentAdditionThroughVSRManager() throws Exception {
        // Test document addition through VSRManager.addToManagedVSR()
        VSRManager vsrManager = new VSRManager(testFileName, testSchema, bufferPool);

        // Create a document to add
        ParquetDocumentInput document = new ParquetDocumentInput(vsrManager.getActiveManagedVSR());

        // Create mock field types and add fields to document
        MappedFieldType idFieldType = Mockito.mock(MappedFieldType.class);
        Mockito.when(idFieldType.typeName()).thenReturn("integer");
        document.addField(idFieldType, 42);

        MappedFieldType nameFieldType = Mockito.mock(MappedFieldType.class);
        Mockito.when(nameFieldType.typeName()).thenReturn("keyword");
        document.addField(nameFieldType, "test-document");

        // Add document through VSRManager
        WriteResult result = vsrManager.addToManagedVSR(document);
        assertNotNull("Write result should not be null", result);

        // VSR should still be ACTIVE after document addition
        assertEquals("VSR should remain ACTIVE after document addition",
                    VSRState.ACTIVE, vsrManager.getActiveManagedVSR().getState());

        // Follow proper VSRManager lifecycle: Write → Flush → Close
        // Flush before close (transitions VSR to FROZEN)
        FlushIn flushIn = Mockito.mock(FlushIn.class);
        ParquetFileMetadata flushResult = vsrManager.flush(flushIn);
        assertNotNull("Flush should return metadata", flushResult);
        assertEquals("VSR should be FROZEN after flush", VSRState.FROZEN, vsrManager.getActiveManagedVSR().getState());

        // Now close should succeed
        vsrManager.close();
    }

    public void testFlushThroughVSRManager() throws Exception {
        // Test flush workflow through VSRManager.flush()
        VSRManager vsrManager = new VSRManager(testFileName, testSchema, bufferPool);

        // Add some data first
        vsrManager.getActiveManagedVSR().setRowCount(10); // Simulate data addition

        // Flush through VSRManager (create mock FlushIn)
        FlushIn flushIn = Mockito.mock(FlushIn.class);
        ParquetFileMetadata result = vsrManager.flush(flushIn);

        assertNotNull("Flush should return metadata", result);

        // VSR should be FROZEN after flush
        assertEquals("VSR should be FROZEN after flush",
                    VSRState.FROZEN, vsrManager.getActiveManagedVSR().getState());

        vsrManager.close();
    }

    public void testVSRManagerStateTransitionWorkflow() throws Exception {
        // Test the complete workflow: create -> add data -> flush -> close
        VSRManager vsrManager = new VSRManager(testFileName, testSchema, bufferPool);

        // 1. Initial state - VSR should be ACTIVE
        assertEquals("Initial VSR should be ACTIVE", VSRState.ACTIVE, vsrManager.getActiveManagedVSR().getState());

        // 2. Add data (simulate document processing)
        vsrManager.getActiveManagedVSR().setRowCount(5);
        assertEquals("VSR should have data", 5, vsrManager.getActiveManagedVSR().getRowCount());

        // 3. Flush - should transition VSR to FROZEN
        FlushIn flushIn = Mockito.mock(FlushIn.class);
        ParquetFileMetadata flushResult = vsrManager.flush(flushIn);

        assertNotNull("Flush should return metadata", flushResult);
        assertEquals("VSR should be FROZEN after flush", VSRState.FROZEN, vsrManager.getActiveManagedVSR().getState());
        assertTrue("VSR should be immutable when frozen", vsrManager.getActiveManagedVSR().isImmutable());

        // 4. Close - cleanup
        vsrManager.close();
    }

    // ===== Integration with VSRPool Pattern Tests =====

    public void testVSRLifecycleIntegrationPattern() {
        // Test the integration pattern between VSRManager and VSRPool
        String vsrId = "test-vsr-integration-" + System.currentTimeMillis();
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // 1. VSRPool creates active VSR
        assertEquals("VSR starts ACTIVE", VSRState.ACTIVE, managedVSR.getState());

        // 2. VSRManager populates data
        managedVSR.setRowCount(15);

        // 3. VSRPool/VSRManager decides to freeze for flushing
        managedVSR.moveToFrozen();
        assertTrue("VSR should be immutable after freezing", managedVSR.isImmutable());

        // 4. VSRManager exports for Rust processing
        try (ArrowExport export = managedVSR.exportToArrow()) {
            assertNotNull("Export should succeed", export);
        }

        // 5. After processing, resources are cleaned up
        managedVSR.close();
        assertEquals("VSR should be CLOSED", VSRState.CLOSED, managedVSR.getState());
    }

    public void testMultipleVSRsWithDifferentStates() {
        // Test managing multiple VSRs in different states (as VSRManager might do)
        String vsrId1 = "test-vsr-1-" + System.currentTimeMillis();
        String vsrId2 = "test-vsr-2-" + System.currentTimeMillis();

        // Use child allocators instead of new RootAllocators to avoid memory leaks
        BufferAllocator childAllocator1 = allocator.newChildAllocator("vsr1", 0, Long.MAX_VALUE);
        BufferAllocator childAllocator2 = allocator.newChildAllocator("vsr2", 0, Long.MAX_VALUE);

        ManagedVSR activeVSR = new ManagedVSR(vsrId1, testSchema, childAllocator1);
        ManagedVSR frozenVSR = new ManagedVSR(vsrId2, testSchema, childAllocator2);

        // Set up different states
        activeVSR.setRowCount(5);

        frozenVSR.setRowCount(10);
        frozenVSR.moveToFrozen();

        // Verify states
        assertEquals("First VSR should be ACTIVE", VSRState.ACTIVE, activeVSR.getState());
        assertEquals("Second VSR should be FROZEN", VSRState.FROZEN, frozenVSR.getState());

        // Verify operations work correctly for each state
        activeVSR.setRowCount(7); // Should work

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
            frozenVSR.setRowCount(12); // Should fail
        });
        assertTrue("Should not allow modification of frozen VSR",
                   exception.getMessage().contains("Cannot modify VSR in state: FROZEN"));

        // Export should only work for frozen VSR
        expectThrows(IllegalStateException.class, activeVSR::exportToArrow);

        try (ArrowExport export = frozenVSR.exportToArrow()) {
            assertNotNull("Frozen VSR export should work", export);
        }

        // Clean up - must freeze active VSR before closing
        activeVSR.moveToFrozen();
        activeVSR.close();
        frozenVSR.close();
    }

    // ===== Error Handling Tests =====

    public void testVSRManagerCloseWithoutFlushFails() throws Exception {
        // Test that VSRManager.close() fails when VSRs are still in ACTIVE state (not flushed)
        VSRManager vsrManager = new VSRManager(testFileName, testSchema, bufferPool);

        // Get active VSR and add some data
        assertEquals("VSR should be ACTIVE", VSRState.ACTIVE, vsrManager.getActiveManagedVSR().getState());
        vsrManager.getActiveManagedVSR().setRowCount(5); // Simulate data addition

        // Try to close without flushing - should fail
        IllegalStateException exception = expectThrows(IllegalStateException.class, vsrManager::close);

        // Verify the error message mentions the VSR is still ACTIVE
        assertTrue("Should mention VSR is still ACTIVE",
                   exception.getMessage().contains("VSR is still ACTIVE"));
        assertTrue("Should mention must freeze first",
                   exception.getMessage().contains("Must freeze VSR before closing"));

        // VSR should still be in ACTIVE state after failed close
        assertEquals("VSR should still be ACTIVE", VSRState.ACTIVE, vsrManager.getActiveManagedVSR().getState());

        // Proper cleanup: flush then close
        FlushIn flushIn = Mockito.mock(FlushIn.class);
        vsrManager.flush(flushIn);
        assertEquals("VSR should be FROZEN after flush", VSRState.FROZEN, vsrManager.getActiveManagedVSR().getState());
        vsrManager.close(); // Should succeed now
    }

    public void testVSRManagerCloseEmptyButUnflushedFails() throws Exception {
        // Test that even an empty VSRManager must be flushed before closing
        VSRManager vsrManager = new VSRManager(testFileName, testSchema, bufferPool);

        // Get active VSR (no data added, but still ACTIVE)
        assertEquals("VSR should be ACTIVE", VSRState.ACTIVE, vsrManager.getActiveManagedVSR().getState());
        assertEquals("VSR should have 0 rows", 0, vsrManager.getActiveManagedVSR().getRowCount());

        // Try to close without flushing - should fail even with no data
        IllegalStateException exception = expectThrows(IllegalStateException.class, vsrManager::close);

        assertTrue("Should mention VSR is still ACTIVE",
                   exception.getMessage().contains("VSR is still ACTIVE"));

        // Must flush first, even with no data
        vsrManager.getActiveManagedVSR().setRowCount(1); // Need minimal data for flush to work
        FlushIn flushIn = Mockito.mock(FlushIn.class);
        vsrManager.flush(flushIn);
        vsrManager.close(); // Should succeed now
    }

    public void testInvalidStateTransitionHandling() {
        // Test error handling for invalid state transitions that VSRManager might encounter
        String vsrId = "test-vsr-error-" + System.currentTimeMillis();
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // Try to close active VSR (should fail - must freeze first)
        IllegalStateException exception = expectThrows(IllegalStateException.class, managedVSR::close);

        assertTrue("Should mention VSR is still ACTIVE",
                   exception.getMessage().contains("VSR is still ACTIVE"));
        assertTrue("Should mention must freeze first",
                   exception.getMessage().contains("Must freeze VSR before closing"));

        // VSR should still be in ACTIVE state after failed close
        assertEquals("VSR should still be ACTIVE", VSRState.ACTIVE, managedVSR.getState());

        // Proper cleanup
        managedVSR.moveToFrozen();
        managedVSR.close();
    }

    // ===== Mock-based Rust Integration Tests =====

    public void testRustBridgeIntegrationPattern() {
        // Test the pattern of integration with RustBridge (without actually calling native code)
        String vsrId = "test-vsr-rust-" + System.currentTimeMillis();
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // Populate VSR as VSRManager would
        managedVSR.setRowCount(20);

        // Freeze before export (as VSRManager does)
        managedVSR.moveToFrozen();

        // Export for Rust (simulating VSRManager calling RustBridge.write)
        try (ArrowExport export = managedVSR.exportToArrow()) {
            assertNotNull("Export should be ready for Rust", export);

            // Verify addresses are valid (would be passed to RustBridge.write)
            assertTrue("Array address should be valid for Rust", export.getArrayAddress() != 0);
            assertTrue("Schema address should be valid for Rust", export.getSchemaAddress() != 0);

            // In real VSRManager, this would be:
            // RustBridge.write(fileName, export.getArrayAddress(), export.getSchemaAddress());
        }

        // After Rust processing, clean up
        managedVSR.close();
    }

    // ===== Resource Management Tests =====

    public void testResourceCleanupAfterStateTransitions() {
        // Test that resources are properly managed through state transitions
        String vsrId = "test-vsr-cleanup-" + System.currentTimeMillis();
        ManagedVSR managedVSR = new ManagedVSR(vsrId, testSchema, allocator);

        // Add data and verify memory allocation
        managedVSR.setRowCount(100);
        long initialMemory = allocator.getAllocatedMemory();
        assertTrue("Should have allocated memory", initialMemory > 0);

        // Freeze (state change should not affect memory)
        managedVSR.moveToFrozen();
        assertTrue("Memory should still be allocated", allocator.getAllocatedMemory() >= initialMemory);

        // Export (should not significantly change memory usage)
        try (ArrowExport export = managedVSR.exportToArrow()) {
            assertNotNull("Export should work", export);
            // Memory might increase slightly for export structures
        }

        // Close should clean up resources
        managedVSR.close();
        assertEquals("VSR should be CLOSED", VSRState.CLOSED, managedVSR.getState());

        // Note: Exact memory cleanup testing is difficult without more intrusive testing
        // but we verify the state transitions work correctly
    }
}
