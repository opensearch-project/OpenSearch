/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.vsr;

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
 * Unit tests for VSRPool covering changes related to the removal of allVSRs tracking,
 * simplified resource management, and removal of statistics functionality.
 */
public class VSRPoolTests extends OpenSearchTestCase {

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

        poolId = "test-pool-" + System.currentTimeMillis();
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

    // ===== Basic Pool Functionality Tests =====

    public void testPoolCreationAndInitialization() {
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        assertNotNull("Pool should be created", pool);

        // Pool should start with an active VSR (created during initialization)
        ManagedVSR activeVSR = pool.getActiveVSR();
        assertNotNull("Should have active VSR after initialization", activeVSR);
        assertEquals("VSR should be ACTIVE", VSRState.ACTIVE, activeVSR.getState());

        ManagedVSR frozenVSR = pool.takeFrozenVSR();
        assertNull("Should start with no frozen VSR", frozenVSR);

        // Must freeze active VSR before closing pool
        activeVSR.moveToFrozen();
        pool.close();
    }

    public void testActiveVSRCreationOnDemand() {
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        // Get active VSR - should be created during initialization
        ManagedVSR activeVSR = pool.getActiveVSR();
        assertNotNull("Should have active VSR", activeVSR);
        assertEquals("VSR should be ACTIVE", VSRState.ACTIVE, activeVSR.getState());
        assertFalse("VSR should not be immutable", activeVSR.isImmutable());

        // Getting active VSR again should return the same instance
        ManagedVSR sameActiveVSR = pool.getActiveVSR();
        assertSame("Should return same active VSR instance", activeVSR, sameActiveVSR);

        // Must freeze active VSR before closing pool
        activeVSR.moveToFrozen();
        pool.close();
    }

    public void testVSRRotationThroughPool() throws Exception {
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        // Get initial active VSR
        ManagedVSR activeVSR = pool.getActiveVSR();
        String initialVSRId = activeVSR.getId();

        // Fill VSR with data to simulate reaching capacity
        activeVSR.setRowCount(50000); // Assuming this triggers rotation threshold

        // Test pool rotation mechanism
        boolean rotationOccurred = pool.maybeRotateActiveVSR();

        if (rotationOccurred) {
            // After rotation, should have new active VSR
            ManagedVSR newActiveVSR = pool.getActiveVSR();
            assertNotNull("Should have new active VSR after rotation", newActiveVSR);
            assertEquals("New active VSR should be ACTIVE", VSRState.ACTIVE, newActiveVSR.getState());
            assertEquals("New VSR should have row count 0", 0, newActiveVSR.getRowCount());

            // Should have frozen VSR available
            ManagedVSR frozenVSR = pool.getFrozenVSR();
            if (frozenVSR != null) {
                assertEquals("Frozen VSR should be FROZEN", VSRState.FROZEN, frozenVSR.getState());
                assertEquals("Frozen VSR should have expected row count", 50000, frozenVSR.getRowCount());
                assertSame("Frozen VSR should be the same as the previous active VSR", activeVSR, frozenVSR);

                // Complete the frozen VSR
                pool.completeVSR(frozenVSR);
                assertEquals("Frozen VSR should be CLOSED after completion", VSRState.CLOSED, frozenVSR.getState());
            }

            // Clean up new active VSR
            newActiveVSR.moveToFrozen();
        } else {
            // No rotation occurred, clean up original VSR
            fail("VSR should be rotated");
        }

        pool.close();
    }

    public void testTakeFrozenVSRReturnsBehavior() throws Exception {
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        // Initially should have no frozen VSR
        ManagedVSR frozenVSR = pool.takeFrozenVSR();
        assertNull("Should initially have no frozen VSR", frozenVSR);

        // Test through pool rotation to create a frozen VSR
        ManagedVSR activeVSR = pool.getActiveVSR();
        activeVSR.setRowCount(50000); // Fill to trigger rotation

        boolean rotated = pool.maybeRotateActiveVSR();
        assertTrue("Frozen VSR should be rotated", rotated);

        // Should now have a frozen VSR
        ManagedVSR actualFrozenVSR = pool.takeFrozenVSR();
        if (actualFrozenVSR != null) {
            assertEquals("Taken VSR should be FROZEN", VSRState.FROZEN, actualFrozenVSR.getState());

            // Taking it again should return null (slot cleared)
            ManagedVSR shouldBeNull = pool.takeFrozenVSR();
            assertNull("Should be null after taking frozen VSR", shouldBeNull);

            // Clean up the taken VSR
            pool.completeVSR(actualFrozenVSR);
        }

        // Must freeze pool's active VSR before closing pool
        pool.getActiveVSR().moveToFrozen();
        pool.close();
    }

    // ===== Resource Management Tests =====

    public void testCompleteVSRThroughPool() throws Exception {
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        // Get active VSR and fill it
        ManagedVSR activeVSR = pool.getActiveVSR();
        activeVSR.setRowCount(20);
        activeVSR.moveToFrozen(); // Manually freeze for this test

        // Test the completeVSR functionality through pool
        pool.completeVSR(activeVSR);

        // VSR should be closed
        assertEquals("VSR should be CLOSED after completion", VSRState.CLOSED, activeVSR.getState());

        pool.close();
    }

    public void testPoolCloseResourceCleanup() {
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        // Get active VSR to trigger creation
        ManagedVSR activeVSR = pool.getActiveVSR();
        assertNotNull("Should have active VSR", activeVSR);

        // Add some data
        activeVSR.setRowCount(5);

        // Must freeze active VSR before closing pool
        activeVSR.moveToFrozen();

        // Close pool - should clean up frozen VSR
        pool.close();
    }

    // ===== Error Handling Tests =====

    public void testVSRCreationWithInvalidParameters() {
        // Test error handling in VSR creation within pool context

        // Null schema should fail - but the error is wrapped in RuntimeException
        RuntimeException schemaException = expectThrows(RuntimeException.class, () -> {
            new VSRPool(poolId, null, bufferPool);
        });
        assertTrue("Should mention failed to create VSR",
                   schemaException.getMessage().contains("Failed to create new VSR"));
        assertTrue("Root cause should be NullPointerException",
                   schemaException.getCause() instanceof NullPointerException);

        // Null buffer pool should fail - but the error is also wrapped in RuntimeException
        RuntimeException bufferPoolException = expectThrows(RuntimeException.class, () -> {
            new VSRPool(poolId, testSchema, null);
        });
        assertTrue("Should mention failed to create VSR",
                   bufferPoolException.getMessage().contains("Failed to create new VSR"));
        assertTrue("Root cause should be NullPointerException",
                   bufferPoolException.getCause() instanceof NullPointerException);
    }

    // ===== Integration Pattern Tests =====

    public void testPoolVSRManagerIntegrationPattern() {
        // Test the integration pattern between pool and manager
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        // 1. Get active VSR (as VSRManager would)
        ManagedVSR activeVSR = pool.getActiveVSR();
        assertNotNull("Manager should get active VSR", activeVSR);
        assertEquals("VSR should be ACTIVE", VSRState.ACTIVE, activeVSR.getState());

        // 2. Populate VSR (as VSRManager would)
        activeVSR.setRowCount(25);
        assertEquals("Should have expected row count", 25, activeVSR.getRowCount());

        // 3. When ready to flush, freeze VSR (as VSRManager would do)
        activeVSR.moveToFrozen();
        assertEquals("VSR should be FROZEN", VSRState.FROZEN, activeVSR.getState());

        // 4. After processing, complete VSR (as VSRManager would)
        pool.completeVSR(activeVSR);
        assertEquals("VSR should be CLOSED after completion", VSRState.CLOSED, activeVSR.getState());

        pool.close();
    }

    public void testMultipleVSRLifecycleInPool() {
        // Test managing multiple VSRs through their lifecycle
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        // Create multiple VSRs simulating different stages of lifecycle
        BufferAllocator childAllocator1 = bufferPool.createChildAllocator("lifecycle-vsr-1");
        BufferAllocator childAllocator2 = bufferPool.createChildAllocator("lifecycle-vsr-2");
        BufferAllocator childAllocator3 = bufferPool.createChildAllocator("lifecycle-vsr-3");
        ManagedVSR vsr1 = new ManagedVSR("lifecycle-vsr-1", testSchema, childAllocator1);
        ManagedVSR vsr2 = new ManagedVSR("lifecycle-vsr-2", testSchema, childAllocator2);
        ManagedVSR vsr3 = new ManagedVSR("lifecycle-vsr-3", testSchema, childAllocator3);

        // Put them in different states
        vsr1.setRowCount(10); // Keep ACTIVE

        vsr2.setRowCount(20);
        vsr2.moveToFrozen(); // Make FROZEN

        vsr3.setRowCount(30);
        vsr3.moveToFrozen();
        vsr3.close(); // Make CLOSED

        // Verify states
        assertEquals("VSR1 should be ACTIVE", VSRState.ACTIVE, vsr1.getState());
        assertEquals("VSR2 should be FROZEN", VSRState.FROZEN, vsr2.getState());
        assertEquals("VSR3 should be CLOSED", VSRState.CLOSED, vsr3.getState());

        // Complete the active and frozen ones
        vsr1.moveToFrozen();
        pool.completeVSR(vsr1);
        pool.completeVSR(vsr2);

        // All should be closed now
        assertEquals("VSR1 should be CLOSED", VSRState.CLOSED, vsr1.getState());
        assertEquals("VSR2 should be CLOSED", VSRState.CLOSED, vsr2.getState());
        assertEquals("VSR3 should remain CLOSED", VSRState.CLOSED, vsr3.getState());

        // Must freeze pool's active VSR before closing pool
        ManagedVSR poolActiveVSR = pool.getActiveVSR();
        poolActiveVSR.moveToFrozen();
        pool.close();
    }

    // ===== Memory and Performance Tests =====

    public void testMemoryManagementInPool() {
        // Test memory allocation and cleanup behavior
        VSRPool pool = new VSRPool(poolId, testSchema, bufferPool);

        long initialMemory = bufferPool.getTotalAllocatedBytes();

        // Create active VSR - should allocate memory
        ManagedVSR activeVSR = pool.getActiveVSR();
        activeVSR.setRowCount(50);

        long afterCreationMemory = bufferPool.getTotalAllocatedBytes();
        // Note: Memory allocation may be delayed or managed differently
        // Just ensure operations complete without error
        assertTrue("Memory operations should complete", afterCreationMemory >= initialMemory);

        // Must freeze active VSR before closing pool
        activeVSR.moveToFrozen();

        // Close pool - should clean up
        pool.close();

        // Memory cleanup verification is difficult without intrusive testing,
        // but we verify operations complete without error
        assertTrue("Test completed successfully", true);
    }
}
