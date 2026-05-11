/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.memory;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.common.settings.Settings;
import org.opensearch.parquet.ParquetSettings;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowBufferPoolTests extends OpenSearchTestCase {

    public void testAllocatedBytesIncreasesOnAllocation() {
        try (ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY)) {
            BufferAllocator child = pool.createChildAllocator("alloc-test");
            assertNotNull(child);
            assertEquals(0, pool.getTotalAllocatedBytes());
            ArrowBuf buf = child.buffer(1024);
            assertTrue(pool.getTotalAllocatedBytes() > 0);
            buf.close();
            child.close();
        }
    }

    public void testMultipleChildAllocators() {
        try (ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY)) {
            BufferAllocator c1 = pool.createChildAllocator("c1");
            BufferAllocator c2 = pool.createChildAllocator("c2");
            ArrowBuf b1 = c1.buffer(512);
            ArrowBuf b2 = c2.buffer(512);
            assertTrue(pool.getTotalAllocatedBytes() >= 1024);
            b1.close();
            b2.close();
            c1.close();
            c2.close();
        }
    }

    public void testAllocatedBytesDecreasesAfterFree() {
        try (ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY)) {
            BufferAllocator child = pool.createChildAllocator("free-test");
            ArrowBuf buf = child.buffer(1024);
            assertTrue(pool.getTotalAllocatedBytes() > 0);
            buf.close();
            child.close();
            assertEquals(0, pool.getTotalAllocatedBytes());
        }
    }

    public void testCloseWithOpenChildAllocatorThrows() {
        ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY);
        BufferAllocator child = pool.createChildAllocator("leaked-child");
        ArrowBuf buf = child.buffer(1024);
        // Closing pool with outstanding child allocations throws
        IllegalStateException e = expectThrows(IllegalStateException.class, pool::close);
        assertTrue(e.getMessage().contains("Memory was leaked"));
        // First close marked root as closed, so child.close() can't notify parent.
        // Release buffer directly — Arrow frees the underlying memory even if allocator is closed.
        buf.getReferenceManager().release();
    }

    public void testApplyLimitsUpdatesRootAndChildCaps() {
        // Start with an absolute root size so percentage-resolution paths don't depend on host
        // memory sizing. Floor 1mb keeps the resolver happy if a percentage is ever supplied.
        Settings initial = Settings.builder()
            .put(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), "8mb")
            .put(ParquetSettings.CHILD_ALLOCATION.getKey(), "2mb")
            .build();
        try (ArrowBufferPool pool = new ArrowBufferPool(initial)) {
            assertEquals(8L * 1024 * 1024, pool.getRootLimit());
            assertEquals(2L * 1024 * 1024, pool.getMaxChildAllocation());

            BufferAllocator childA = pool.createChildAllocator("child-a");
            assertEquals(2L * 1024 * 1024, childA.getLimit());

            // Bump both limits at runtime.
            Settings updated = Settings.builder()
                .put(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), "16mb")
                .put(ParquetSettings.CHILD_ALLOCATION.getKey(), "4mb")
                .build();
            pool.applyLimits(updated);

            assertEquals("root limit must be pushed to live RootAllocator", 16L * 1024 * 1024, pool.getRootLimit());
            assertEquals("child cap field tracks new value", 4L * 1024 * 1024, pool.getMaxChildAllocation());
            assertEquals("live child allocator must see new cap", 4L * 1024 * 1024, childA.getLimit());

            // New children created after applyLimits must also see the new cap.
            BufferAllocator childB = pool.createChildAllocator("child-b");
            assertEquals(4L * 1024 * 1024, childB.getLimit());

            childA.close();
            childB.close();
        }
    }

    public void testApplyLimitsLowerThanCurrentUsageDoesNotReclaim() {
        Settings initial = Settings.builder()
            .put(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), "16mb")
            .put(ParquetSettings.CHILD_ALLOCATION.getKey(), "4mb")
            .build();
        try (ArrowBufferPool pool = new ArrowBufferPool(initial)) {
            BufferAllocator child = pool.createChildAllocator("c");
            ArrowBuf buf = child.buffer(2L * 1024 * 1024); // 2mb in flight

            // Lower the root limit below current allocated memory.
            Settings tightened = Settings.builder()
                .put(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), "1mb")
                .put(ParquetSettings.CHILD_ALLOCATION.getKey(), "1mb")
                .build();
            pool.applyLimits(tightened);

            // Existing buffer is unaffected — Arrow does not reclaim already-allocated memory.
            assertTrue(pool.getTotalAllocatedBytes() >= 2L * 1024 * 1024);
            // Future allocations beyond the new cap fail.
            expectThrows(org.apache.arrow.memory.OutOfMemoryException.class, () -> child.buffer(2L * 1024 * 1024));

            buf.close();
            child.close();
        }
    }

    public void testApplyLimitsIsNoopWhenValuesUnchanged() {
        Settings settings = Settings.builder()
            .put(ParquetSettings.MAX_NATIVE_ALLOCATION.getKey(), "8mb")
            .put(ParquetSettings.CHILD_ALLOCATION.getKey(), "2mb")
            .build();
        try (ArrowBufferPool pool = new ArrowBufferPool(settings)) {
            long rootBefore = pool.getRootLimit();
            long childBefore = pool.getMaxChildAllocation();
            pool.applyLimits(settings);
            assertEquals(rootBefore, pool.getRootLimit());
            assertEquals(childBefore, pool.getMaxChildAllocation());
        }
    }
}
