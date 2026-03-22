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
}
