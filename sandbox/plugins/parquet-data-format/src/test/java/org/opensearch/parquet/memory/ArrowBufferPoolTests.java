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
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowBufferPoolTests extends OpenSearchTestCase {

    private ArrowNativeAllocator nativeAllocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Each test gets its own allocator with the standard pools pre-created.
        // Production code receives this via dependency injection; tests build it explicitly.
        nativeAllocator = new ArrowNativeAllocator();
        nativeAllocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_INGEST, 0L, Long.MAX_VALUE, null);
    }

    @Override
    public void tearDown() throws Exception {
        if (nativeAllocator != null) {
            nativeAllocator.close();
            nativeAllocator = null;
        }
        super.tearDown();
    }

    public void testAllocatedBytesIncreasesOnAllocation() {
        ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY, nativeAllocator);
        BufferAllocator child = pool.createChildAllocator("alloc-test");
        try {
            assertNotNull(child);
            assertEquals(0, pool.getTotalAllocatedBytes());
            ArrowBuf buf = child.buffer(1024);
            assertTrue(pool.getTotalAllocatedBytes() > 0);
            buf.close();
        } finally {
            child.close();
            pool.close();
        }
    }

    public void testMultipleChildAllocators() {
        ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY, nativeAllocator);
        BufferAllocator c1 = pool.createChildAllocator("c1");
        BufferAllocator c2 = pool.createChildAllocator("c2");
        try {
            ArrowBuf b1 = c1.buffer(512);
            ArrowBuf b2 = c2.buffer(512);
            assertTrue(pool.getTotalAllocatedBytes() >= 1024);
            b1.close();
            b2.close();
        } finally {
            c1.close();
            c2.close();
            pool.close();
        }
    }

    public void testAllocatedBytesDecreasesAfterFree() {
        ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY, nativeAllocator);
        BufferAllocator child = pool.createChildAllocator("free-test");
        try {
            ArrowBuf buf = child.buffer(1024);
            assertTrue(pool.getTotalAllocatedBytes() > 0);
            buf.close();
        } finally {
            child.close();
            pool.close();
        }
        // Pool's BufferAllocator is owned by the framework; the assertion now confirms
        // the *child* that we closed returned its bytes to the parent.
        assertEquals(0, pool.getTotalAllocatedBytes());
    }

    /**
     * Multiple ArrowBufferPool instances backed by the same NativeAllocator must report only
     * their own memory usage via getTotalAllocatedBytes(), not the pool-wide total.
     */
    public void testMultiplePoolInstancesReportOwnUsageNotPoolWideTotal() {
        ArrowBufferPool pool1 = new ArrowBufferPool(Settings.EMPTY, nativeAllocator);
        ArrowBufferPool pool2 = new ArrowBufferPool(Settings.EMPTY, nativeAllocator);

        BufferAllocator child1 = pool1.createChildAllocator("pool-1");
        BufferAllocator child2 = pool2.createChildAllocator("pool-2");

        ArrowBuf buf1 = child1.buffer(1024);
        ArrowBuf buf2 = child2.buffer(2048);

        long actualTotal = 1024 + 2048;

        // Sum of individual pool reports must equal the actual total.
        long sum = pool1.getTotalAllocatedBytes() + pool2.getTotalAllocatedBytes();
        assertEquals("Sum of per-pool bytes must equal actual total", actualTotal, sum);

        buf1.close();
        buf2.close();
        child1.close();
        child2.close();
        pool1.close();
        pool2.close();
    }

    public void testChildAllocatorEnforcedByParentPoolLimit() {
        // Constrain the framework's ingest pool to a finite limit.
        long poolLimit = 1024L * 1024;
        nativeAllocator.setPoolLimit(NativeAllocatorPoolConfig.POOL_INGEST, poolLimit);

        ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY, nativeAllocator);
        BufferAllocator child = pool.createChildAllocator("c-full");
        try {
            // Child can allocate within the parent pool's limit
            ArrowBuf buf = child.buffer(512 * 1024);
            assertNotNull(buf);
            buf.close();

            // Child cannot allocate more than the parent pool's limit
            expectThrows(org.apache.arrow.memory.OutOfMemoryException.class, () -> child.buffer(poolLimit + 1));
        } finally {
            child.close();
            pool.close();
        }
    }
}
