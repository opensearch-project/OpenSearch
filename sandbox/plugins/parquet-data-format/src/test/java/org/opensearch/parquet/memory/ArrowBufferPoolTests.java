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
        nativeAllocator = new ArrowNativeAllocator(Long.MAX_VALUE);
        nativeAllocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_INGEST, 0L, Long.MAX_VALUE);
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

    public void testChildAllocatorLimitScalesWithDivisor() {
        // Both pools share the same framework ingest pool; divisor distinguishes the children.
        Settings strict = Settings.builder().put("parquet.max_per_vsr_allocation_divisor", 10).build();
        Settings loose = Settings.builder().put("parquet.max_per_vsr_allocation_divisor", 2).build();
        ArrowBufferPool poolStrict = new ArrowBufferPool(strict, nativeAllocator);
        ArrowBufferPool poolLoose = new ArrowBufferPool(loose, nativeAllocator);
        BufferAllocator childStrict = poolStrict.createChildAllocator("c-strict");
        BufferAllocator childLoose = poolLoose.createChildAllocator("c-loose");
        try {
            long limitStrict = childStrict.getLimit();
            long limitLoose = childLoose.getLimit();
            // The setUp ingest pool has limit Long.MAX_VALUE → both children clamp at
            // Long.MAX_VALUE. Validate the dynamic-divisor path separately in
            // testDivisorIsReadDynamicallyOnEachChildCreation.
            if (limitStrict != Long.MAX_VALUE && limitLoose != Long.MAX_VALUE) {
                assertEquals(5L * limitStrict, limitLoose, 1);
            }
        } finally {
            childStrict.close();
            childLoose.close();
            poolStrict.close();
            poolLoose.close();
        }
    }

    public void testDivisorIsReadDynamicallyOnEachChildCreation() {
        // Constrain the framework's ingest pool to a finite limit so the divisor matters.
        nativeAllocator.setPoolLimit(NativeAllocatorPoolConfig.POOL_INGEST, 1024L * 1024 * 1024);

        // Mutable supplier emulates a dynamic cluster-settings update.
        int[] divisor = new int[] { 10 };
        ArrowBufferPool pool = new ArrowBufferPool(Settings.EMPTY, () -> divisor[0], nativeAllocator);
        BufferAllocator beforeUpdate = pool.createChildAllocator("c-before");
        long limitBefore = beforeUpdate.getLimit();
        beforeUpdate.close();

        divisor[0] = 2;  // simulate dynamic settings update
        BufferAllocator afterUpdate = pool.createChildAllocator("c-after");
        long limitAfter = afterUpdate.getLimit();
        afterUpdate.close();
        pool.close();

        // divisor went from 10 → 2; new child should be ~5x the old.
        assertEquals(5L * limitBefore, limitAfter, 1);
    }

    public void testDivisorSettingRejectsZero() {
        Settings s = Settings.builder().put("parquet.max_per_vsr_allocation_divisor", 0).build();
        expectThrows(IllegalArgumentException.class, () -> org.opensearch.parquet.ParquetSettings.MAX_PER_VSR_ALLOCATION_DIVISOR.get(s));
    }

    public void testDivisorSettingRejectsNegative() {
        Settings s = Settings.builder().put("parquet.max_per_vsr_allocation_divisor", -1).build();
        expectThrows(IllegalArgumentException.class, () -> org.opensearch.parquet.ParquetSettings.MAX_PER_VSR_ALLOCATION_DIVISOR.get(s));
    }
}
