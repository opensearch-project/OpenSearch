/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.memory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Comprehensive test suite for ArrowBufferPool covering essential functionality:
 **/
public class ArrowBufferPoolTests extends OpenSearchTestCase {

    private ArrowBufferPool pool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        cleanupPool();
        super.tearDown();
    }

    private void cleanupPool() {
        if (pool != null) {
            try {
                pool.close();
            } catch (Exception e) {
                logger.warn("Failed to close pool during tearDown", e);
            } finally {
                pool = null;
            }
        }
    }

    private void assertDoesNotThrow(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            fail("Expected no exception, but got: " + e.getMessage());
        }
    }

    public void testConstructorInitializesWithCorrectDefaults() {
            pool = new ArrowBufferPool(Settings.EMPTY);

            assertNotNull("Pool should be created successfully", pool);

            long initialAllocatedBytes = pool.getTotalAllocatedBytes();
            assertEquals("Initial allocated bytes should be zero", 0L, initialAllocatedBytes);

            BufferAllocator childAllocator = pool.createChildAllocator("test-child");
            assertNotNull("Pool should create child allocator", childAllocator);

            long expectedMaxChildAllocation = 1024L * 1024L * 1024L; // 1GB in bytes
            assertEquals("Child allocator should have 1GB max allocation",
                        expectedMaxChildAllocation, childAllocator.getLimit());

            assertEquals("Child allocator should have correct name", "test-child", childAllocator.getName());

            assertEquals("Child allocator should start with zero allocation",
                        0L, childAllocator.getAllocatedMemory());

            childAllocator.close();
    }

    public void testCanCreateChildAllocator() {
            pool = new ArrowBufferPool(Settings.EMPTY);

            BufferAllocator childAllocator = pool.createChildAllocator("test");

            assertNotNull("Child allocator should not be null", childAllocator);

            assertEquals("Child allocator should have correct name", "test", childAllocator.getName());

            long expectedMaxAllocation = 1024L * 1024L * 1024L; // 1GB in bytes
            assertEquals("Child allocator should have 1GB maximum allocation",
                        expectedMaxAllocation, childAllocator.getLimit());

            assertEquals("Child allocator should start with zero allocated memory",
                        0L, childAllocator.getAllocatedMemory());

            BufferAllocator childAllocator2 = pool.createChildAllocator("test-2");
            assertNotNull("Second child allocator should not be null", childAllocator2);
            assertEquals("Second child allocator should have correct name", "test-2", childAllocator2.getName());

            assertNotSame("Child allocators should be different instances", childAllocator, childAllocator2);

            childAllocator.close();
            childAllocator2.close();

    }

    public void testAllocatingBufferIncreasesAllocatedMemory() {
            pool = new ArrowBufferPool(Settings.EMPTY);

            long initialPoolMemory = pool.getTotalAllocatedBytes();
            assertEquals("Pool should start with zero allocated memory", 0L, initialPoolMemory);

            BufferAllocator childAllocator = pool.createChildAllocator("memory-test");
            long initialChildMemory = childAllocator.getAllocatedMemory();
            assertEquals("Child allocator should start with zero allocated memory", 0L, initialChildMemory);

            int bufferSize = 1024; // 1KB
            ArrowBuf buffer = childAllocator.buffer(bufferSize);

            assertNotNull("Buffer should not be null", buffer);
            assertTrue("Buffer should have correct capacity", buffer.capacity() >= bufferSize);

            long poolMemoryAfterAllocation = pool.getTotalAllocatedBytes();
            long childMemoryAfterAllocation = childAllocator.getAllocatedMemory();

            assertTrue("Pool allocated memory should increase after buffer allocation",
                      poolMemoryAfterAllocation > initialPoolMemory);
            assertTrue("Child allocated memory should increase after buffer allocation",
                      childMemoryAfterAllocation > initialChildMemory);

            assertTrue("Pool should track at least the buffer size",
                      poolMemoryAfterAllocation >= bufferSize);
            assertTrue("Child should track at least the buffer size",
                      childMemoryAfterAllocation >= bufferSize);

            ArrowBuf buffer2 = childAllocator.buffer(2048); // 2KB
            assertNotNull("Second buffer should not be null", buffer2);

            long poolMemoryAfterSecondAllocation = pool.getTotalAllocatedBytes();
            assertTrue("Pool memory should increase further after second allocation",
                      poolMemoryAfterSecondAllocation > poolMemoryAfterAllocation);

            buffer.close();
            buffer2.close();
            childAllocator.close();

    }

    public void testReleasingBufferReducesAllocatedMemory() {
            pool = new ArrowBufferPool(Settings.EMPTY);

            BufferAllocator childAllocator = pool.createChildAllocator("release-test");
            int bufferSize = 1024; // 1KB
            ArrowBuf buffer = childAllocator.buffer(bufferSize);

            assertNotNull("Buffer should be allocated", buffer);
            long memoryAfterAllocation = pool.getTotalAllocatedBytes();
            assertTrue("Memory should be allocated", memoryAfterAllocation > 0);

            buffer.close();

            long memoryAfterRelease = pool.getTotalAllocatedBytes();
            assertTrue("Memory should be reduced after buffer release",
                      memoryAfterRelease < memoryAfterAllocation);

            childAllocator.close();

            long finalMemory = pool.getTotalAllocatedBytes();
            assertEquals("All memory should be released after closing child allocator", 0L, finalMemory);

            BufferAllocator childAllocator2 = pool.createChildAllocator("cycle-test");
            ArrowBuf buffer1 = childAllocator2.buffer(512);
            ArrowBuf buffer2 = childAllocator2.buffer(1024);

            long memoryWithTwoBuffers = pool.getTotalAllocatedBytes();
            assertTrue("Memory should increase with multiple buffers", memoryWithTwoBuffers > 0);

            buffer1.close();
            long memoryAfterFirstRelease = pool.getTotalAllocatedBytes();
            assertTrue("Memory should decrease after first buffer release",
                      memoryAfterFirstRelease < memoryWithTwoBuffers);

            buffer2.close();
            long memoryAfterSecondRelease = pool.getTotalAllocatedBytes();
            assertTrue("Memory should decrease further after second buffer release",
                      memoryAfterSecondRelease < memoryAfterFirstRelease);

            childAllocator2.close();
            assertEquals("All memory should be released", 0L, pool.getTotalAllocatedBytes());
    }

    public void testClosingPoolReleasesAllMemory() {
            pool = new ArrowBufferPool(Settings.EMPTY);

            BufferAllocator childAllocator1 = pool.createChildAllocator("close-test-1");
            BufferAllocator childAllocator2 = pool.createChildAllocator("close-test-2");

            ArrowBuf buffer1 = childAllocator1.buffer(1024);
            ArrowBuf buffer2 = childAllocator2.buffer(2048);

            long memoryBeforeClose = pool.getTotalAllocatedBytes();
            assertTrue("Memory should be allocated before close", memoryBeforeClose > 0);

            buffer1.close();
            buffer2.close();
            childAllocator1.close();
            childAllocator2.close();

            pool.close();

            long memoryAfterClose = pool.getTotalAllocatedBytes();
            assertEquals("All memory should be released after pool close", 0L, memoryAfterClose);

            assertDoesNotThrow(() -> pool.close());
            assertDoesNotThrow(() -> pool.close());

            assertEquals("Memory should remain zero after multiple closes", 0L, pool.getTotalAllocatedBytes());
    }


    public void testPoolWithCustomSettings() {
            Settings customSettings = Settings.builder()
                .put("some.custom.setting", "value")
                .build();

            pool = new ArrowBufferPool(customSettings);

            assertNotNull("Pool should be created with custom settings", pool);
            assertEquals("Pool should start with zero memory", 0L, pool.getTotalAllocatedBytes());

            BufferAllocator childAllocator = pool.createChildAllocator("custom-settings-test");
            assertNotNull("Child allocator should be created", childAllocator);
            assertEquals("Child allocator should have 1GB limit",
                        1024L * 1024L * 1024L, childAllocator.getLimit());

            childAllocator.close();
    }
}
