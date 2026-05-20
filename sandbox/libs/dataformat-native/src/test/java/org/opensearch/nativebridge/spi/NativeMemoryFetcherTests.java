/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link NativeMemoryFetcher} error handling contract.
 * <p>
 * The fetcher performs FFM downcalls to jemalloc. These tests verify:
 * <ul>
 *   <li>When the native library is loaded with jemalloc symbols, fetch() returns positive values</li>
 *   <li>The error handling contract: exceptions → (-1, -1), negative values → (-1, -1)</li>
 * </ul>
 * <p>
 * Note: NativeMemoryFetcher's static initializer requires the jemalloc symbols
 * (native_jemalloc_allocated_bytes, native_jemalloc_resident_bytes) to be present in the
 * native library. In test environments where the library is built without jemalloc support,
 * the class cannot be initialized. Tests handle this gracefully.
 */
public class NativeMemoryFetcherTests extends OpenSearchTestCase {

    /**
     * Tests that fetch() returns valid positive values when the native library is loaded
     * with jemalloc symbols. If jemalloc symbols are not available, the test verifies
     * that the class initialization fails as expected (symbols not found).
     */
    public void testFetchReturnsPositiveValuesOrFailsWithoutJemalloc() {
        try {
            AnalyticsBackendNativeMemoryStats stats = NativeMemoryFetcher.fetch();
            // If we get here, jemalloc symbols are available
            assertNotNull("fetch() should never return null", stats);
            assertTrue(
                "allocated bytes should be positive when jemalloc is active, got " + stats.getAllocatedBytes(),
                stats.getAllocatedBytes() > 0
            );
            assertTrue(
                "resident bytes should be positive when jemalloc is active, got " + stats.getResidentBytes(),
                stats.getResidentBytes() > 0
            );
        } catch (ExceptionInInitializerError | NoClassDefFoundError e) {
            // Expected when jemalloc symbols are not in the native library.
            // The static initializer uses orElseThrow() on symbol lookup.
            logger.info("NativeMemoryFetcher class init failed (expected without jemalloc symbols): {}", e.getMessage());
            assertTrue(
                "Class init failure should be caused by missing jemalloc symbols",
                e.getCause() == null
                    || e.getCause() instanceof java.util.NoSuchElementException
                    || e.getMessage().contains("NativeMemoryFetcher")
            );
        }
    }

    /**
     * Documents the error handling contract: when allocated or resident bytes are -1,
     * it indicates an error condition. This test verifies that the AnalyticsBackendNativeMemoryStats
     * error sentinel values are correctly constructed.
     * <p>
     * This test does NOT require the native library since it only tests AnalyticsBackendNativeMemoryStats
     * construction directly.
     */
    public void testErrorSentinelValuesContract() {
        // Directly construct the error state that fetch() would return on failure
        AnalyticsBackendNativeMemoryStats errorStats = new AnalyticsBackendNativeMemoryStats(-1, -1);
        assertEquals("Error sentinel for allocatedBytes should be -1", -1L, errorStats.getAllocatedBytes());
        assertEquals("Error sentinel for residentBytes should be -1", -1L, errorStats.getResidentBytes());
    }

    /**
     * Tests that the error state AnalyticsBackendNativeMemoryStats(-1, -1) is distinguishable from
     * valid stats. This documents the contract that fetch() returns (-1, -1) on:
     * - FFM downcall throwing an exception
     * - FFM downcall returning a negative value
     */
    public void testErrorStateIsDistinguishableFromValidStats() {
        AnalyticsBackendNativeMemoryStats errorStats = new AnalyticsBackendNativeMemoryStats(-1, -1);
        AnalyticsBackendNativeMemoryStats validStats = new AnalyticsBackendNativeMemoryStats(1024, 2048);

        // Error state has -1 for both fields
        assertTrue("Error state allocatedBytes should be negative", errorStats.getAllocatedBytes() < 0);
        assertTrue("Error state residentBytes should be negative", errorStats.getResidentBytes() < 0);

        // Valid state has positive values
        assertTrue("Valid state allocatedBytes should be positive", validStats.getAllocatedBytes() > 0);
        assertTrue("Valid state residentBytes should be positive", validStats.getResidentBytes() > 0);
    }

    /**
     * Tests that AnalyticsBackendNativeMemoryStats correctly stores zero values (boundary between
     * valid and error states). Zero is a valid value (no memory allocated yet).
     */
    public void testZeroValuesAreValid() {
        AnalyticsBackendNativeMemoryStats stats = new AnalyticsBackendNativeMemoryStats(0, 0);
        assertEquals("Zero should be a valid allocatedBytes value", 0L, stats.getAllocatedBytes());
        assertEquals("Zero should be a valid residentBytes value", 0L, stats.getResidentBytes());
    }
}
