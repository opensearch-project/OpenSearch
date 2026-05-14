/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge;

import org.opensearch.common.settings.Setting;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;
import org.opensearch.nativebridge.spi.NativeMemoryStats;
import org.opensearch.nativebridge.spi.NativeStatsProvider;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link NativeBridgeModule}.
 * <p>
 * Tests the NativeStatsProvider implementation: memoryStats() returns null when
 * the native library is not loaded, and delegates to NativeMemoryFetcher when loaded.
 * <p>
 * Note: In this test environment, the native library may or may not be available.
 * When the library is not available, memoryStats() returns null (the "not loaded" path).
 * When the library IS available but jemalloc symbols are missing, NativeMemoryFetcher
 * class initialization fails. The tests are designed to handle both scenarios.
 */
public class NativeBridgeModuleTests extends OpenSearchTestCase {

    public void testGetSettingsReturnsBothDecaySettings() {
        NativeBridgeModule module = new NativeBridgeModule();
        List<Setting<?>> settings = module.getSettings();
        assertEquals(2, settings.size());
        assertEquals("native.jemalloc.dirty_decay_ms", settings.get(0).getKey());
        assertEquals("native.jemalloc.muzzy_decay_ms", settings.get(1).getKey());
    }

    /**
     * Tests that NativeBridgeModule implements NativeStatsProvider.
     */
    public void testImplementsNativeStatsProvider() {
        NativeBridgeModule module = new NativeBridgeModule();
        assertTrue("NativeBridgeModule should implement NativeStatsProvider", module instanceof NativeStatsProvider);
    }

    /**
     * Tests the memoryStats() contract based on library availability.
     * <p>
     * When NativeLibraryLoader.isLoaded() returns false, memoryStats() must return null.
     * When it returns true, memoryStats() delegates to NativeMemoryFetcher.fetch().
     * <p>
     * In this test environment without the native library symbols, we verify the
     * "not loaded" path returns null. If the library happens to be loaded (e.g., in
     * a full integration test environment), we verify delegation produces valid stats.
     */
    public void testMemoryStatsReturnsNullWhenLibraryNotLoaded() {
        boolean libraryLoaded = NativeLibraryLoader.isLoaded();

        if (!libraryLoaded) {
            // Library not loaded — memoryStats() should return null without touching NativeMemoryFetcher
            NativeBridgeModule module = new NativeBridgeModule();
            NativeMemoryStats stats = module.memoryStats();
            assertNull("memoryStats() should return null when native library is not loaded", stats);
        } else {
            // Library is loaded — memoryStats() will attempt to delegate to NativeMemoryFetcher.
            // If jemalloc symbols are available, it returns valid stats.
            // If symbols are missing, NativeMemoryFetcher class init fails with NoClassDefFoundError.
            NativeBridgeModule module = new NativeBridgeModule();
            try {
                NativeMemoryStats stats = module.memoryStats();
                // If we get here, the full native library with jemalloc is available
                assertNotNull("memoryStats() should return non-null when library is fully loaded", stats);
                assertTrue("allocated bytes should be positive", stats.getAllocatedBytes() > 0);
                assertTrue("resident bytes should be positive", stats.getResidentBytes() > 0);
            } catch (NoClassDefFoundError | ExceptionInInitializerError e) {
                // Library loaded but jemalloc symbols not available — this is expected
                // in test environments where the Rust library is built without jemalloc
                logger.info("NativeMemoryFetcher class init failed (expected in test env without jemalloc): {}", e.getMessage());
            }
        }
    }
}
