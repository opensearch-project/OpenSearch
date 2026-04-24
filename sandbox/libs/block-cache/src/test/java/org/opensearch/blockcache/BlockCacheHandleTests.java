/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

/**
 * Unit tests for {@link BlockCacheHandle} validation and lifecycle.
 *
 * <p>These tests do NOT require the native Foyer library. They exercise only the
 * Java-side validation and the delegation contract of {@link BlockCacheHandle}
 * using in-line {@link BlockCache} implementations (lambdas / anonymous classes).
 *
 * <p>Tests that require the native library (i.e. actual {@code FoyerBlockCache}
 * creation/destruction) belong in integration tests that load the native shared lib.
 */
public class BlockCacheHandleTests {

    // ── Construction guard ────────────────────────────────────────────────────

    @Test
    public void testConstructorThrowsOnNullCache() {
        assertThrows(
            NullPointerException.class,
            () -> new BlockCacheHandle(null)
        );
    }

    // ── Delegation ────────────────────────────────────────────────────────────

    @Test
    public void testGetCacheReturnsSameInstance() {
        BlockCache mockCache = () -> {};
        BlockCacheHandle handle = new BlockCacheHandle(mockCache);

        assertNotNull(handle.getCache());
        assertSame(mockCache, handle.getCache());
    }

    @Test
    public void testGetCacheReturnsBlockCacheInterface() {
        BlockCacheHandle handle = new BlockCacheHandle(() -> {});
        BlockCache cache = handle.getCache();  // compile-time check: returns interface
        assertNotNull(cache);
    }

    @Test
    public void testCloseDelegatesToCache() {
        AtomicInteger closeCount = new AtomicInteger(0);
        BlockCacheHandle handle = new BlockCacheHandle(closeCount::incrementAndGet);
        handle.close();

        assertEquals("close() must delegate to cache.close()", 1, closeCount.get());
    }

    @Test
    public void testCloseCallsUnderlyingCacheEachTime() {
        // BlockCacheHandle delegates close() on every call.
        // Idempotency is the responsibility of the BlockCache implementation
        // (e.g. FoyerBlockCache uses an AtomicBoolean). BlockCacheHandle
        // intentionally adds no extra guard — the impl owns the contract.
        AtomicInteger closeCount = new AtomicInteger(0);
        BlockCacheHandle handle = new BlockCacheHandle(closeCount::incrementAndGet);
        handle.close();
        handle.close();

        assertEquals("BlockCacheHandle delegates both close() calls to the impl", 2, closeCount.get());
    }

    // ── FoyerBlockCache input validation (no native required) ─────────────────

    private static final long BLOCK = 64 * 1024 * 1024L;   // 64 MB default
    private static final String ENGINE = "auto";

    @Test
    public void testFoyerBlockCacheThrowsOnZeroDiskBytes() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new org.opensearch.blockcache.foyer.FoyerBlockCache(0, "/tmp/cache", BLOCK, ENGINE)
        );
        assertEquals("diskBytes must be > 0, got: 0", ex.getMessage());
    }

    @Test
    public void testFoyerBlockCacheThrowsOnNegativeDiskBytes() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new org.opensearch.blockcache.foyer.FoyerBlockCache(-1, "/tmp/cache", BLOCK, ENGINE)
        );
    }

    @Test
    public void testFoyerBlockCacheThrowsOnNullDiskDir() {
        assertThrows(
            NullPointerException.class,
            () -> new org.opensearch.blockcache.foyer.FoyerBlockCache(1024, null, BLOCK, ENGINE)
        );
    }

    @Test
    public void testFoyerBlockCacheThrowsOnBlankDiskDir() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new org.opensearch.blockcache.foyer.FoyerBlockCache(1024, "   ", BLOCK, ENGINE)
        );
        assertEquals("diskDir must not be blank", ex.getMessage());
    }

    @Test
    public void testFoyerBlockCacheThrowsOnEmptyDiskDir() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new org.opensearch.blockcache.foyer.FoyerBlockCache(1024, "", BLOCK, ENGINE)
        );
        assertEquals("diskDir must not be blank", ex.getMessage());
    }

    @Test
    public void testFoyerBlockCacheThrowsOnZeroBlockSize() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new org.opensearch.blockcache.foyer.FoyerBlockCache(1024, "/tmp/cache", 0, ENGINE)
        );
        assertEquals("blockSizeBytes must be > 0, got: 0", ex.getMessage());
    }

    @Test
    public void testFoyerBlockCacheThrowsOnNegativeBlockSize() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new org.opensearch.blockcache.foyer.FoyerBlockCache(1024, "/tmp/cache", -1, ENGINE)
        );
    }

    @Test
    public void testFoyerBlockCacheThrowsOnNullIoEngine() {
        assertThrows(
            NullPointerException.class,
            () -> new org.opensearch.blockcache.foyer.FoyerBlockCache(1024, "/tmp/cache", BLOCK, null)
        );
    }
}
