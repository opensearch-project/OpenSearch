/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.pagecache;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

/**
 * Unit tests for {@link PageCacheHandle} validation and lifecycle.
 *
 * <p>These tests do NOT require the native Foyer library. They exercise only the
 * Java-side validation and the delegation contract of {@link PageCacheHandle}
 * using in-line {@link PageCache} implementations (lambdas / anonymous classes).
 *
 * <p>Tests that require the native library (i.e. actual {@code FoyerPageCache}
 * creation/destruction) belong in integration tests that load the native shared lib.
 */
public class PageCacheHandleTests {

    // ── Construction guard ────────────────────────────────────────────────────

    @Test
    public void testConstructorThrowsOnNullCache() {
        assertThrows(
            NullPointerException.class,
            () -> new PageCacheHandle(null)
        );
    }

    // ── Delegation ────────────────────────────────────────────────────────────

    @Test
    public void testGetCacheReturnsSameInstance() {
        PageCache mockCache = () -> {};
        PageCacheHandle handle = new PageCacheHandle(mockCache);

        assertNotNull(handle.getCache());
        assertSame(mockCache, handle.getCache());
    }

    @Test
    public void testGetCacheReturnsPageCacheInterface() {
        PageCacheHandle handle = new PageCacheHandle(() -> {});
        PageCache cache = handle.getCache();  // compile-time check: returns interface
        assertNotNull(cache);
    }

    @Test
    public void testCloseDelegatesToCache() {
        AtomicInteger closeCount = new AtomicInteger(0);
        PageCacheHandle handle = new PageCacheHandle(closeCount::incrementAndGet);
        handle.close();

        assertEquals("close() must delegate to cache.close()", 1, closeCount.get());
    }

    @Test
    public void testCloseCallsUnderlyingCacheEachTime() {
        // PageCacheHandle delegates close() on every call.
        // Idempotency is the responsibility of the PageCache implementation
        // (e.g. FoyerPageCache uses an AtomicBoolean). PageCacheHandle
        // intentionally adds no extra guard — the impl owns the contract.
        AtomicInteger closeCount = new AtomicInteger(0);
        PageCacheHandle handle = new PageCacheHandle(closeCount::incrementAndGet);
        handle.close();
        handle.close();

        assertEquals("PageCacheHandle delegates both close() calls to the impl", 2, closeCount.get());
    }

    // ── FoyerPageCache input validation (no native required) ─────────────────
    // All constructor calls include the required blockSizeBytes and ioEngine params.
    // Validation of the new params is tested in the dedicated tests below.

    private static final long BLOCK = 64 * 1024 * 1024L;   // 64 MB default
    private static final String ENGINE = "auto";

    @Test
    public void testFoyerPageCacheThrowsOnZeroDiskBytes() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new org.opensearch.pagecache.foyer.FoyerPageCache(0, "/tmp/cache", BLOCK, ENGINE)
        );
        assertEquals("diskBytes must be > 0, got: 0", ex.getMessage());
    }

    @Test
    public void testFoyerPageCacheThrowsOnNegativeDiskBytes() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new org.opensearch.pagecache.foyer.FoyerPageCache(-1, "/tmp/cache", BLOCK, ENGINE)
        );
    }

    @Test
    public void testFoyerPageCacheThrowsOnNullDiskDir() {
        assertThrows(
            NullPointerException.class,
            () -> new org.opensearch.pagecache.foyer.FoyerPageCache(1024, null, BLOCK, ENGINE)
        );
    }

    @Test
    public void testFoyerPageCacheThrowsOnBlankDiskDir() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new org.opensearch.pagecache.foyer.FoyerPageCache(1024, "   ", BLOCK, ENGINE)
        );
        assertEquals("diskDir must not be blank", ex.getMessage());
    }

    @Test
    public void testFoyerPageCacheThrowsOnEmptyDiskDir() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new org.opensearch.pagecache.foyer.FoyerPageCache(1024, "", BLOCK, ENGINE)
        );
        assertEquals("diskDir must not be blank", ex.getMessage());
    }

    @Test
    public void testFoyerPageCacheThrowsOnZeroBlockSize() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new org.opensearch.pagecache.foyer.FoyerPageCache(1024, "/tmp/cache", 0, ENGINE)
        );
        assertEquals("blockSizeBytes must be > 0, got: 0", ex.getMessage());
    }

    @Test
    public void testFoyerPageCacheThrowsOnNegativeBlockSize() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new org.opensearch.pagecache.foyer.FoyerPageCache(1024, "/tmp/cache", -1, ENGINE)
        );
    }

    @Test
    public void testFoyerPageCacheThrowsOnNullIoEngine() {
        assertThrows(
            NullPointerException.class,
            () -> new org.opensearch.pagecache.foyer.FoyerPageCache(1024, "/tmp/cache", BLOCK, null)
        );
    }
}
