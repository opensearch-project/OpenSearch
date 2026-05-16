/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link FoyerBlockCache} constructor argument validation.
 *
 * <p>All validation guards throw before reaching {@code FoyerBridge.createCache()},
 * so these tests run without the native library.
 */
public class FoyerBlockCacheTests extends OpenSearchTestCase {

    // ── diskBytes validation ──────────────────────────────────────────────────

    public void testConstructorThrowsWhenDiskBytesIsZero() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new FoyerBlockCache(0L, "/tmp/cache", 1L, "auto", 0L)
        );
        assertTrue("message should mention diskBytes", ex.getMessage().contains("diskBytes"));
    }

    public void testConstructorThrowsWhenDiskBytesIsNegative() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new FoyerBlockCache(-1L, "/tmp/cache", 1L, "auto", 0L)
        );
        assertTrue(ex.getMessage().contains("diskBytes"));
    }

    // ── diskDir validation ────────────────────────────────────────────────────

    public void testConstructorThrowsWhenDiskDirIsNull() {
        NullPointerException ex = expectThrows(NullPointerException.class, () -> new FoyerBlockCache(1L, null, 1L, "auto", 0L));
        assertTrue("message should mention diskDir", ex.getMessage().contains("diskDir"));
    }

    public void testConstructorThrowsWhenDiskDirIsBlank() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new FoyerBlockCache(1L, "   ", 1L, "auto", 0L));
        assertTrue(ex.getMessage().contains("diskDir"));
    }

    public void testConstructorThrowsWhenDiskDirIsEmptyString() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new FoyerBlockCache(1L, "", 1L, "auto", 0L));
        assertTrue(ex.getMessage().contains("diskDir"));
    }

    // ── blockSizeBytes validation ─────────────────────────────────────────────

    public void testConstructorThrowsWhenBlockSizeBytesIsZero() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new FoyerBlockCache(1L, "/tmp/cache", 0L, "auto", 0L)
        );
        assertTrue("message should mention blockSizeBytes", ex.getMessage().contains("blockSizeBytes"));
    }

    public void testConstructorThrowsWhenBlockSizeBytesIsNegative() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new FoyerBlockCache(1L, "/tmp/cache", -1L, "auto", 0L)
        );
        assertTrue(ex.getMessage().contains("blockSizeBytes"));
    }

    // ── ioEngine validation ───────────────────────────────────────────────────

    public void testConstructorThrowsWhenIoEngineIsNull() {
        NullPointerException ex = expectThrows(NullPointerException.class, () -> new FoyerBlockCache(1L, "/tmp/cache", 1L, null, 0L));
        assertTrue("message should mention ioEngine", ex.getMessage().contains("ioEngine"));
    }

    // ── sweepIntervalSecs validation ──────────────────────────────────────────

    public void testConstructorThrowsWhenSweepIntervalSecsIsNegative() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new FoyerBlockCache(1L, "/tmp/cache", 1L, "auto", -1L)
        );
        assertTrue("message should mention sweepIntervalSecs", ex.getMessage().contains("sweepIntervalSecs"));
        assertTrue("message should contain the bad value -1", ex.getMessage().contains("-1"));
    }

    public void testConstructorAcceptsSweepIntervalSecsOfZero() {
        // 0 = use Rust-side default (30s); must NOT throw before reaching FoyerBridge
        // This will fail at the native call, but not at argument validation.
        // We can't fully invoke the constructor without the native library, so we just
        // verify the guard doesn't fire for 0.
        try {
            new FoyerBlockCache(1L, "/tmp/cache", 1L, "auto", 0L);
            fail("Expected native library call to fail (UnsatisfiedLinkError or similar)");
        } catch (IllegalArgumentException e) {
            fail("Validation guard fired unexpectedly for sweepIntervalSecs=0: " + e.getMessage());
        } catch (Throwable ignored) {
            // Expected: native library not loaded in unit test environment
        }
    }

    // ── Error message quality ─────────────────────────────────────────────────

    /**
     * Verify error messages include the bad value so operators can act on them
     * without reading source code.
     */
    public void testDiskBytesErrorMessageContainsBadValue() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new FoyerBlockCache(-42L, "/tmp/cache", 1L, "auto", 0L)
        );
        assertTrue("error message should contain the bad value -42", ex.getMessage().contains("-42"));
    }

    public void testBlockSizeBytesErrorMessageContainsBadValue() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new FoyerBlockCache(1L, "/tmp/cache", -8L, "auto", 0L)
        );
        assertTrue("error message should contain the bad value -8", ex.getMessage().contains("-8"));
    }

    // ── Guard ordering: earlier guard fires first ─────────────────────────────

    /**
     * diskBytes is checked before diskDir, so a zero diskBytes with null diskDir
     * should throw on diskBytes, not NPE on diskDir.
     */
    public void testDiskBytesGuardFiresBeforeDiskDirNullCheck() {
        // zero diskBytes + null diskDir: first guard (diskBytes) should fire
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new FoyerBlockCache(0L, null, 1L, "auto", 0L));
        assertTrue(ex.getMessage().contains("diskBytes"));
    }
}
