/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link NativeArenaPurger} Rust-backed periodic purge.
 * These tests verify the FFI wiring and scheduling behavior.
 * The Rust purge thread is a native OS thread managed outside JVM lifecycle,
 * so thread leak detection is disabled for this suite.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class NativeArenaPurgerTests extends OpenSearchTestCase {

    private static final long INTERVAL_MS = 50;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        NativeArenaPurger.init(0, INTERVAL_MS);
    }

    @Override
    public void tearDown() throws Exception {
        NativeArenaPurger.setCheckIntervalMs(0);
        super.tearDown();
    }

    public void testPurgeDisabledWhenIntervalIsZero() throws Exception {
        NativeArenaPurger.setCheckIntervalMs(0);
        assertBusy(() -> {
            long countBefore = NativeArenaPurger.getPurgeCount();
            Thread.sleep(INTERVAL_MS * 3);
            assertEquals("No purges should fire when interval=0", countBefore, NativeArenaPurger.getPurgeCount());
        });
    }

    public void testPurgeFiresPeriodically() throws Exception {
        long countBefore = NativeArenaPurger.getPurgeCount();
        assertBusy(() -> assertTrue("Purge should have fired at least once", NativeArenaPurger.getPurgeCount() > countBefore));
    }

    public void testPurgeOnlyFiresAboveThreshold() throws Exception {
        NativeArenaPurger.setThresholdBytes(Long.MAX_VALUE);
        assertBusy(() -> {
            long countBefore = NativeArenaPurger.getPurgeCount();
            Thread.sleep(INTERVAL_MS * 3);
            assertEquals("Purge should not fire when below threshold", countBefore, NativeArenaPurger.getPurgeCount());
        });
    }
}
