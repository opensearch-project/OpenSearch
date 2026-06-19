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

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Start with threshold=0 (always purge) and short interval for fast test feedback
        NativeArenaPurger.init(0, 200);
    }

    @Override
    public void tearDown() throws Exception {
        NativeArenaPurger.setCheckIntervalMs(0);
        super.tearDown();
    }

    public void testPurgeDisabledWhenIntervalIsZero() throws Exception {
        NativeArenaPurger.setCheckIntervalMs(0);
        // Wait for the current sleep cycle to complete + 1s buffer for the thread to see interval=0
        Thread.sleep(500);

        long before = NativeArenaPurger.getPurgeCount();
        Thread.sleep(500);
        assertEquals("No purges should fire when interval=0", before, NativeArenaPurger.getPurgeCount());
    }

    public void testPurgeFiresPeriodically() throws Exception {
        long before = NativeArenaPurger.getPurgeCount();
        Thread.sleep(500);
        assertTrue("Purge should have fired at least once", NativeArenaPurger.getPurgeCount() > before);
    }

    public void testPurgeOnlyFiresAboveThreshold() throws Exception {
        // Set threshold very high — purge should never fire
        NativeArenaPurger.setThresholdBytes(Long.MAX_VALUE);
        long before = NativeArenaPurger.getPurgeCount();
        Thread.sleep(500);
        assertEquals("Purge should not fire when below threshold", before, NativeArenaPurger.getPurgeCount());
    }
}
